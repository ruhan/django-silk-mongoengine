from collections import Counter
import json

#from django.db import models
#from django.db.models import DateTimeField, StringField, StringField, ForeignKey, IntField, BooleanField, F, \
#    ManyToManyField, OneToOneField, FloatField
from mongoengine import *
from mongoengine import signals
from django.utils import timezone
from django.db import transaction
from uuid import uuid1
import sqlparse

# Django 1.8 removes commit_on_success, django 1.5 does not have atomic
atomic = getattr(transaction, 'atomic', None) or getattr(transaction, 'commit_on_success')


# Seperated out so can use in tests w/o models
def _time_taken(start_time, end_time):
    d = end_time - start_time
    return d.seconds * 1000 + d.microseconds / 1000


def time_taken(self):
    return _time_taken(self.start_time, self.end_time)


class CaseInsensitiveDictionary(dict):
    def __getitem__(self, key):
        return super(CaseInsensitiveDictionary, self).__getitem__(key.lower())

    def __setitem__(self, key, value):
        super(CaseInsensitiveDictionary, self).__setitem__(key.lower(), value)

    def update(self, other=None, **kwargs):
        for k, v in other.items():
            self[k] = v
        for k, v in kwargs.items():
            self[k] = v

    def __init__(self, d):
        super(CaseInsensitiveDictionary, self).__init__()
        for k, v in d.items():
            self[k] = v


class Request(Document):
    #id = StringField(max_length=36, default=uuid1, primary_key=True)
    response = ReferenceField('Response', )#related_name='response',)# db_index=True)
    status_code = IntField()
    path = StringField(max_length=300,)# db_index=True)
    query_params = StringField()#blank=True, default='')
    raw_body = StringField()#blank=True, default='')
    body = StringField()#blank=True, default='')
    method = StringField(max_length=10)
    start_time = DateTimeField(default=timezone.now,)# db_index=True)
    view_name = StringField(max_length=300,)# blank=True, default='', ) #db_index=True, )
    end_time = DateTimeField()#null=True,)# blank=True)
    time_taken = FloatField()#blank=True, null=True)
    encoded_headers = StringField()#blank=True, default='')
    meta_time = FloatField()#null=True,)# blank=True)
    meta_num_queries = IntField()#null=True,)# blank=True)
    meta_time_spent_queries = FloatField()#null=True,)# blank=True)
    pyprofile = StringField()#blank=True, default='')

    @property
    def total_meta_time(self):
        return (self.meta_time or 0) + (self.meta_time_spent_queries or 0)

    # defined in atomic transaction within SQLQuery save()/delete() as well
    # as in bulk_create of SQLQueryManager
    # TODO: This is probably a bad way to do this, .count() will prob do?
    num_sql_queries = IntField(default=0)


    @property
    def time_spent_on_sql_queries(self):
        # TODO: Perhaps there is a nicer way to do this with Django aggregates?
        # My initial thought was to perform:
        # SQLQuery.objects.filter.aggregate(Sum(F('end_time')) - Sum(F('start_time')))
        # However this feature isnt available yet, however there has been talk for use of F objects
        # within aggregates for four years here: https://code.djangoproject.com/ticket/14030
        # It looks like this will go in soon at which point this should be changed.
        return sum(x.time_taken for x in SQLQuery.objects.filter(request=self))

    @property
    def headers(self):
        if self.encoded_headers:
            raw = json.loads(self.encoded_headers)
        else:
            raw = {}
        return CaseInsensitiveDictionary(raw)

    @property
    def content_type(self):
        return self.headers.get('content-type', None)

    def save(self, *args, **kwargs):
        # sometimes django requests return the body as 'None'
        if self.raw_body is None: self.raw_body = ''
        if self.body is None: self.body = ''

        if self.end_time and self.start_time:
            interval = self.end_time - self.start_time
            self.time_taken = interval.total_seconds() * 1000
        return super(Request, self).save(*args, **kwargs)


class Response(Document):
    #id = StringField(max_length=36, default=uuid1, primary_key=True)
    request = ReferenceField('Request', )#related_name='response',)# db_index=True)
    status_code = IntField()
    raw_body = StringField()#blank=True, default='')
    body = StringField()#blank=True, default='')
    encoded_headers = StringField()#blank=True, default='')

    @property
    def content_type(self):
        return self.headers.get('content-type', None)

    @property
    def headers(self):
        if self.encoded_headers:
            raw = json.loads(self.encoded_headers)
        else:
            raw = {}
        return CaseInsensitiveDictionary(raw)


class SQLQuery(Document):
    query = StringField()
    start_time = DateTimeField(default=timezone.now, )#blank=True, null=True,)
    end_time = DateTimeField()#null=True,blank=True)
    time_taken = FloatField()#blank=True, null=True)
    request = ReferenceField('Request', )#related_name='queries', null=True, )#blank=True,)# db_index=True)
    traceback = StringField()

    @property
    def traceback_ln_only(self):
        return '\n'.join(self.traceback.split('\n')[::2])

    @property
    def formatted_query(self):
        return sqlparse.format(self.query, reindent=True, keyword_case='upper')

    # TODO: Surely a better way to handle this? May return false positives
    @property
    def num_joins(self):
        return self.query.lower().count('join ')

    @property
    def tables_involved(self):
        """A rreally ather rudimentary way to work out tables involved in a query.
        TODO: Can probably parse the SQL using sqlparse etc and pull out table info that way?"""
        components = [x.strip() for x in self.query.split()]
        tables = []
        for idx, c in enumerate(components):
            # TODO: If django uses aliases on column names they will be falsely identified as tables...
            if c.lower() == 'from' or c.lower() == 'join' or c.lower() == 'as':
                try:
                    nxt = components[idx + 1]
                    if not nxt.startswith('('):  # Subquery
                        stripped = nxt.strip().strip(',')
                        if stripped:
                            tables.append(stripped)
                except IndexError:  # Reach the end
                    pass
        return tables

    @atomic()
    def save(self, *args, **kwargs):
        if self.end_time and self.start_time:
            interval = self.end_time - self.start_time
            self.time_taken = interval.total_seconds() * 1000
        if not self.pk:
            if self.request:
                self.request.num_sql_queries += 1
                self.request.save()
        super(SQLQuery, self).save(*args, **kwargs)

    @atomic()
    def delete(self, *args, **kwargs):
        self.request.num_sql_queries -= 1
        self.request.save()
        super(SQLQuery, self).delete(*args, **kwargs)


class BaseProfile(Document):
    name = StringField(max_length=300,)# blank=True, default='')
    start_time = DateTimeField(default=timezone.now)
    end_time = DateTimeField()#null=True, )#blank=True)
    request = ReferenceField('Request', )#null=True,)# blank=True,)# db_index=True)
    time_taken = FloatField()#blank=True, null=True)

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if self.end_time and self.start_time:
            interval = self.end_time - self.start_time
            self.time_taken = interval.total_seconds() * 1000
        super(BaseProfile, self).save(*args, **kwargs)


class Profile(Document):
    name = StringField(max_length=300,)# blank=True, default='')
    start_time = DateTimeField(default=timezone.now)
    end_time = DateTimeField()#null=True, )#blank=True)
    request = ReferenceField('Request', )#null=True,)# blank=True,)# db_index=True)
    time_taken = FloatField()#blank=True, null=True)
    file_path = StringField(max_length=300, )#blank=True, default='')
    line_num = IntField()#null=True,)# blank=True)
    end_line_num = IntField()#null=True,)# blank=True)
    func_name = StringField(max_length=300,)# blank=True, default='')
    exception_raised = BooleanField(default=False)
    #queries = ManyToManyField('SQLQuery', related_name='profiles',)# db_index=True)
    dynamic = BooleanField(default=False)

    @property
    def is_function_profile(self):
        return self.func_name is not None

    @property
    def is_context_profile(self):
        return self.func_name is None

    @property
    def time_spent_on_sql_queries(self):
        time_spent = sum(x.time_taken for x in self.queries.all())
        return time_spent

    def save(self, *args, **kwargs):
        if self.end_time and self.start_time:
            interval = self.end_time - self.start_time
            self.time_taken = interval.total_seconds() * 1000
        super(BaseProfile, self).save(*args, **kwargs)
