# -*- coding: utf-8 -*-
"""
    celery.app.defaults
    ~~~~~~~~~~~~~~~~~~~

    Configuration introspection and defaults.

"""
from __future__ import absolute_import

import sys

from collections import deque, namedtuple
from datetime import timedelta

from celery.five import items
from celery.utils import strtobool
from celery.utils.functional import memoize

__all__ = ['Option', 'NAMESPACES', 'flatten', 'find']

is_jython = sys.platform.startswith('java')
is_pypy = hasattr(sys, 'pypy_version_info')

DEFAULT_POOL = 'prefork'
if is_jython:
    DEFAULT_POOL = 'threads'
elif is_pypy:
    if sys.pypy_version_info[0:3] < (1, 5, 0):
        DEFAULT_POOL = 'solo'
    else:
        DEFAULT_POOL = 'prefork'

DEFAULT_ACCEPT_CONTENT = ['json', 'pickle', 'msgpack', 'yaml']
DEFAULT_PROCESS_LOG_FMT = """
    [%(asctime)s: %(levelname)s/%(processName)s] %(message)s
""".strip()
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s] %(message)s'
DEFAULT_TASK_LOG_FMT = """[%(asctime)s: %(levelname)s/%(processName)s] \
%(task_name)s[%(task_id)s]: %(message)s"""

searchresult = namedtuple('searchresult', ('namespace', 'key', 'type'))


class Option(object):
    alt = None
    deprecate_by = None
    remove_by = None
    typemap = dict(string=str, int=int, float=float, any=lambda v: v,
                   bool=strtobool, dict=dict, tuple=tuple)

    def __init__(self, default=None, *args, **kwargs):
        self.default = default
        self.type = kwargs.get('type') or 'string'
        for attr, value in items(kwargs):
            setattr(self, attr, value)

    def to_python(self, value):
        return self.typemap[self.type](value)

    def __repr__(self):
        return '<Option: type->{0} default->{1!r}>'.format(self.type,
                                                           self.default)

NAMESPACES = {
    'accept_content': Option(DEFAULT_ACCEPT_CONTENT, type='list'),
    'admins': Option((), type='tuple'),
    'enable_utc': Option(True, type='bool'),
    'imports': Option((), type='tuple'),
    'include': Option((), type='tuple'),
    'server_email': Option('celery@localhost'),
    'timezone': Option(type='string'),
    'beat': {
        'schedule': Option({}, type='dict'),
        'scheduler': Option('celery.beat:PersistentScheduler'),
        'schedule_filename': Option('celerybeat-schedule'),
        'sync_every': Option(0, type='int'),
        'max_loop_interval': Option(0, type='float'),
    },
    'broker': {
        'url': Option(None, type='string'),
        'connection_timeout': Option(4, type='float'),
        'connection_retry': Option(True, type='bool'),
        'connection_max_retries': Option(100, type='int'),
        'failover_strategy': Option(None, type='string'),
        'heartbeat': Option(None, type='int'),
        'heartbeat_checkrate': Option(3.0, type='int'),
        'login_method': Option(None, type='string'),
        'pool_limit': Option(10, type='int'),
        'use_ssl': Option(False, type='bool'),
        'transport': Option(type='string'),
        'transport_options': Option({}, type='dict'),
        'host': Option(type='string'),
        'port': Option(type='int'),
        'user': Option(type='string'),
        'password': Option(type='string'),
        'vhost': Option(type='string'),
    },
    'cache': {
        'backend': Option(),
        'backend_options': Option({}, type='dict'),
    },
    'cassandra': {
        'column_family': Option(type='string'),
        'detailed_mode': Option(False, type='bool'),
        'keyspace': Option(type='string'),
        'read_consistency': Option(type='string'),
        'servers': Option(type='list'),
        'port': Option(type="string"),
        'entry_ttl': Option(type="float"),
        'write_consistency': Option(type='string'),
    },
    'chord': {
        'propagates': Option(True, type='bool'),
    },
    'couchbase': {
        'backend_settings': Option(None, type='dict'),
    },
    'email': {
        'host': Option('localhost'),
        'port': Option(25, type='int'),
        'host_user': Option(),
        'host_password': Option(),
        'timeout': Option(2, type='float'),
        'use_ssl': Option(False, type='bool'),
        'use_tls': Option(False, type='bool'),
        'charset': Option('us-ascii'),
    },
    'mongodb': {
        'backend_settings': Option(type='dict'),
    },
    'event': {
        'serializer': Option('json'),
        'queue_expires': Option(60.0, type='float'),
        'queue_ttl': Option(5.0, type='float'),
    },
    'redis': {
        'db': Option(type='int'),
        'host': Option(type='string'),
        'max_connections': Option(type='int'),
        'password': Option(type='string'),
        'port': Option(type='int'),
    },
    'result': {
        'backend': Option(type='string'),
        'cache_max': Option(100, type='int'),
        'compression': Option(type='str'),
        'exchange': Option('celeryresults'),
        'exchange_type': Option('direct'),
        'persistent': Option(None, type='bool'),
        'expires': Option(timedelta(days=1), type='float'),
        'serializer': Option('json'),
    },
    'riak': {
        'backend_settings': Option(type='dict'),
    },
    'security': {
        'key': Option(type='string'),
        'certificate': Option(type='string'),
        'cert_store': Option(type='string'),
    },
    'sqlalchemy': {
        'short_lived_sessions': Option(False, type='bool'),
        'table_names': Option(type='dict'),
        'dburi': Option(),
        'engine_options': Option(type='dict'),
    },
    'task': {
        'acks_late': Option(False, type='bool'),
        'always_eager': Option(False, type='bool'),
        'annotations': Option(type='any'),
        'create_missing_queues': Option(True, type='bool'),
        'default_rate_limit': Option(type='string'),
        'default_routing_key': Option('celery'),
        'default_queue': Option('celery'),
        'default_exchange': Option('celery'),
        'default_exchange_type': Option('direct'),
        'default_delivery_mode': Option(2, type='string'),
        'eager_propagates_exceptions': Option(False, type='bool'),
        'ignore_result': Option(False, type='bool'),
        'compression': Option(type='string'),
        'reject_on_worker_lost': Option(type='bool'),
        'routes': Option(type='any'),
        'send_error_emails': Option(False, type='bool'),
        'send_sent_event': Option(False, type='bool'),
        'store_errors_even_if_ignored': Option(False, type='bool'),
        'protocol': Option(1, type='int'),
        'publish_retry': Option(True, type='bool'),
        'publish_retry_policy': Option({
            'max_retries': 3,
            'interval_start': 0,
            'interval_max': 1,
            'interval_step': 0.2}, type='dict'),
        'serializer': Option('json'),
        'soft_time_limit': Option(type='float'),
        'time_limit': Option(type='float'),
        'track_started': Option(False, type='bool'),
        'queues': Option(type='dict'),
        'queue_ha_policy': Option(None, type='string'),
        'queue_max_priority': Option(None, type='int'),
    },
    'worker': {
        'agent': Option(None, type='string'),
        'autoscaler': Option('celery.worker.autoscale:Autoscaler'),
        'autoreloader': Option('celery.worker.autoreload:Autoreloader'),
        'concurrency': Option(0, type='int'),
        'disable_rate_limits': Option(False, type='bool'),
        'timer': Option(type='string'),
        'timer_precision': Option(1.0, type='float'),
        'force_execv': Option(False, type='bool'),
        'hijack_root_logger': Option(True, type='bool'),
        'consumer': Option('celery.worker.consumer:Consumer', type='string'),
        'enable_remote_control': Option(True, type='bool'),
        'log_format': Option(DEFAULT_PROCESS_LOG_FMT),
        'log_color': Option(type='bool'),
        'max_tasks_per_child': Option(type='int'),
        'pool': Option(DEFAULT_POOL),
        'pool_putlocks': Option(True, type='bool'),
        'pool_restarts': Option(False, type='bool'),
        'prefetch_multiplier': Option(4, type='int'),
        'send_events': Option(False, type='bool'),
        'state_db': Option(),
        'task_log_format': Option(DEFAULT_TASK_LOG_FMT),
        'redirect_stdouts': Option(True, type='bool'),
        'redirect_stdouts_level': Option('WARNING'),
        'direct': Option(False, type='bool'),
        'lost_wait': Option(10.0, type='float')
    },
}


def flatten(d, ns=''):
    stack = deque([(ns, d)])
    while stack:
        name, space = stack.popleft()
        for key, value in items(space):
            if isinstance(value, dict):
                stack.append((name + key + '_', value))
            else:
                yield name + key, value
DEFAULTS = {key: value.default for key, value in flatten(NAMESPACES)}


def find_deprecated_settings(source):
    from celery.utils import warn_deprecated
    for name, opt in flatten(NAMESPACES):
        if (opt.deprecate_by or opt.remove_by) and getattr(source, name, None):
            warn_deprecated(description='The {0!r} setting'.format(name),
                            deprecation=opt.deprecate_by,
                            removal=opt.remove_by,
                            alternative='Use the {0.alt} instead'.format(opt))
    return source


@memoize(maxsize=None)
def find(name, namespace='celery'):
    # - Try specified namespace first.
    namespace = namespace.lower()
    try:
        return searchresult(
            namespace, name.lower(), NAMESPACES[namespace][name.lower()],
        )
    except KeyError:
        # - Try all the other namespaces.
        for ns, keys in items(NAMESPACES):
            if ns.lower() == name.lower():
                return searchresult(None, ns, keys)
            elif isinstance(keys, dict):
                try:
                    return searchresult(ns, name.lower(), keys[name.lower()])
                except KeyError:
                    pass
    # - See if name is a qualname last.
    return searchresult(None, name.lower(), DEFAULTS[name.lower()])
