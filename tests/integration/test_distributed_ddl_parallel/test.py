# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

from functools import wraps
import threading
import time
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

def add_instance(name):
    main_configs=[
        'configs/ddl.xml',
        'configs/remote_servers.xml',
    ]
    dictionaries=[
        'configs/dict.xml',
    ]
    return cluster.add_instance(name,
        main_configs=main_configs,
        dictionaries=dictionaries,
        with_zookeeper=True)

initiator = add_instance('initiator')
n1 = add_instance('n1')
n2 = add_instance('n2')

@pytest.fixture(scope='module', autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()

# verifies that functions executes longer then `sec`
def longer_then(sec):
    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            ts = time.time()
            result = func(*args, **kwargs)
            te = time.time()
            took = te-ts
            assert took >= sec
            return result
        return inner
    return wrapper

# It takes 7 seconds to load slow_dict.
def thread_reload_dictionary():
    initiator.query('SYSTEM RELOAD DICTIONARY ON CLUSTER cluster slow_dict')

# NOTE: uses inner function to exclude slow start_cluster() from timeout.

def test_dict_load():
    @pytest.mark.timeout(10)
    @longer_then(7)
    def inner_test():
        initiator.query('SYSTEM RELOAD DICTIONARY slow_dict')
    inner_test()

def test_all_in_parallel():
    @pytest.mark.timeout(10)
    @longer_then(7)
    def inner_test():
        threads = []
        for _ in range(2):
            threads.append(threading.Thread(target=thread_reload_dictionary))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    inner_test()

def test_two_in_parallel_two_queued():
    @pytest.mark.timeout(19)
    @longer_then(14)
    def inner_test():
        threads = []
        for _ in range(4):
            threads.append(threading.Thread(target=thread_reload_dictionary))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    inner_test()
