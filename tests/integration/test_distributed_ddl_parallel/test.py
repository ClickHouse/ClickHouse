# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time
from functools import wraps

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)


# By default the exceptions that was throwed in threads will be ignored
# (they will not mark the test as failed, only printed to stderr).
#
# Wrap thrading.Thread and re-throw exception on join()
class SafeThread(threading.Thread):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.exception = None

    def run(self):
        try:
            self.target()
        except Exception as e:  # pylint: disable=broad-except
            self.exception = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception


def add_instance(name, ddl_config=None):
    main_configs = [
        "configs/remote_servers.xml",
    ]
    if ddl_config:
        main_configs.append(ddl_config)
    dictionaries = [
        "configs/dict.xml",
    ]
    return cluster.add_instance(
        name, main_configs=main_configs, dictionaries=dictionaries, with_zookeeper=True
    )


initiator = add_instance("initiator")
# distributed_ddl.pool_size = 2
n1 = add_instance("n1", "configs/ddl_a.xml")
n2 = add_instance("n2", "configs/ddl_a.xml")
# distributed_ddl.pool_size = 20
n3 = add_instance("n3", "configs/ddl_b.xml")
n4 = add_instance("n4", "configs/ddl_b.xml")


@pytest.fixture(scope="module", autouse=True)
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
            took = te - ts
            assert took >= sec
            return result

        return inner

    return wrapper


# It takes 7 seconds to load slow_dict_7.
def execute_reload_dictionary_slow_dict_7():
    initiator.query(
        "SYSTEM RELOAD DICTIONARY ON CLUSTER cluster_a slow_dict_7",
        settings={
            "distributed_ddl_task_timeout": 60,
        },
    )


def execute_reload_dictionary_slow_dict_3():
    initiator.query(
        "SYSTEM RELOAD DICTIONARY ON CLUSTER cluster_b slow_dict_3",
        settings={
            "distributed_ddl_task_timeout": 60,
        },
    )


def execute_smoke_query():
    initiator.query(
        "DROP DATABASE IF EXISTS foo ON CLUSTER cluster_b",
        settings={
            "distributed_ddl_task_timeout": 60,
        },
    )


def check_log():
    # ensure that none of tasks processed multiple times
    for _, instance in list(cluster.instances.items()):
        assert not instance.contains_in_log("Coordination::Exception: Node exists")


# NOTE: uses inner function to exclude slow start_cluster() from timeout.


def test_slow_dict_load_7():
    @pytest.mark.timeout(10)
    @longer_then(7)
    def inner_test():
        initiator.query("SYSTEM RELOAD DICTIONARY slow_dict_7")

    inner_test()


def test_all_in_parallel():
    @pytest.mark.timeout(10)
    @longer_then(7)
    def inner_test():
        threads = []
        for _ in range(2):
            threads.append(SafeThread(target=execute_reload_dictionary_slow_dict_7))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(70)

    inner_test()
    check_log()


def test_two_in_parallel_two_queued():
    @pytest.mark.timeout(19)
    @longer_then(14)
    def inner_test():
        threads = []
        for _ in range(4):
            threads.append(SafeThread(target=execute_reload_dictionary_slow_dict_7))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(70)

    inner_test()
    check_log()


def test_smoke():
    for _ in range(100):
        execute_smoke_query()
    check_log()


def test_smoke_parallel():
    threads = []
    for _ in range(50):
        threads.append(SafeThread(target=execute_smoke_query))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(70)
    check_log()


def test_smoke_parallel_dict_reload():
    threads = []
    for _ in range(90):
        threads.append(SafeThread(target=execute_reload_dictionary_slow_dict_3))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(70)
    check_log()
