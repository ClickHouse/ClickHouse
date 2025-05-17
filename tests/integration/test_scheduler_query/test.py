# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance(
    "node",
    stay_alive=True,
    main_configs=[],
    with_zookeeper=True,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clear_workloads_and_resources():
    node.query(
        f"""
        drop workload if exists production;
        drop workload if exists development;
        drop workload if exists main;
        drop workload if exists admin;
        drop workload if exists all;
        drop resource if exists query;
    """
    )
    yield


def assert_profile_event(node, query_id, profile_event, check):
    assert check(
        int(
            node.query(
                f"select ProfileEvents['{profile_event}'] from system.query_log where current_database = currentDatabase() and query_id = '{query_id}' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
            )
        )
    )


def test_create():
    node.query(
        f"""
        create resource query (query);
        create workload all settings max_concurrent_queries=20;
        create workload admin in all settings priority=0;
        create workload main in all settings priority=1, max_concurrent_queries=10;
        create workload production in main settings weight=9;
        create workload development in main settings weight=1, max_concurrent_queries=5;
    """
    )

    def do_checks():
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/admin/%' and type='fifo'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/admin' and type='unified' and priority=0"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/production/%' and type='fifo'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/production' and type='unified' and weight=9"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/development/%' and type='fifo'"
            )
            == "1\n"
        )
        assert (
            node.query(
                f"select count() from system.scheduler where path ilike '%/all/%' and type='inflight_limit' and resource='query' and max_requests=20"
            )
            == "1\n"
        )

    do_checks()
    node.restart_clickhouse()  # Check that workloads persist
    do_checks()


class QueryPool:
    def __init__(self, num_queries, workload):
        self.num_queries = num_queries
        self.workload = workload

    def start(self):
        self.stop_event = threading.Event()
        self.threads = []
        def query_thread(stop_event, workload, max_threads=2):
            while not stop_event.is_set():
                node.query(
                    f"select count(*) from numbers_mt(100000000) settings workload='{workload}', max_threads={max_threads}"
                )

        for i in range(self.num_queries):
            self.threads.append(threading.Thread(target=query_thread, args=(self.stop_event, self.workload)))
        for thread in self.threads:
            thread.start()

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join()


def test_max_concurrent_queries():
    node.query(
        f"""
        create resource query (query);
        create workload all settings max_concurrent_queries=6;
        create workload admin in all settings priority=0;
        create workload main in all settings priority=1, max_concurrent_queries=4;
        create workload production in main settings weight=9;
        create workload development in main settings weight=1, max_concurrent_queries=2;
    """
    )

    def concurrent_queries():
        return int(
            node.query(
                f"select value from system.metrics where name='ConcurrentQueryAcquired'"
            ).strip()
        )

    def check_metrics(limit):
        for i in range(3):
            assert(concurrent_queries() <= limit)
            time.sleep(0.1)
        while concurrent_queries() < limit:
            time.sleep(0.1)

    def inflight_queries(workload):
        return int(
            node.query(
                f"select inflight_requests from system.scheduler where path='/all/semaphore' and resource='query'"
            ).strip()
        )

    def check_scheduler(workload, limit):
        for i in range(3):
            assert(inflight_queries(workload) <= limit)
            time.sleep(0.1)
        while inflight_queries(workload) < limit:
            time.sleep(0.1)


    admin = QueryPool(6, 'admin')
    production = QueryPool(6, 'production')
    development = QueryPool(6, 'development')

    production.start()
    check_metrics(4)
    check_scheduler('production', 4)
    production.stop()

    development.start()
    check_metrics(2)
    check_scheduler('development', 2)
    development.stop()

    production.start()
    development.start()
    check_metrics(4)
    admin.start()
    check_metrics(6)
    check_scheduler('admin', 6)
    production.stop()
    development.stop()
    admin.stop()

