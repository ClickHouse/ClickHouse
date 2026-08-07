# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import random
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
        drop workload if exists admin;
        drop workload if exists all;
        drop resource if exists cpu;
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

def assert_query(node, query_id, noncompeting, competing):
    node.query("SYSTEM FLUSH LOGS")
    # Note that we cannot guarantee that all slots that should be (a) granted, (b) acquired and (c) passed to a thread, will actually undergo even the first stage before query finishes
    # So any attempt to make a stricter checks here lead to flakyiness due to described race condition. Workaround would require failpoint.
    assert_profile_event(
        node,
        query_id,
        "ConcurrencyControlSlotsAcquired",
        lambda x: x <= competing,
    )
    assert_profile_event(
        node,
        query_id,
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x <= noncompeting,
    )
    s_count = node.query(
        f"select length(thread_ids) from system.query_log where current_database = currentDatabase() and query_id = '{query_id}' and type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
    ).strip()
    count = int(s_count) if s_count else 0
    assert count >= 3 and count <= 2 + noncompeting + competing

# It is intended to acquire all slots before real load start to ensure all queries from the real load start simultaneously
class BusyPeriod:
    def __init__(self, workload, max_concurrent_threads):
        self.workload = workload

        def query_thread(workload, max_concurrent_threads):
            try:
                node.query(
                    f"SELECT count(*) FROM numbers_mt(1e12) settings workload='{workload}', max_threads={max_concurrent_threads}",
                    query_id=f"busy_{workload}",
                )
            except QueryRuntimeException:
                pass

        self.background_thread = threading.Thread(
            target=query_thread, args=(workload, max_concurrent_threads)
        )
        self.background_thread.start()
        self.workload = workload

    def release(self):
        node.query(f"KILL QUERY WHERE query_id = 'busy_{self.workload}' SYNC")



def test_create_workload():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=100;
        create workload admin in all settings priority=0;
        create workload production in all settings priority=1, weight=9;
        create workload development in all settings priority=1, weight=1;
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
                f"select count() from system.scheduler where path ilike '%/all/%' and type='inflight_limit' and resource='cpu' and max_requests=100"
            )
            == "1\n"
        )

    do_checks()
    node.restart_clickhouse()  # Check that workloads persist
    do_checks()


def test_concurrency_control_compatibility():
    node.query(
        f"""
        create resource cpu (worker thread); -- concurrency control doesn't count master thread towards the cpu slots limit
        create workload all settings max_concurrent_threads=50;
    """
    )

    # test_concurrent_threads_soft_limit_defined_50
    node.query(
        "select count(*) from numbers_mt(10000000) settings max_threads=100, workload='all'",
        query_id="test_compatibility",
    )
    assert_query(node, 'test_compatibility', 1, 50)

    # test_use_concurrency_control_soft_limit_defined_50
    node.query(
        "select count(*) from numbers_mt(10000000) settings max_threads=100, workload='all', use_concurrency_control=0",
        query_id="test_compatibility",
    )
    node.query("SYSTEM FLUSH LOGS")
    assert_profile_event(
        node,
        "test_compatibility",
        "ConcurrencyControlSlotsAcquired",
        lambda x: x == 0,
    )
    assert_profile_event(
        node,
        "test_compatibility",
        "ConcurrencyControlSlotsAcquiredNonCompeting",
        lambda x: x == 0,
    )

    node.query(
        f"""
        create or replace workload all settings max_concurrent_threads=1;
    """
    )

    # test_concurrent_threads_soft_limit_defined_1
    node.query(
        "select count(*) from numbers_mt(10000000) settings max_threads=100, workload='all'",
        query_id="test_compatibility",
    )
    assert_query(node, 'test_compatibility', 1, 1)

    # test_concurrent_threads_soft_limit_limit_reached
    # Concurrent threads limit is set to 10
    # Background query starts in a separate thread to reach this limit.
    # When this limit is reached the foreground query gets less than 6 threads despite the fact that it has settings max_threads=6
    node.query(
        f"""
        create or replace workload all settings max_concurrent_threads=10;
    """
    )
    def background_query():
        try:
            node.query(
                "SELECT count(*) FROM numbers_mt(1e11) settings max_threads=100, workload='all'",
                query_id="background_query",
            )
        except QueryRuntimeException:
            pass

    background_thread = threading.Thread(target=background_query)
    background_thread.start()

    def limit_reached():
        s_count = node.query(
            "SELECT sum(length(thread_ids)) FROM system.processes"
        ).strip()
        if s_count:
            count = int(s_count)
        else:
            count = 0
        return count >= 11

    while not limit_reached():
        time.sleep(0.1)

    node.query(
        "SELECT count(*) FROM numbers_mt(10000000) settings max_threads=6, workload='all'",
        query_id="test_compatibility",
    )
    assert_query(node, 'test_compatibility', 1, 4)

    node.query("KILL QUERY WHERE query_id = 'background_query' SYNC")
    background_thread.join()


def test_independent_pools():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all;
        create workload production in all settings max_concurrent_threads=15;
        create workload development in all settings max_concurrent_threads=10;
        create workload admin in all settings max_concurrent_threads=5;
    """
    )

    def query_thread(workload):
        node.query(
            f"select count(*) from numbers_mt(10000000) settings workload='{workload}', max_threads=100",
            query_id=f"test_{workload}",
        )

    threads = [
        threading.Thread(target=query_thread, args=('production',)),
        threading.Thread(target=query_thread, args=('development',)),
        threading.Thread(target=query_thread, args=('admin',)),
    ]

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert_query(node, 'test_production', 0, 15)
    assert_query(node, 'test_development', 0, 10)
    assert_query(node, 'test_admin', 0, 5)


def test_slot_allocation_fairness():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=2;
        create workload production in all settings weight=3;
        create workload development in all;
        create workload admin in all settings priority=-1;
    """
    )

    busy = BusyPeriod(workload="admin", max_concurrent_threads=10)

    stop_event = threading.Event()

    def query_thread(workload, max_threads=2):
        while not stop_event.is_set():
            node.query(
                f"select count(*) from numbers_mt(100000000) settings workload='{workload}', max_threads={max_threads}"
            )

    threads = []
    for i in range(10):
        threads.append(threading.Thread(target=query_thread, args=('production',)))
        threads.append(threading.Thread(target=query_thread, args=('development',)))

    for thread in threads:
        thread.start()

    time.sleep(1) # ensure real queries are queued
    busy.release()

    def enough_slots():
        return int(
            node.query(
                "select sum(dequeued_requests) from system.scheduler where resource='cpu' and type='fifo'"
            ).strip()
        ) > 100

    while not enough_slots():
        time.sleep(0.1)

    stop_event.set()

    for thread in threads:
        thread.join()

    production = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='cpu' and path ilike '%/production/%' and type='fifo'"
        ).strip()
    )

    development = int(
        node.query(
            f"select dequeued_requests from system.scheduler where resource='cpu' and path ilike '%/development/%' and type='fifo'"
        ).strip()
    )

    total = production + development
    if total > 1000: # do not check if CI is too slow to run 1000 threads in 3 seconds
        assert (abs(production / 3.0 - development) / total) < 0.05 # unfairness should be around 1 slot in theory, but we allow up to 5 percent
