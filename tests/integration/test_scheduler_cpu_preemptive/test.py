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
    main_configs=[
        "configs/cpu_slot_preemption.xml",
    ],
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


def test_create_workload():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=100;
        create workload admin in all settings priority=0;
        create workload production in all settings priority=1, weight=9;
        create workload development in all settings priority=1, weight=1, max_cpus=2;
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

    def assert_query(node, query_id, slots):
        node.query("SYSTEM FLUSH LOGS")
        # Note that we cannot guarantee that all slots that should be (a) granted, (b) acquired and (c) passed to a thread, will actually undergo even the first stage before query finishes
        # So any attempt to make a stricter checks here lead to flakyiness due to described race condition. Workaround would require failpoint.
        # We assume threads will never be preempted and downscaled and upscaled again, so we cna check ConcurrencyControlSlotsAcquired against limit
        assert_profile_event(
            node,
            query_id,
            "ConcurrencyControlSlotsAcquired",
            lambda x: x <= slots,
        )
        assert_profile_event(
            node,
            query_id,
            "ConcurrencyControlPreemptions",
            lambda x: x == 0,
        )
        assert_profile_event(
            node,
            query_id,
            "ConcurrencyControlDownscales",
            lambda x: x == 0,
        )
        # NOTE: checking thread_ids length is pointless, because query could downscale and then upscale again, gaining more threads than slots

    assert_query(node, 'test_production', 15)
    assert_query(node, 'test_development', 10)
    assert_query(node, 'test_admin', 5)


class QueryPool:
    def __init__(self, num_queries: int, workload: str) -> None:
        self.num_queries: int = num_queries
        self.workload: str = workload
        self.stop_event: threading.Event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.stopped: bool = True

    def start(self, max_number) -> None:
        assert self.stopped, "Pool is already running"

        def query_thread() -> None:
            while not self.stop_event.is_set():
                node.query(
                    f"with (select rand64() % {max_number * 1000000000})::UInt64 as n select count(*) from numbers_mt(n) settings "
                    f"workload='{self.workload}', max_threads=2"
                )

        for _ in range(self.num_queries):
            self.threads.append(threading.Thread(target=query_thread, args=()))
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        for thread in self.threads:
            thread.join()
        self.threads.clear()
        self.stop_event = threading.Event()
        self.stopped = True


def get_all_dequeued_costs() -> dict[str, float]:
    rows = node.query(
        "select path, dequeued_cost from system.scheduler where resource='cpu' and type='unified'"
    ).strip().split('\n')
    return {line.split()[0].split('/')[-1]: float(line.split()[1]) for line in rows if line}


# Checks that each workload receives specified share of CPU nanoseconds (dequeued_cost)
def ensure_shares(minimum_runtime: float, assertions: list[tuple[str, float]]) -> None:
    initial_costs = get_all_dequeued_costs()
    time.sleep(minimum_runtime)
    while True:
        time.sleep(0.1)
        current_costs = get_all_dequeued_costs()
        deltas = {workload: current_costs[workload] - initial_costs[workload] for workload, _ in assertions}
        total_delta = sum(deltas.values())
        if total_delta > 0 and all(
            abs(deltas[workload] / total_delta - share) / share < 0.1
            for workload, share in assertions
        ):
            break


def test_cpu_time_fairness():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=8;
        create workload production in all settings weight=3;
        create workload development in all;
        create workload admin in all settings priority=-1;
    """
    )

    admin = QueryPool(6, "admin")
    production = QueryPool(6, "production")
    development = QueryPool(6, "development")

    production.start(10)
    development.start(10)
    ensure_shares(
        0.2,
        [
            ("production", 0.75),
            ("development", 0.25),
        ],
    )
    production.stop()
    development.stop()

    production.start(100)
    development.start(10)
    ensure_shares(
        0.2,
        [
            ("production", 0.75),
            ("development", 0.25),
        ],
    )
    production.stop()
    development.stop()

    production.start(10)
    development.start(100)
    ensure_shares(
        0.2,
        [
            ("production", 0.75),
            ("development", 0.25),
        ],
    )
    production.stop()
    development.stop()
