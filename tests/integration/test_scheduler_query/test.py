# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

node: ClickHouseInstance = cluster.add_instance(
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


def assert_profile_event(node, query_id, profile_event, check) -> None:
    assert check(
        int(
            node.query(
                f"select ProfileEvents['{profile_event}'] from system.query_log where "
                "current_database = currentDatabase() and query_id = '{query_id}' and "
                "type = 'QueryFinish' order by query_start_time_microseconds desc limit 1"
            )
        )
    )


def test_create() -> None:
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

    def do_checks() -> None:
        common_select_part = "select count() from system.scheduler where path ilike"

        assert node.query(f"{common_select_part} '%/admin/%' and type='fifo'") == "1\n"

        assert (
            node.query(f"{common_select_part} '%/admin' and type='unified' and priority=0") == "1\n"
        )

        assert node.query(f"{common_select_part} '%/production/%' and type='fifo'") == "1\n"

        assert (
            node.query(f"{common_select_part} '%/production' and type='unified' and weight=9")
            == "1\n"
        )

        assert node.query(f"{common_select_part} '%/development/%' and type='fifo'") == "1\n"

        assert (
            node.query(
                f"{common_select_part} '%/all/%' and type='inflight_limit' and "
                "resource='query' and max_requests=20"
            )
            == "1\n"
        )

    do_checks()
    node.restart_clickhouse()  # Check that workloads persist
    do_checks()


class QueryPool:
    def __init__(self, num_queries: int, workload: str) -> None:
        self.num_queries: int = num_queries
        self.workload: str = workload
        self.stop_event: threading.Event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.stopped: bool = True

    def start(self) -> None:
        assert self.stopped, "Pool is already running"

        def query_thread() -> None:
            while not self.stop_event.is_set():
                node.query(
                    f"select count(*) from numbers_mt(100000000) settings "
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


def test_max_concurrent_queries() -> None:
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

    def concurrent_queries() -> int:
        return int(
            node.query(
                f"select value from system.metrics where name='ConcurrentQueryAcquired'"
            ).strip()
        )

    def ensure_total_concurrency(limit: int) -> None:
        for _ in range(10):
            assert concurrent_queries() <= limit
            time.sleep(0.1)
        while concurrent_queries() < limit:
            time.sleep(0.1)

    def inflight_queries(workload) -> int:
        return int(
            node.query(
                f"select inflight_requests from system.scheduler where "
                f"path like '%/{workload}/semaphore' and resource='query'"
            ).strip()
        )

    def ensure_workload_concurrency(workload, limit: int) -> None:
        for _ in range(10):
            assert inflight_queries(workload) <= limit
            time.sleep(0.1)
        while inflight_queries(workload) < limit:
            time.sleep(0.1)

    admin = QueryPool(6, "admin")
    production = QueryPool(6, "production")
    development = QueryPool(6, "development")

    production.start()
    ensure_total_concurrency(4)
    ensure_workload_concurrency("main", 4)
    production.stop()

    development.start()
    ensure_total_concurrency(2)
    ensure_workload_concurrency("development", 2)
    development.stop()

    production.start()
    development.start()
    ensure_total_concurrency(4)
    admin.start()
    ensure_total_concurrency(6)
    ensure_workload_concurrency("all", 6)
    production.stop()
    development.stop()
    admin.stop()
