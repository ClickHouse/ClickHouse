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


@pytest.fixture(scope="function")
def with_custom_config(request):
    for name, server_settings in request.param.items():
        node = cluster.instances[name]
        xml = "".join(f"<{k}>{v}</{k}>" for k, v in server_settings.items())
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"echo '<clickhouse>{xml}</clickhouse>' > /etc/clickhouse-server/config.d/99-custom_config.xml",
            ]
        )
        node.query("system reload config")
    yield
    for name, server_settings in request.param.items():
        node = cluster.instances[name]
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"rm -f /etc/clickhouse-server/config.d/99-custom_config.xml",
            ]
        )
        node.query("system reload config")


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
            f"select count(*) from numbers_mt(10000000000) settings workload='{workload}', max_threads=100",
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
        # So any attempt to make a stricter checks here lead to flakiness due to described race condition. Workaround would require failpoint.
        # We assume threads will never be preempted and downscaled and upscaled again, so we can check ConcurrencyControlSlotsAcquired against limit
        assert_profile_event(
            node,
            query_id,
            "ConcurrencyControlSlotsAcquired",
            lambda x: x <= slots,
        )
        # Short preemptions may happen due to lags in the scheduler thread, but dowscales should not
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


# For debugging purposes
LOG = []
def mylog(message: str, *args) -> None:
    # Format a human-readable timestamp and append a tuple to LOG
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    LOG.append((timestamp, message, args))


class QueryPool:
    def __init__(self, num_queries: int, workload: str) -> None:
        self.num_queries: int = num_queries
        self.workload: str = workload
        self.stop_event: threading.Event = threading.Event()
        self.threads: list[threading.Thread] = []
        self.stopped: bool = True

    def start_random(self, max_billions: int, max_threads: int) -> None:
        assert self.stopped, "Pool is already running"

        def query_thread() -> None:
            while not self.stop_event.is_set():
                mylog(f"Running query in workload {self.workload}")
                node.query(
                    f"WITH {max_billions * 1000000000} AS T, (SELECT (T / 2) + (rand64() % T) / 2)::UInt64 AS N SELECT count(*) FROM numbers_mt(N) SETTINGS "
                    f"workload='{self.workload}', max_threads={max_threads}"
                )

        for _ in range(self.num_queries):
            self.threads.append(threading.Thread(target=query_thread, args=()))
        for thread in self.threads:
            thread.start()

    def start_fixed(self, billions: int, max_threads: int) -> None:
        assert self.stopped, "Pool is already running"

        def query_thread() -> None:
            while not self.stop_event.is_set():
                mylog(f"Running query in workload {self.workload}")
                node.query(
                    f"with (select {billions * 1000000000})::UInt64 as n select count(*) from numbers_mt(n) settings "
                    f"workload='{self.workload}', max_threads={max_threads}"
                )

        for _ in range(self.num_queries):
            self.threads.append(threading.Thread(target=query_thread, args=()))
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        mylog(f"Stopping workload {self.workload}")
        self.stop_event.set()
        for thread in self.threads:
            thread.join()
        self.threads.clear()
        self.stop_event = threading.Event()
        self.stopped = True
        mylog(f"Workload {self.workload} stopped")


def get_all_dequeued_costs() -> dict[str, float]:
    rows = node.query(
        "select path, dequeued_cost from system.scheduler where resource='cpu' and type='unified'"
    ).strip().split('\n')
    return {line.split()[0].split('/')[-1]: float(line.split()[1]) for line in rows if line}


# Checks that each workload receives specified share of CPU nanoseconds (dequeued_cost)
def ensure_shares(minimum_runtime: float, assertions: list[tuple[str, float]]) -> None:
    mylog("Starting ensure_shares")
    time.sleep(0.2) # Give opportunity to all workloads to start participation
    initial_costs = get_all_dequeued_costs()
    mylog(f"Initial costs: {initial_costs}")
    time.sleep(minimum_runtime)
    while True:
        time.sleep(0.1)
        current_costs = get_all_dequeued_costs()
        deltas = {workload: current_costs[workload] - initial_costs[workload] for workload, _ in assertions}
        total_delta = sum(deltas.values())
        mylog(f"Current deltas: {deltas}, total delta: {total_delta}")
        if total_delta > 0 and all(
            abs(deltas[workload] / total_delta - share) / share < 0.1
            for workload, share in assertions
        ):
            break
    mylog("Finished ensure_shares")


def test_threads_oversubscription():
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=1;
        create workload production in all;
    """
    )

    production = QueryPool(10, "production")
    production.start_fixed(1, 2)
    time.sleep(2)
    production.stop()


@pytest.mark.parametrize(
    "queries,threads,production_length,development_length,randomize",
    [
        pytest.param(10, 2, 1, 1, False, id="fixed_equal"),
        pytest.param(10, 2, 4, 0.5, False, id="fixed_longer_prd"),
        pytest.param(10, 2, 0.5, 4, False, id="fixed_longer_dev"),
        pytest.param(10, 2, 1, 1, True, id="random_equal"),
        pytest.param(10, 2, 4, 0.5, True, id="random_longer_prd"),
        pytest.param(10, 2, 0.5, 4, True, id="random_longer_dev"),
    ]
)
def test_cpu_time_fairness(queries, threads, production_length, development_length, randomize):

    # We use max_cpus=1 to make sure that we have voilated constraint.
    # In CI we should have at least one CPU core, so we never hit CPU bottleneck w/o hitting scheduler limit.
    # This turns ON fair scheduling and we test should not be flaky.
    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=8, max_cpus=1;
        create workload production in all settings weight=3;
        create workload development in all;
    """
    )

    production = QueryPool(queries, "production")
    development = QueryPool(queries, "development")

    if randomize:
        production.start_random(production_length, threads)
        development.start_random(development_length, threads)
    else:
        production.start_fixed(production_length, threads)
        development.start_fixed(development_length, threads)

    ensure_shares(
        0.4,
        [
            ("production", 0.75),
            ("development", 0.25),
        ],
    )
    production.stop()
    development.stop()


class DynamicQueryPool:
    def __init__(self, workload: str) -> None:
        self.workload: str = workload
        self._lock = threading.Lock()
        self._next_id: int = 1
        self._threads: dict[int, tuple[threading.Thread, threading.Event]] = {}

    def start(self, billions: int, max_threads: int) -> int:
        with self._lock:
            thread_id = self._next_id
            self._next_id += 1

        stop_event = threading.Event()

        def query_thread(local_stop_event: threading.Event, tid: int) -> None:
            while not local_stop_event.is_set():
                mylog(f"Running query in workload {self.workload}, thread_id={tid}")
                node.query(
                    f"with (select {billions * 1000000000})::UInt64 as n select count(*) from numbers_mt(n) settings "
                    f"workload='{self.workload}', max_threads={max_threads}"
                )

        thread = threading.Thread(target=query_thread, args=(stop_event, thread_id))
        with self._lock:
            self._threads[thread_id] = (thread, stop_event)
        thread.start()
        return thread_id

    def stop(self, thread_id: int) -> None:
        with self._lock:
            pair = self._threads.get(thread_id)
        if not pair:
            return
        thread, stop_event = pair
        mylog(f"Stopping workload {self.workload}, thread_id={thread_id}")
        stop_event.set()
        thread.join()
        with self._lock:
            self._threads.pop(thread_id, None)
        mylog(f"Workload {self.workload} thread_id={thread_id} stopped")


@pytest.mark.parametrize(
    "with_custom_config",
    [
        pytest.param(
            {'node': {"cpu_slot_preemption_timeout_ms": "1"}},
            id="cpu-slot-preemption-timeout-1ms",
        )
    ],
    indirect=True,
)
def test_downscaling(with_custom_config):
    if node.is_built_with_address_sanitizer():
        pytest.skip("doesn't fit in timeouts due to heavy workload")

    node.query(
        f"""
        create resource cpu (master thread, worker thread);
        create workload all settings max_concurrent_threads=8;
        create workload development in all;
    """
    )

    development = DynamicQueryPool("development")

    active_ids: list[int] = []
    try:
        for _ in range(2):
            # Gradually increase concurrency to 16
            for _ in range(16):
                tid = development.start(billions=1, max_threads=8)
                active_ids.append(tid)
                time.sleep(0.05)
            # Gradually decrease back to 0
            for tid in reversed(active_ids):
                development.stop(tid)
                time.sleep(0.05)
            active_ids.clear()
    finally:
        for tid in active_ids:
            development.stop(tid)
