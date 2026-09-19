# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

# node_on: use_ddl_workload=1  -> DDL is scheduled under the `ddl_workload` setting.
node_on: ClickHouseInstance = cluster.add_instance(
    "node_on",
    stay_alive=True,
    main_configs=["configs/use_ddl_workload.xml"],
    with_zookeeper=True,
    cpu_limit=15,
)

# node_off: use_ddl_workload=0 (default) -> DDL is exempt from workload admission (hard skip).
node_off: ClickHouseInstance = cluster.add_instance(
    "node_off",
    stay_alive=True,
    main_configs=["configs/no_ddl_workload.xml"],
    with_zookeeper=True,
    cpu_limit=15,
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def clean():
    for node in (node_on, node_off):
        node.query(
            """
            drop table if exists ddl_dst_0 sync;
            drop table if exists ddl_dst_1 sync;
            drop workload if exists regular;
            drop workload if exists ddlwl;
            drop workload if exists all;
            drop resource if exists query;
            """
        )
    yield


def setup_workloads(node) -> None:
    node.query(
        """
        create resource query (query);
        create workload all settings max_concurrent_queries=20;
        create workload regular in all settings max_concurrent_queries=10;
        create workload ddlwl in all settings max_concurrent_queries=10;
        """
    )


def inflight(node, workload: str) -> int:
    return int(
        node.query(
            f"select inflight_requests from system.scheduler where "
            f"path like '%/{workload}/semaphore' and resource='query'"
        ).strip()
        or "0"
    )


class QueryPool:
    """Keep `num` copies of a query continuously in flight (a single query finishes too fast to
    observe an in-flight slot), mirroring tests/integration/test_scheduler_query."""

    def __init__(self, node, make_query, num: int = 2):
        self.node = node
        self.make_query = make_query  # i -> SQL
        self.num = num
        self.threads: list = []
        self.stop_event = threading.Event()
        self.error: str = None

    def start(self) -> None:
        def run(i: int) -> None:
            while not self.stop_event.is_set():
                try:
                    self.node.query(self.make_query(i))
                except Exception as ex:  # noqa: BLE001
                    self.error = str(ex)
                    return

        for i in range(self.num):
            t = threading.Thread(target=run, args=(i,))
            self.threads.append(t)
            t.start()

    def stop(self) -> None:
        self.stop_event.set()
        for t in self.threads:
            t.join()
        self.threads.clear()


def wait_inflight_at_least(node, workload: str, limit: int, timeout: float = 60.0) -> None:
    deadline = time.time() + timeout
    last = 0
    while time.time() < deadline:
        last = inflight(node, workload)
        if last >= limit:
            return
        time.sleep(0.1)
    raise AssertionError(f"inflight('{workload}') never reached {limit} (last={last})")


def _ddl_pool(node, workload_settings: str) -> QueryPool:
    # A CPU-bound CREATE ... AS SELECT (DDL) that holds its query slot while the SELECT runs.
    return QueryPool(
        node,
        lambda i: (
            f"create or replace table ddl_dst_{i} engine=MergeTree order by tuple() as "
            f"select count(*) from numbers_mt(100000000) settings {workload_settings}, max_threads=2"
        ),
        num=2,
    )


def test_server_setting_reflects_config() -> None:
    assert node_on.query("select value from system.server_settings where name='use_ddl_workload'").strip() == "1"
    assert node_off.query("select value from system.server_settings where name='use_ddl_workload'").strip() == "0"


def test_ddl_uses_ddl_workload_when_enabled() -> None:
    # use_ddl_workload=1: DDL is admitted under `ddl_workload` ('ddlwl'), NOT the session
    # `workload` ('regular').
    setup_workloads(node_on)
    pool = _ddl_pool(node_on, "ddl_workload='ddlwl', workload='regular'")
    pool.start()
    try:
        wait_inflight_at_least(node_on, "ddlwl", 1)
        assert inflight(node_on, "regular") == 0, "DDL must not run under the session workload"
    finally:
        pool.stop()
    assert pool.error is None, pool.error


def test_regular_query_uses_workload_when_enabled() -> None:
    # A non-DDL query is unaffected: it stays under the `workload` setting ('regular').
    setup_workloads(node_on)
    pool = QueryPool(
        node_on,
        lambda i: "select count(*) from numbers_mt(100000000) settings workload='regular', max_threads=2",
        num=2,
    )
    pool.start()
    try:
        wait_inflight_at_least(node_on, "regular", 1)
        assert inflight(node_on, "ddlwl") == 0
    finally:
        pool.stop()
    assert pool.error is None, pool.error


def test_ddl_exempt_when_disabled() -> None:
    # use_ddl_workload=0 (default): DDL is exempt from workload admission (hard skip). A regular
    # query under 'regular' is still admitted (control); a DDL under the same workload never takes
    # a query slot.
    setup_workloads(node_off)

    control = QueryPool(
        node_off,
        lambda i: "select count(*) from numbers_mt(100000000) settings workload='regular', max_threads=2",
        num=2,
    )
    control.start()
    try:
        wait_inflight_at_least(node_off, "regular", 1)  # regular queries ARE admitted
    finally:
        control.stop()

    ddl = _ddl_pool(node_off, "workload='regular'")
    ddl.start()
    try:
        # While the DDL pool runs, its slots must never appear under 'regular' (it is exempt).
        deadline = time.time() + 10.0
        while time.time() < deadline:
            assert inflight(node_off, "regular") == 0, "DDL must be exempt from workload admission when use_ddl_workload=0"
            time.sleep(0.2)
    finally:
        ddl.stop()
    assert ddl.error is None, ddl.error
