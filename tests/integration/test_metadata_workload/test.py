# pylint: disable=unused-argument
# pylint: disable=redefined-outer-name
# pylint: disable=line-too-long

import threading
import time
import uuid

import pytest

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster, ClickHouseInstance

cluster = ClickHouseCluster(__file__)

# node_on: use_metadata_workload=1  -> metadata statements are scheduled under the `metadata_workload` setting.
node_on: ClickHouseInstance = cluster.add_instance(
    "node_on",
    stay_alive=True,
    main_configs=["configs/use_metadata_workload.xml"],
    with_zookeeper=True,
    cpu_limit=15,
)

# node_off: use_metadata_workload=0 (default) -> metadata statements are exempt from workload scheduling.
node_off: ClickHouseInstance = cluster.add_instance(
    "node_off",
    stay_alive=True,
    main_configs=["configs/no_metadata_workload.xml"],
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
    yield
    for node in (node_on, node_off):
        node.query(
            """
            drop table if exists t sync;
            drop table if exists t2 sync;
            drop table if exists t3 sync;
            drop view if exists v sync;
            drop workload if exists regular;
            drop workload if exists mdwl;
            drop workload if exists all;
            drop resource if exists query;
            """
        )


def setup_workloads(node) -> None:
    # Each workload admits a single query at a time.
    node.query(
        """
        create resource query (query);
        create workload all;
        create workload regular in all settings max_concurrent_queries=1;
        create workload mdwl in all settings max_concurrent_queries=1;
        """
    )


class SlotHolder:
    """Holds the only query slot of `workload` with a long-running query."""

    def __init__(self, node, workload: str):
        self.node = node
        self.query_id = f"holder_{uuid.uuid4().hex}"
        self.thread = threading.Thread(target=self._run, args=(workload,))

    def _run(self, workload: str) -> None:
        try:
            self.node.query(
                f"select sleepEachRow(1) from numbers(120) settings max_block_size=1, workload='{workload}'",
                query_id=self.query_id,
            )
        except QueryRuntimeException:
            pass  # killed at exit

    def __enter__(self):
        self.thread.start()
        # Presence in system.processes implies the slot was admitted: ProcessList::insert admits before publishing.
        deadline = time.time() + 60
        while self.node.query(f"select count() from system.processes where query_id = '{self.query_id}'").strip() == "0":
            assert time.time() < deadline, "the slot holder was never admitted"
            time.sleep(0.1)
        return self

    def __exit__(self, *args):
        self.node.query(f"kill query where query_id = '{self.query_id}' sync")
        self.thread.join()


def run(node, sql: str, **settings) -> None:
    node.query(sql, settings={"workload_admission_timeout_ms": 1000, **settings})


def run_times_out(node, sql: str, **settings) -> None:
    error = node.query_and_get_error(sql, settings={"workload_admission_timeout_ms": 1000, **settings})
    assert "QUERY_SLOT_ACQUISITION_TIMEOUT" in error, error


def test_server_setting_reflects_config() -> None:
    assert node_on.query("select value from system.server_settings where name='use_metadata_workload'").strip() == "1"
    assert node_off.query("select value from system.server_settings where name='use_metadata_workload'").strip() == "0"


def test_metadata_statements_exempt_when_disabled() -> None:
    setup_workloads(node_off)
    run(node_off, "create table t (x UInt64) engine=MergeTree order by x")
    with SlotHolder(node_off, "regular"):
        # The slot of 'regular' is taken: a regular query cannot be admitted.
        run_times_out(node_off, "select 1", workload="regular")

        # Metadata statements are exempt from workload scheduling.
        run(node_off, "create table t2 (x UInt64) engine=MergeTree order by x", workload="regular")
        run(node_off, "alter table t2 add column y UInt64", workload="regular")
        run(node_off, "create view v as select x from t2", workload="regular")
        run(node_off, "rename table t2 to t3", workload="regular")
        run(node_off, "truncate table t3", workload="regular")
        run(node_off, "drop view v sync", workload="regular")
        run(node_off, "drop table t3 sync", workload="regular")

        # Statements that may move a lot of data stay under `workload`.
        run_times_out(node_off, "create table t2 engine=MergeTree order by x as select 1 as x", workload="regular")
        run_times_out(node_off, "optimize table t final", workload="regular")


def test_metadata_statements_use_metadata_workload_when_enabled() -> None:
    setup_workloads(node_on)
    with SlotHolder(node_on, "mdwl"):
        # Metadata statements are scheduled under `metadata_workload`, whose slot is taken.
        run_times_out(node_on, "create table t (x UInt64) engine=MergeTree order by x", workload="regular", metadata_workload="mdwl")

        # Other statements stay under `workload`, whose slot is free.
        run(node_on, "select 1", workload="regular", metadata_workload="mdwl")
        run(node_on, "create table t2 engine=MergeTree order by x as select 1 as x", workload="regular", metadata_workload="mdwl")

    with SlotHolder(node_on, "regular"):
        # Metadata statements do not need the slot of `workload`.
        run(node_on, "create table t (x UInt64) engine=MergeTree order by x", workload="regular", metadata_workload="mdwl")
        run(node_on, "alter table t add column y UInt64", workload="regular", metadata_workload="mdwl")
        run_times_out(node_on, "optimize table t final", workload="regular", metadata_workload="mdwl")
