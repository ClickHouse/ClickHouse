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

node: ClickHouseInstance = cluster.add_instance("node", stay_alive=True)


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
    node.query(
        """
        drop database if exists db2 sync;
        drop table if exists t sync;
        drop table if exists t2 sync;
        drop table if exists t3 sync;
        drop view if exists v sync;
        drop function if exists f;
        drop workload if exists w2;
        drop workload if exists regular;
        drop workload if exists all;
        drop resource if exists query;
        """
    )


class SlotHolder:
    """Holds the only query slot of `workload` with a long-running query."""

    def __init__(self, workload: str):
        self.query_id = f"holder_{uuid.uuid4().hex}"
        self.thread = threading.Thread(target=self._run, args=(workload,))

    def _run(self, workload: str) -> None:
        try:
            node.query(
                f"select sleepEachRow(1) from numbers(120) settings max_block_size=1, workload='{workload}'",
                query_id=self.query_id,
            )
        except QueryRuntimeException:
            pass  # killed at exit

    def __enter__(self):
        self.thread.start()
        # Presence in system.processes implies the slot was admitted: ProcessList::insert admits before publishing.
        deadline = time.time() + 60
        while node.query(f"select count() from system.processes where query_id = '{self.query_id}'").strip() == "0":
            assert time.time() < deadline, "the slot holder was never admitted"
            time.sleep(0.1)
        return self

    def __exit__(self, *args):
        node.query(f"kill query where query_id = '{self.query_id}' sync")
        self.thread.join()


def run(sql: str) -> None:
    node.query(sql, settings={"workload": "regular", "workload_admission_timeout_ms": 1000})


def run_times_out(sql: str) -> None:
    error = node.query_and_get_error(sql, settings={"workload": "regular", "workload_admission_timeout_ms": 1000})
    assert "QUERY_SLOT_ACQUISITION_TIMEOUT" in error, error


def test_metadata_statements_are_exempt() -> None:
    # `regular` admits a single query at a time.
    node.query(
        """
        create resource query (query);
        create workload all;
        create workload regular in all settings max_concurrent_queries=1;
        create table t (x UInt64) engine=MergeTree order by x;
        """
    )
    with SlotHolder("regular"):
        # The slot of `regular` is taken: a regular query cannot be admitted.
        run_times_out("select 1")

        # Metadata statements are exempt from workload scheduling.
        run("create table t2 (x UInt64) engine=MergeTree order by x")
        run("alter table t2 add column y UInt64")
        run("alter table t2 update y = 1 where 1")
        run("rename table t2 to t3")
        run("truncate table t3")
        run("detach table t3 sync")
        run("attach table t3")
        run("drop table t3 sync")
        run("alter table t drop partition tuple()")
        run("create database db2 engine=Atomic")
        run("drop database db2 sync")
        run("create workload w2 in all")
        run("drop workload w2")
        run("create function f as (x) -> x + 1")
        run("drop function f")
        run("system reload config")

        # Statements that may move a lot of data stay under `workload`.
        run_times_out("create table t2 engine=MergeTree order by x as select 1 as x")
        run_times_out("create table t2 as numbers(10)")
        run_times_out("create view v as select x from t")
        run_times_out("optimize table t final")
        run_times_out("alter table t freeze")
        run_times_out("alter table t attach partition tuple()")
