#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/global_profiler.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        yield cluster
    finally:
        cluster.shutdown()


def test_global_thread_profiler(start_cluster):
    if node1.is_built_with_sanitizer():
        return

    node1.query(
        "CREATE TABLE t (key UInt32, value String) Engine = MergeTree() ORDER BY key"
    )

    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")
    node1.query("INSERT INTO t SELECT number, toString(number) from numbers(100)")

    time.sleep(5)

    node1.query("SYSTEM FLUSH LOGS")

    assert (
        int(
            node1.query(
                "SELECT count() FROM system.trace_log where trace_type='Real' and query_id = ''"
            ).strip()
        )
        > 0
    )
