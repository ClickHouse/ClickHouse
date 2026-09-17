#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/global_profiler.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()

        if node1.is_built_with_memory_sanitizer():
            pytest.skip("The sampling query profiler is unavailable under MemorySanitizer")

        config_path = "/etc/clickhouse-server/config.d/global_profiler.xml"
        node1.replace_in_config(config_path, ">0<", ">10000000<")
        node1.restart_clickhouse()

        yield cluster
    finally:
        cluster.shutdown()


def test_global_thread_profiler(start_cluster):
    if node1.is_built_with_sanitizer() or node1.is_built_with_llvm_coverage():
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
