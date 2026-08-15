#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster, CLICKHOUSE_START_COMMAND
from helpers.client import CommandRequest, QueryRuntimeException


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/min_cpu_busy_time.xml"],
    clickhouse_start_cmd=f"taskset -c 0 {CLICKHOUSE_START_COMMAND}",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/early_drop.xml"],
    clickhouse_start_cmd=f"taskset -c 1 {CLICKHOUSE_START_COMMAND}",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_overload(started_cluster):
    queries = []
    for i in range(4):
        queries.append(node1.get_query_request("select * from numbers(1e18) format null", ignore_error=True, timeout=15))

    def wait_for_queries():
        for query in queries:
            query.get_answer()

    for i in range(30):
        try:
            node1.query("select 1 settings min_os_cpu_wait_time_ratio_to_throw=2, max_os_cpu_wait_time_ratio_to_throw=6")
        except QueryRuntimeException as ex:
            assert "(SERVER_OVERLOADED)" in str(ex), "Only server overloaded error is expected"
            wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
            return
        time.sleep(0.3)

    assert False, "Expected to get the server overloaded error at least once"
    wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs


def test_drop_connections(started_cluster):
    queries = []
    for i in range(4):
        queries.append(node2.get_query_request("select * from numbers(1e18) format null", ignore_error=True, timeout=15))

    def wait_for_queries():
        for query in queries:
            query.get_answer()

    for i in range(30):
        try:
            node2.query("select 1")
        except QueryRuntimeException as ex:
            assert "Connection reset by peer" in str(ex), "Only connection drop is expected"
            assert node2.contains_in_log("CPU is overloaded, CPU is waiting for execution way more than executing"), "Expected server overloaded error in the log"
            assert node2.contains_in_log("probability used to decide whether to drop the connection"), "Expected message that the connection was dropped due to server overload in the log"
            wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
            return
        time.sleep(0.3)

    assert False, "Expected to drop the connection due to the server being overloaded at least once"
    wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
