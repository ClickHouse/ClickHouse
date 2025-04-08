#!/usr/bin/env python3

import time

import pytest

from helpers.cluster import ClickHouseCluster, CLICKHOUSE_START_COMMAND
from helpers.client import CommandRequest, QueryRuntimeException


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/min_cpu_busy_time.xml"],
    clickhouse_start_cmd=f"taskset -c 0 {CLICKHOUSE_START_COMMAND}",
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_overload(started_cluster):
    for i in range(4):
        node.get_query_request("select * from numbers(1e18) format null", ignore_error=True, timeout=30)

    for i in range(30):
        try:
            node.query("select 1 settings min_os_cpu_wait_time_ratio_to_throw=2, max_os_cpu_wait_time_ratio_to_throw=6")
        except QueryRuntimeException as ex:
            assert "(SERVER_OVERLOADED)" in str(ex), "Only server overloaded error is expected"
            return
        time.sleep(0.5)

    assert False, "Expected to get the server overloaded error at least once"
