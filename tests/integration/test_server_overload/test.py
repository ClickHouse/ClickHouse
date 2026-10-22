#!/usr/bin/env python3

import math
import re
import time

import pytest

from helpers.cluster import ClickHouseCluster, CLICKHOUSE_START_COMMAND
from helpers.client import QueryRuntimeException


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


CPU_OVERLOAD_RAMP_RE = re.compile(
    r"metric\) is ([-+0-9.eE]+)\. "
    r"Min ratio for error (?:\([^)]*\) )?([-+0-9.eE]+), "
    r"max ratio for error (?:\([^)]*\) )?([-+0-9.eE]+), "
    r"probability used to decide whether to (?:discard the query|drop the connection) ([-+0-9.eE]+)\."
)


def assert_cpu_overload_ramp(message):
    """The rejection came from the CPU overload throttle, and the probability it reported is the
    linear interpolation of the ratio it reported between the min and max ratio settings."""
    matches = CPU_OVERLOAD_RAMP_RE.findall(message)
    assert matches, "Expected the CPU overload throttle to report the ratio and the probability it used"
    for ratio, min_ratio, max_ratio, probability in matches:
        ratio, min_ratio, max_ratio, probability = map(float, (ratio, min_ratio, max_ratio, probability))
        expected = (min(max(min_ratio, ratio), max_ratio) - min_ratio) / (max_ratio - min_ratio)
        assert math.isclose(probability, expected, abs_tol=1e-6), \
            f"Probability {probability} does not match the ramp between {min_ratio} and {max_ratio} at ratio {ratio}"


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
        queries.append(node1.get_query_request("select * from numbers(1e18) format null", ignore_error=True, timeout=30))

    def wait_for_queries():
        for query in queries:
            query.get_answer()

    for i in range(60):
        try:
            # Rejection is probabilistic, ramping linearly from min ratio to max ratio, so probe in a
            # loop; a max close to min keeps the per-probe probability high under this load.
            node1.query("select 1 settings min_os_cpu_wait_time_ratio_to_throw=1, max_os_cpu_wait_time_ratio_to_throw=1.5")
        except QueryRuntimeException as ex:
            assert "(SERVER_OVERLOADED)" in str(ex), "Only server overloaded error is expected"
            assert_cpu_overload_ramp(str(ex))
            wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
            return
        time.sleep(0.3)

    assert False, "Expected to get the server overloaded error at least once"
    wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs


def test_drop_connections(started_cluster):
    queries = []
    for i in range(4):
        queries.append(node2.get_query_request("select * from numbers(1e18) format null", ignore_error=True, timeout=30))

    def wait_for_queries():
        for query in queries:
            query.get_answer()

    for i in range(60):
        try:
            node2.query("select 1")
        except QueryRuntimeException as ex:
            assert "Connection reset by peer" in str(ex), "Only connection drop is expected"
            assert node2.contains_in_log("CPU is overloaded, CPU is waiting for execution way more than executing"), "Expected server overloaded error in the log"
            assert_cpu_overload_ramp(node2.grep_in_log("probability used to decide whether to drop the connection"))
            wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
            return
        time.sleep(0.3)

    assert False, "Expected to drop the connection due to the server being overloaded at least once"
    wait_for_queries() # Needed for flaky check to make sure CPU is not loaded with queries from previous runs
