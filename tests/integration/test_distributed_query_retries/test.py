"""Test for the `distributed_query_retries` setting: a replica is killed while it is
executing a distributed query, and the initiator must retry the query on another replica."""

import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"])
node2 = cluster.add_instance("node2")
node3 = cluster.add_instance("node3")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node2, node3):
            node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO t SELECT number FROM numbers(10)")

        node1.query("CREATE TABLE t_distr (x UInt64) ENGINE = Distributed('two_replicas', 'default', 't')")

        yield cluster
    finally:
        cluster.shutdown()


# The aggregation returns a single block at the end of the query, and sleepEachRow makes the
# query slow enough to kill the replica before that block (and hence before any result data)
# is sent to the initiator.
QUERY = "SELECT sum(x + sleepEachRow(0.25)) FROM t_distr"

SETTINGS = {
    # `in_order` makes the first attempt always go to node2, so killing node2 exercises the retry.
    "load_balancing": "in_order",
    "use_hedged_requests": 0,
    "max_threads": 1,
    "distributed_query_retry_interval_ms": 100,
}


def wait_query_started_on(node):
    for _ in range(100):
        if node.query("SELECT count() FROM system.processes WHERE query LIKE '%sleepEachRow%' AND is_initial_query = 0").strip() != "0":
            return
        time.sleep(0.1)
    raise Exception(f"The query did not start on {node.name}")


def run_query_and_kill_replica(settings):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: node1.query(QUERY, settings=settings))

        try:
            wait_query_started_on(node2)
            node2.stop_clickhouse(kill=True)
            return future.result(timeout=120)
        finally:
            future.cancel()
            node2.start_clickhouse()


def test_no_retries_by_default(started_cluster):
    node1.rotate_logs()

    with pytest.raises(Exception):
        run_query_and_kill_replica(SETTINGS)

    assert not node1.contains_in_log("will retry (1/")


def test_retry_when_replica_is_killed(started_cluster):
    node1.rotate_logs()

    result = run_query_and_kill_replica({**SETTINGS, "distributed_query_retries": 2})

    assert result.strip() == "45"
    assert node1.contains_in_log("will retry (1/2)")
