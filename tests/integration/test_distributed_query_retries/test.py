"""Test for the `distributed_query_retries` setting: a replica is killed while it is
executing a distributed query, and the initiator must retry the query on another replica."""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `stay_alive` is required on node1: every test restarts it, see `prepare_initiator`.
node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], stay_alive=True)
# `stay_alive` is required on node2: the test kills it and starts it again.
node2 = cluster.add_instance("node2", stay_alive=True)
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


def prepare_initiator():
    """Every test kills node2, and a killed replica keeps a non-zero error count in the initiator's
    connection pool. The pool tries the replicas with the fewest errors first, whatever
    `load_balancing` says, so without resetting those counters a subsequent test would run on node3
    and never exercise the retry. Restarting the initiator resets them."""

    node1.restart_clickhouse()
    node1.rotate_logs()


def run_query_and_kill_replica(settings, query=QUERY):
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: node1.query(query, settings=settings))

        try:
            wait_query_started_on(node2)
            node2.stop_clickhouse(kill=True)
            return future.result(timeout=120)
        finally:
            future.cancel()
            node2.start_clickhouse()


def test_no_retries_by_default(started_cluster):
    prepare_initiator()

    with pytest.raises(Exception):
        run_query_and_kill_replica(SETTINGS)

    assert not node1.contains_in_log("will retry (1/")


def test_retry_when_replica_is_killed(started_cluster):
    prepare_initiator()

    result = run_query_and_kill_replica({**SETTINGS, "distributed_query_retries": 2})

    assert result.strip() == "45"
    assert node1.contains_in_log("will retry (1/2)")


# The `ORDER BY` makes the remote server send its rows only at the end of the query, so the replica
# can be killed before any of them arrives, and `OFFSET` drops all of them on the initiator, so the
# query returns no rows at all. The final statistics of the query still come from the remote server
# (`rows_before_limit_at_least` is taken from the `ProfileInfo` packet), and `RemoteSource`
# accumulates them, so a retry must not report the numbers of the failed attempt again.
ZERO_ROWS_QUERY = "SELECT x FROM t_distr ORDER BY x + sleepEachRow(0.25) LIMIT 5 OFFSET 100 FORMAT JSON"


def test_statistics_are_reported_once_after_a_retry(started_cluster):
    prepare_initiator()

    result = json.loads(run_query_and_kill_replica({**SETTINGS, "distributed_query_retries": 2}, ZERO_ROWS_QUERY))

    assert node1.contains_in_log("will retry (1/2)")
    assert result["data"] == []
    # The replica has 10 rows, and they must be counted once, not once per attempt.
    assert result["rows_before_limit_at_least"] == 10
    assert result["statistics"]["rows_read"] == 10
