"""Test for the `distributed_query_retries` setting: a replica is killed while it is
executing a distributed query, and the initiator must retry the query on another replica."""

import json
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# `stay_alive` is required on node1: every test restarts it, see `prepare_initiator`.
# ZooKeeper is for the replicated table of `test_retry_on_stale_local_replica_path`.
node1 = cluster.add_instance("node1", main_configs=["configs/remote_servers.xml"], with_zookeeper=True, stay_alive=True)
# `stay_alive` is required on node2/node3: tests kill and restart them.
node2 = cluster.add_instance("node2", with_zookeeper=True, stay_alive=True)
node3 = cluster.add_instance("node3", with_zookeeper=True, stay_alive=True)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        for node in (node2, node3):
            node.query("CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x")
            node.query("INSERT INTO t SELECT number FROM numbers(10)")

        node1.query("CREATE TABLE t_distr (x UInt64) ENGINE = Distributed('two_replicas', 'default', 't')")
        node1.query("CREATE TABLE t_two_shards (x UInt64) ENGINE = Distributed('two_shards', 'default', 't')")

        for node in (node1, node2, node3):
            node.query(
                "CREATE TABLE t_repl (x UInt64) "
                f"ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_repl', '{node.name}') ORDER BY x"
            )
        node1.query("CREATE TABLE t_repl_distr (x UInt64) ENGINE = Distributed('three_replicas', 'default', 't_repl')")

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


def wait_query_finished_on(node):
    for _ in range(100):
        if node.query("SELECT count() FROM system.processes WHERE query LIKE '%sleepEachRow%' AND is_initial_query = 0").strip() == "0":
            return
        time.sleep(0.1)
    raise Exception(f"The query did not finish on {node.name}")


def initiator_read_rows(query_id):
    node1.query("SYSTEM FLUSH LOGS")
    return int(
        node1.query(
            f"SELECT read_rows FROM system.query_log WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
            f"ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()
    )


def remote_read_rows(node, initial_query_id):
    node.query("SYSTEM FLUSH LOGS")
    return int(
        node.query(
            f"SELECT max(read_rows) FROM system.query_log "
            f"WHERE initial_query_id = '{initial_query_id}' AND type = 'QueryFinish' AND is_initial_query = 0"
        ).strip()
        or "0"
    )


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


def test_deferred_progress_not_lost_when_shard_cancelled_by_limit(started_cluster):
    """`distributed_query_retries > 0` defers Progress; LIMIT must still flush it when closing a shard.

    `sleepEachRow` + a small `interactive_delay` make each shard send Progress before the first Data
    row, so the shard cancelled by LIMIT has non-empty deferred progress on the initiator.
    """

    prepare_initiator()

    limit_query = (
        "SELECT x FROM t_two_shards WHERE NOT ignore(sleepEachRow(0.3)) LIMIT 1 "
        "SETTINGS use_hedged_requests = 0, max_threads = 2, log_queries = 1, "
        "interactive_delay = 50000, distributed_query_retry_interval_ms = 100"
    )

    qid0 = "dq_retry_limit_progress_0"
    qid2 = "dq_retry_limit_progress_2"
    node1.query(limit_query + ", distributed_query_retries = 0", query_id=qid0)
    node1.query(limit_query + ", distributed_query_retries = 2", query_id=qid2)

    rows0 = initiator_read_rows(qid0)
    rows2 = initiator_read_rows(qid2)
    remote0 = [remote_read_rows(node2, qid0), remote_read_rows(node3, qid0)]
    remote2 = [remote_read_rows(node2, qid2), remote_read_rows(node3, qid2)]

    # Both shards must have executed and read rows; otherwise initiator equality can pass vacuously
    # when the cancelled shard never contributed Progress in either run.
    assert min(remote0) > 0 and min(remote2) > 0
    assert rows0 > 0
    assert rows2 == rows0


def run_query_and_kill_replica_while_retry_is_paused(query, settings, query_id):
    """Kill node2 mid-query, hold the retry at the fail point after the failed replica has been
    penalized but before the query is re-sent, bring node2 back up, and release the retry, so both
    replicas are available for the retry and only the penalty can make it prefer node3."""

    node1.query("SYSTEM ENABLE FAILPOINT remote_query_executor_prepare_retry_pause")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: node1.query(query, settings=settings, query_id=query_id))
        try:
            wait_query_started_on(node2)
            node2.stop_clickhouse(kill=True)
            node1.query("SYSTEM WAIT FAILPOINT remote_query_executor_prepare_retry_pause PAUSE", timeout=60)
            node2.start_clickhouse()
            node1.query("SYSTEM DISABLE FAILPOINT remote_query_executor_prepare_retry_pause")

            return future.result(timeout=120)
        finally:
            future.cancel()
            try:
                node1.query("SYSTEM DISABLE FAILPOINT remote_query_executor_prepare_retry_pause")
            except Exception:
                pass


def assert_retry_ran_on_node3(query_id):
    assert node1.contains_in_log("will retry (1/2)")
    # The retry must run on node3: `in_order` alone would reconnect to node2, which is up again,
    # but its mid-query failure must have moved it to the back of the failover order.
    assert remote_read_rows(node3, query_id) == 10
    assert remote_read_rows(node2, query_id) == 0


@pytest.mark.parametrize("use_hedged_requests", [0, 1])
def test_retry_prefers_another_replica(started_cluster, use_hedged_requests):
    """A replica that failed mid-query must be penalized in the connection pool, so the retry
    connects to another replica even under `load_balancing = 'in_order'` and even when the failed
    replica is reachable again by the time of the retry (e.g. after a transient network error).

    With `use_hedged_requests = 1`, `HedgedConnections` drops the failed replica before the error
    reaches the retry, so it must remember which replica failed instead of penalizing nobody (or a
    surviving hedge)."""

    prepare_initiator()

    query_id = f"dq_retry_prefers_another_replica_hedged_{use_hedged_requests}"
    result = run_query_and_kill_replica_while_retry_is_paused(
        QUERY, {**SETTINGS, "use_hedged_requests": use_hedged_requests, "distributed_query_retries": 2}, query_id
    )

    assert result.strip() == "45"
    assert_retry_ran_on_node3(query_id)


def wait_local_replica_is_stale(node, table, min_delay):
    for _ in range(100):
        delay = int(node.query(f"SELECT absolute_delay FROM system.replicas WHERE table = '{table}'").strip())
        if delay >= min_delay:
            return
        time.sleep(0.2)
    raise Exception(f"The replica of {table} on {node.name} did not become stale")


@pytest.mark.parametrize("use_hedged_requests", [0, 1])
def test_retry_on_stale_local_replica_path(started_cluster, use_hedged_requests):
    """With `prefer_localhost_replica = 1` and a stale local replica, the query is sent to a remote
    replica through the lazy pipe (`ReadFromRemote::addLazyPipe`), which acquires the connections up
    front. The retry must reacquire them from the pool and run on another replica as well."""

    prepare_initiator()

    for node in (node2, node3):
        node.query("SYSTEM SYNC REPLICA t_repl")
    node1.query("SYSTEM STOP FETCHES t_repl")
    try:
        # A fetch that node1 cannot process keeps an entry in its replication queue, and the age of
        # that entry is the replica's absolute delay.
        node2.query("INSERT INTO t_repl SELECT number FROM numbers(10)")
        node3.query("SYSTEM SYNC REPLICA t_repl")
        wait_local_replica_is_stale(node1, "t_repl", 2)

        query_id = f"dq_retry_stale_local_replica_hedged_{use_hedged_requests}"
        result = run_query_and_kill_replica_while_retry_is_paused(
            "SELECT sum(x + sleepEachRow(0.25)) FROM t_repl_distr",
            {
                **SETTINGS,
                "use_hedged_requests": use_hedged_requests,
                "distributed_query_retries": 2,
                "prefer_localhost_replica": 1,
                "max_replica_delay_for_distributed_queries": 1,
            },
            query_id,
        )

        assert result.strip() == "45"
        assert node1.contains_in_log("Local replica of shard 1 is stale")
        assert_retry_ran_on_node3(query_id)
    finally:
        node1.query("SYSTEM START FETCHES t_repl")
        node2.query("TRUNCATE TABLE t_repl")
        for node in (node1, node3):
            node.query("SYSTEM SYNC REPLICA t_repl")


def test_no_resend_after_finish_during_prepare_retry_pause(started_cluster):
    """finish() during prepare-retry (after sent_query cleared) must not re-send; query completes
    from the other shard while the killed shard stays down."""

    prepare_initiator()
    node1.query("SYSTEM ENABLE FAILPOINT remote_query_executor_prepare_retry_pause")

    query = (
        "SELECT x FROM t_two_shards WHERE NOT ignore(sleepEachRow(0.3)) LIMIT 1 "
        "SETTINGS use_hedged_requests = 0, max_threads = 2, "
        "distributed_query_retries = 2, distributed_query_retry_interval_ms = 50"
    )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(lambda: node1.query(query))
        try:
            wait_query_started_on(node2)
            wait_query_started_on(node3)
            node3.stop_clickhouse(kill=True)
            node1.query("SYSTEM WAIT FAILPOINT remote_query_executor_prepare_retry_pause PAUSE", timeout=60)
            # While the node3 executor remains paused, node2 satisfying LIMIT closes upstream ports
            # and `finish()` runs on the paused executor. The secondary query on node2 ending is the
            # observable side effect of that LIMIT completion.
            wait_query_finished_on(node2)
            node1.query("SYSTEM DISABLE FAILPOINT remote_query_executor_prepare_retry_pause")
            # node3 is still down: success means the paused executor did not need a successful resend.
            assert future.result(timeout=120).strip() != ""
            assert node1.contains_in_log("will retry (1/2)")
        finally:
            future.cancel()
            try:
                node1.query("SYSTEM DISABLE FAILPOINT remote_query_executor_prepare_retry_pause")
            except Exception:
                pass
            node3.start_clickhouse()
