"""
The callers of a Keeper client share one connection attempt. A query stops waiting for it
after get_zookeeper_lock_acquire_timeout_ms or when it is cancelled; a caller without a query
context is not limited by the setting.
"""

import concurrent.futures
import uuid

import pytest

from helpers.cluster import ClickHouseCluster, QueryRuntimeException
from helpers.test_tools import assert_eq_with_retry

cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node = cluster.add_instance(
    "node",
    with_zookeeper=True,
    main_configs=["configs/zookeeper.xml", "configs/disable_ddl.xml", "configs/auxiliary_zookeepers.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_for_query(query_id):
    assert_eq_with_retry(
        node,
        f"SELECT count() FROM system.processes WHERE query_id = '{query_id}'",
        "1",
        retry_count=300,
        sleep_time=0.1,
    )


def session_expirations():
    return int(node.query("SELECT value FROM system.metrics WHERE metric = 'ZooKeeperSessionExpired'"))


@pytest.mark.parametrize("zk_name", ["default", "zookeeper2"])
def test_queries_share_connection_attempt(started_cluster, zk_name):
    zk_filter = "" if zk_name == "default" else f" AND zookeeperName = '{zk_name}'"
    base_query = f"SELECT * FROM system.zookeeper WHERE path = '/'{zk_filter} LIMIT 1"
    zk_prefix = "" if zk_name == "default" else f"{zk_name}:"
    failpoint = (
        "context_zookeeper_connection_attempt_pause"
        if zk_name == "default"
        else "context_auxiliary_zookeeper_connection_attempt_pause"
    )
    suffix = uuid.uuid4().hex
    # Every query makes one Keeper try, so a waiter gets the attempt's own error.
    no_limit = {"get_zookeeper_lock_acquire_timeout_ms": 0, "insert_keeper_max_retries": 0}

    def run(query, query_id, settings):
        try:
            node.query(query, query_id=query_id, settings=settings, timeout=120)
            return None
        except QueryRuntimeException as e:
            return str(e)

    node.query(base_query)
    with cluster.pause_container("zoo1"):
        # Once this query fails, the Keeper session is expired.
        with pytest.raises(QueryRuntimeException) as e:
            node.query(base_query)
        assert "KEEPER_EXCEPTION" in str(e.value)
        expirations = session_expirations()

        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
                # Takes the client outside a query pipeline; the fail point pauses its connection attempt with no lock held.
                connector = executor.submit(
                    run,
                    f"SYSTEM DROP REPLICA 'r' FROM ZKPATH '{zk_prefix}/clickhouse/{suffix}'",
                    f"connector_{suffix}",
                    no_limit,
                )
                node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=60)

                if zk_name == "default":
                    # The client mutex is free while the attempt is in flight.
                    limit = {"get_zookeeper_lock_acquire_timeout_ms": 5000}
                    assert node.query("SELECT zookeeperSessionUptime()", settings=limit) == "0\n"
                    node.query("SYSTEM RECONNECT ZOOKEEPER", settings=limit)

                # A table creation reaches Keeper through the global context, which the profile's limit does not bound.
                create = executor.submit(
                    run,
                    f"CREATE TABLE rmt_{suffix} (x UInt8) ENGINE = ReplicatedMergeTree('{zk_prefix}/clickhouse/tables/{suffix}', 'r') ORDER BY x",
                    f"create_{suffix}",
                    no_limit,
                )
                wait_for_query(f"create_{suffix}")

                # A query stops waiting for the attempt after its limit ...
                error = run(base_query, f"limited_{suffix}", {**no_limit, "get_zookeeper_lock_acquire_timeout_ms": 200})
                assert error and "TIMEOUT_EXCEEDED" in error and "connection attempt (200 ms)" in error, error

                # ... and when it is cancelled.
                killed = executor.submit(run, base_query, f"killed_{suffix}", no_limit)
                wait_for_query(f"killed_{suffix}")
                node.query(f"KILL QUERY WHERE query_id = 'killed_{suffix}' ASYNC")
                error = killed.result(timeout=60)
                assert error and "QUERY_WAS_CANCELLED" in error, error
                error = run(base_query, f"timed_out_{suffix}", {**no_limit, "max_execution_time": 1})
                assert error and "TIMEOUT_EXCEEDED" in error and "connection attempt" not in error, error

                # Queries with no limit wait for the attempt. It started before them, so they make one more attempt
                # together and get its error.
                waiters = [executor.submit(run, base_query, f"waiter_{i}_{suffix}", no_limit) for i in range(3)]
                for i in range(3):
                    wait_for_query(f"waiter_{i}_{suffix}")
                assert not connector.done() and not create.done() and not any(w.done() for w in waiters)
                node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")
                for f in [connector, create, *waiters]:
                    error = f.result(timeout=120)
                    assert error and "All connection tries failed while connecting to ZooKeeper" in error, error
                # The connector's attempt and the one the waiters shared.
                assert session_expirations() - expirations == 2
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

    # zoo1 rejects connections until it has rejoined the ensemble, so the next test waits for it here.
    node.query_with_retry(base_query, retry_count=60, sleep_time=1)
    node.query(f"DROP TABLE IF EXISTS rmt_{suffix} SYNC")


@pytest.mark.parametrize("zk_name", ["default", "zookeeper2"])
def test_zookeeper_lock_acquire_timeout_success_when_no_contention(started_cluster, zk_name):
    """
    Verify that queries succeed normally when there's no lock contention,
    even with a short timeout configured.
    """
    zk_filter = "" if zk_name == "default" else f" AND zookeeperName = '{zk_name}'"
    result = node.query(
        f"SELECT count() FROM system.zookeeper WHERE path = '/'{zk_filter}",
        settings={"get_zookeeper_lock_acquire_timeout_ms": 100},
    )
    assert int(result.strip()) > 0
