"""
Test for get_zookeeper_lock_acquire_timeout_ms setting.

When ZooKeeper is unavailable and one thread holds the zookeeper_mutex while trying
to reconnect, other threads should fail fast with TIMEOUT_EXCEEDED rather than
blocking indefinitely.
"""

import pytest
import time
import concurrent.futures
from helpers.cluster import ClickHouseCluster, QueryRuntimeException

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


@pytest.mark.parametrize("zk_name", ["default", "zookeeper2"])
def test_zookeeper_lock_acquire_timeout(started_cluster, zk_name):
    """
    Test that queries fail with TIMEOUT_EXCEEDED when they can't acquire
    the zookeeper_mutex (or auxiliary_zookeepers_mutex) within the configured timeout.

    Strategy:
    1. Pause ZooKeeper so getZooKeeper()/getAuxiliaryZooKeeper() blocks on reconnection
    2. Fire one query with long timeout (holds mutex while blocked on ZK)
    3. Fire concurrent queries with short timeout - they should fail fast
    4. Verify the short-timeout queries fail with TIMEOUT_EXCEEDED quickly
    """
    zk_filter = "" if zk_name == "default" else f" AND zookeeperName = '{zk_name}'"
    base_query = f"SELECT * FROM system.zookeeper WHERE path = '/'{zk_filter} LIMIT 1"

    node.query(base_query)
    with cluster.pause_container("zoo1"):
        # once this query fails, it means the Zookeeper session is expired
        with pytest.raises(QueryRuntimeException) as e:
            node.query(base_query)
        assert "KEEPER_EXCEPTION" in str(e.value)

        long_timeout_ms = 30000
        short_timeout_ms = 200
        # The client timeout must outlast the server-side timeout plus process startup and sanitizer scheduling delays.
        client_timeout_seconds = 60
        max_short_query_duration_seconds = 2  # they should fail close to short_timeout_ms, but give some buffer
        # `SYSTEM RECONNECT ZOOKEEPER` can spend additional time in command dispatch on sanitizer builds.
        max_system_reconnect_duration_seconds = 10

        short_queries = {
            f"system_zookeeper_{i}": base_query
            for i in range(3 if zk_name == "default" else 5)
        }
        if zk_name == "default":
            short_queries["session_uptime"] = "SELECT zookeeperSessionUptime()"
            short_queries["system_reconnect"] = "SYSTEM RECONNECT ZOOKEEPER"

        # `SYSTEM RELOAD CONFIG` and `SYSTEM RELOAD ASYNCHRONOUS METRICS` first wait on their own
        # serialization mutexes, so this Keeper-lock test cannot establish the same timeout contract for them.
        results = {}

        def run_query(query_id, query, lock_acquire_timeout_ms):
            start = time.monotonic()
            try:
                node.query(
                    query,
                    settings={"get_zookeeper_lock_acquire_timeout_ms": lock_acquire_timeout_ms},
                    timeout=client_timeout_seconds,
                    query_id=query_id,
                )
                return (query_id, "success", time.monotonic() - start, None)
            except Exception as e:
                return (query_id, "error", time.monotonic() - start, str(e))

        # Fire queries concurrently:
        # - One with long timeout (will hold the mutex)
        # - Several with short timeout (should fail fast)
        failpoint = (
            "context_zookeeper_lock_acquired_pause"
            if zk_name == "default"
            else "context_auxiliary_zookeeper_lock_acquired_pause"
        )
        node.query(f"SYSTEM ENABLE FAILPOINT {failpoint}")
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(short_queries) + 1) as executor:
                # Start the long-timeout query first
                long_future = executor.submit(run_query, "long", base_query, long_timeout_ms)

                try:
                    # Long-timeout query should be running and acquire the mutex
                    node.query(f"SYSTEM WAIT FAILPOINT {failpoint} PAUSE", timeout=10)

                    # Now fire short-timeout queries
                    short_futures = [
                        executor.submit(run_query, query_id, query, short_timeout_ms)
                        for query_id, query in short_queries.items()
                    ]

                    for f in concurrent.futures.as_completed(short_futures, timeout=client_timeout_seconds + 10):
                        query_id, status, duration, error = f.result()
                        results[query_id] = (status, duration, error)
                finally:
                    node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

                _, long_status, _, long_error = long_future.result()
                assert long_status == "error", f"Expected error, got {long_status}"
                assert (
                    "DB::Exception: All connection tries failed while connecting to ZooKeeper."
                    in long_error
                ), f"Expected 'all connection tries failed' error, got {long_error}"
        finally:
            node.query(f"SYSTEM DISABLE FAILPOINT {failpoint}")

        # Analyze results from short-timeout queries
        # Look for our specific mutex acquire timeout error message
        assert len(results) == len(short_queries), (
            f"Expected {len(short_queries)} results, got {len(results)}"
        )

        def assert_lock_timeout(query_id, max_duration_seconds):
            status, duration, error = results[query_id]
            assert status == "error", f"Expected {query_id} to fail, got {status}"
            assert "TIMEOUT_EXCEEDED" in error, (
                f"Expected TIMEOUT_EXCEEDED from {query_id}, got {error}"
            )
            assert "acquiring" in error and "ZooKeeper lock" in error, (
                f"Expected 'acquiring ... ZooKeeper lock' from {query_id}, got {error}"
            )
            assert f"({short_timeout_ms} ms)" in error, (
                f"Expected the {short_timeout_ms} ms Keeper-lock timeout from {query_id}, got {error}"
            )
            assert duration < max_duration_seconds, (
                f"Expected {query_id} timeout < {max_duration_seconds}s, got {duration}s"
            )

        if zk_name == "default":
            assert_lock_timeout("system_reconnect", max_system_reconnect_duration_seconds)

        for query_id in short_queries:
            if query_id != "system_reconnect":
                assert_lock_timeout(query_id, max_short_query_duration_seconds)


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
