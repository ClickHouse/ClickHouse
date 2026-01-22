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
    main_configs=["configs/zookeeper.xml", "configs/disable_ddl.xml"],
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


def test_zookeeper_lock_acquire_timeout(started_cluster):
    """
    Test that queries fail with TIMEOUT_EXCEEDED when they can't acquire
    the zookeeper_mutex within the configured timeout.

    Strategy:
    1. Pause ZooKeeper so getZooKeeper() blocks indefinitely on reconnection
    2. Fire one query with long timeout (holds mutex while blocked on ZK)
    3. Fire concurrent queries with short timeout - they should fail fast
    4. Verify the short-timeout queries fail with TIMEOUT_EXCEEDED quickly
    """
    node.query("SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 1")
    with cluster.pause_container("zoo1"):
        # once this query fails, it means the Zookeeper session is expired
        with pytest.raises(QueryRuntimeException) as e:
            node.query("SELECT * FROM system.zookeeper WHERE path = '/' LIMIT 1")
        assert "KEEPER_EXCEPTION" in str(e)

        long_timeout_ms = 30000
        short_timeout_ms = 200
        max_short_query_duration_seconds = 2 # they should fail close to short_timeout_ms, but give some buffer
        num_short_queries = 5

        results = []

        def run_query(query_id, lock_acquire_timeout_ms):
            start = time.time()
            try:
                node.query(
                    f"SELECT '{query_id}', * FROM system.zookeeper WHERE path = '/' LIMIT 1",
                    settings={"get_zookeeper_lock_acquire_timeout_ms": lock_acquire_timeout_ms},
                    timeout=(lock_acquire_timeout_ms / 1000) * 10,
                    query_id=query_id,
                )
                return ("success", time.time() - start, None)
            except Exception as e:
                return ("error", time.time() - start, str(e))

        # Fire queries concurrently:
        # - One with long timeout (will hold the mutex)
        # - Several with short timeout (should fail fast)
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_short_queries + 1) as executor:
            # Start the long-timeout query first
            long_future = executor.submit(run_query, "long", long_timeout_ms)
            
            # Give it a moment to acquire the mutex
            time.sleep(1)
            
            # Now fire short-timeout queries
            short_futures = [executor.submit(run_query, f"short_{i}", short_timeout_ms) for i in range(num_short_queries)]

            for f in concurrent.futures.as_completed(short_futures, timeout=10):
                results.append(f.result())

            long_result = long_future.result()
            assert long_result[0] == "error", f"Expected error, got {long_result[0]}"
            assert "DB::Exception: All connection tries failed while connecting to ZooKeeper." in long_result[2], f"Expected 'all connection tries failed' error, got {long_result[2]}"

        # Analyze results from short-timeout queries
        # Look for our specific mutex acquire timeout error message
        assert len(results) == num_short_queries, f"Expected {num_short_queries} results, got {len(results)}"
        for r in results:
            assert r[0] == "error", f"Expected error, got {r[0]}"
            assert "acquiring ZooKeeper lock" in r[2], f"Expected 'acquiring ZooKeeper lock' error, got {r[2]}"
            assert r[1] < max_short_query_duration_seconds, f"Expected timeout < {max_short_query_duration_seconds}s, got {r[1]}s"


def test_zookeeper_lock_acquire_timeout_success_when_no_contention(started_cluster):
    """
    Verify that queries succeed normally when there's no lock contention,
    even with a short timeout configured.
    """
    result = node.query(
        "SELECT count() FROM system.zookeeper WHERE path = '/'",
        settings={"get_zookeeper_lock_acquire_timeout_ms": 100},
    )
    assert int(result.strip()) >= 0
