import time

import pytest
import redis

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

REDIS_CONFIG = "configs/query_result_cache_redis.xml"

node1 = cluster.add_instance(
    "node1",
    main_configs=[REDIS_CONFIG],
    with_redis=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[REDIS_CONFIG],
    with_redis=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def get_redis_client():
    return redis.Redis(
        host="localhost",
        port=cluster.redis_port,
        password="clickhouse",
        db=0,
    )


def flush_redis(r):
    r.flushdb()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def cache_hit(node, query):
    """Return True if the second execution of *query* is a cache hit."""
    node.query(f"{query} SETTINGS use_query_cache = true")
    result = node.query(
        f"""
        SYSTEM FLUSH LOGS query_log;
        SELECT ProfileEvents['QueryCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{query} SETTINGS use_query_cache = true;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    return result.strip() == "1"


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


def test_single_node_hit_miss(started_cluster):
    """First execution is a miss, second is a hit on the same node."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 42"
    # miss
    node1.query(f"{q} SETTINGS use_query_cache = true")
    node1.query("SYSTEM FLUSH LOGS query_log")
    hits_misses = node1.query(
        f"""
        SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true;'
        ORDER BY event_time_microseconds
        LIMIT 1
        """
    )
    assert hits_misses.strip() == "0\t1", f"Expected miss, got: {hits_misses}"

    # hit
    node1.query(f"{q} SETTINGS use_query_cache = true")
    node1.query("SYSTEM FLUSH LOGS query_log")
    hits_misses2 = node1.query(
        f"""
        SELECT ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert hits_misses2.strip() == "1\t0", f"Expected hit, got: {hits_misses2}"


def test_cross_node_sharing(started_cluster):
    """node1 writes a shared entry; node2 should hit it."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 1234"
    # node1 writes
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    # node2 reads
    node2.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    node2.query("SYSTEM FLUSH LOGS query_log")
    result = node2.query(
        f"""
        SELECT ProfileEvents['QueryCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert result.strip() == "1", f"Expected cross-node hit, got: {result}"


def test_ttl_expiry(started_cluster):
    """After TTL expires the entry is gone; a subsequent read is a miss."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 9999"
    # write with 1-second TTL
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1, query_cache_ttl = 1"
    )

    # wait for expiry
    time.sleep(2)

    # must be a miss
    node2.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    node2.query("SYSTEM FLUSH LOGS query_log")
    result = node2.query(
        f"""
        SELECT ProfileEvents['QueryCacheMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert result.strip() == "1", f"Expected miss after TTL, got: {result}"


def test_clear_all(started_cluster):
    """SYSTEM DROP QUERY CACHE clears all entries; both nodes see a miss."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 7777"
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    # clear from node1
    node1.query("SYSTEM CLEAR QUERY CACHE")

    # node2 should miss
    node2.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    node2.query("SYSTEM FLUSH LOGS query_log")
    result = node2.query(
        f"""
        SELECT ProfileEvents['QueryCacheMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert result.strip() == "1", f"Expected miss after clear, got: {result}"


def test_clear_by_tag(started_cluster):
    """SYSTEM DROP QUERY CACHE TAG 'x' removes only entries with that tag."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'keep', query_cache_share_between_users = 1"
    )
    node1.query(
        "SELECT 2 SETTINGS use_query_cache = true, query_cache_tag = 'drop', query_cache_share_between_users = 1"
    )

    # drop only 'drop' tag
    node1.query("SYSTEM CLEAR QUERY CACHE TAG 'drop'")

    # 'keep' tag entry still exists on node2
    node2.query(
        "SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'keep', query_cache_share_between_users = 1"
    )
    node2.query("SYSTEM FLUSH LOGS query_log")
    keep_hit = node2.query(
        """
        SELECT ProfileEvents['QueryCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = 'SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = \\'keep\\', query_cache_share_between_users = 1;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert keep_hit.strip() == "1", f"Expected 'keep' tag entry to survive, got: {keep_hit}"

    # 'drop' tag entry was deleted
    node2.query(
        "SELECT 2 SETTINGS use_query_cache = true, query_cache_tag = 'drop', query_cache_share_between_users = 1"
    )
    node2.query("SYSTEM FLUSH LOGS query_log")
    drop_miss = node2.query(
        """
        SELECT ProfileEvents['QueryCacheMisses']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = 'SELECT 2 SETTINGS use_query_cache = true, query_cache_tag = \\'drop\\', query_cache_share_between_users = 1;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert drop_miss.strip() == "1", f"Expected 'drop' tag entry to be gone, got: {drop_miss}"


def test_user_isolation(started_cluster):
    """Non-shared entries from one user must not be read by another user."""
    r = get_redis_client()
    flush_redis(r)

    # user1 writes a non-shared entry
    node1.query(
        "SELECT 555 SETTINGS use_query_cache = true",
        user="default",
    )

    # user2 (different uid) must see a miss — create a second user first
    node1.query(
        "CREATE USER IF NOT EXISTS cache_test_user2 IDENTIFIED WITH no_password"
    )
    node1.query(
        "GRANT SELECT ON system.* TO cache_test_user2"
    )
    try:
        node1.query(
            "SELECT 555 SETTINGS use_query_cache = true",
            user="cache_test_user2",
        )
        node1.query("SYSTEM FLUSH LOGS query_log")
        result = node1.query(
            """
            SELECT ProfileEvents['QueryCacheMisses']
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND user = 'cache_test_user2'
              AND query = 'SELECT 555 SETTINGS use_query_cache = true;'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
            """
        )
        assert result.strip() == "1", f"Expected user isolation miss, got: {result}"
    finally:
        node1.query("DROP USER IF EXISTS cache_test_user2")


def test_shared_entry(started_cluster):
    """query_cache_share_between_users=1 allows cross-user cache hits."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "CREATE USER IF NOT EXISTS cache_share_user IDENTIFIED WITH no_password"
    )
    node1.query("GRANT SELECT ON system.* TO cache_share_user")
    try:
        # default user writes shared
        node1.query(
            "SELECT 888 SETTINGS use_query_cache = true, query_cache_share_between_users = 1",
            user="default",
        )

        # cache_share_user reads
        node1.query(
            "SELECT 888 SETTINGS use_query_cache = true, query_cache_share_between_users = 1",
            user="cache_share_user",
        )
        node1.query("SYSTEM FLUSH LOGS query_log")
        result = node1.query(
            """
            SELECT ProfileEvents['QueryCacheHits']
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND user = 'cache_share_user'
              AND query = 'SELECT 888 SETTINGS use_query_cache = true, query_cache_share_between_users = 1;'
            ORDER BY event_time_microseconds DESC
            LIMIT 1
            """
        )
        assert result.strip() == "1", f"Expected shared hit, got: {result}"
    finally:
        node1.query("DROP USER IF EXISTS cache_share_user")


def test_redis_failure_graceful(started_cluster):
    """When Redis is stopped, queries execute normally (degraded to cache miss)."""
    # Restart cluster to reset Redis state; we pause/resume instead of
    # stopping the whole cluster to keep the test fast.
    # Use node1 to execute a query while Redis is paused.
    cluster.pause_container("redis1")
    try:
        # Must not raise
        result = node1.query(
            "SELECT 111 SETTINGS use_query_cache = true",
            timeout=10,
        )
        assert result.strip() == "111", f"Query should still return results: {result}"
    finally:
        cluster.unpause_container("redis1")


def test_max_entry_size(started_cluster):
    """Results exceeding max_entry_size_in_bytes are not written to the cache."""
    r = get_redis_client()
    flush_redis(r)

    # Generate a result that is roughly 11 MiB (> 10 MiB limit in config).
    # repeat('x', 1024*1024) repeated 11 times → 11 MiB
    node1.query(
        "SELECT repeat('x', 1048576) FROM numbers(11)"
        " SETTINGS use_query_cache = true, max_block_size = 100",
    )
    # The entry should NOT be in the cache: Redis key count stays at 0.
    key_count = r.dbsize()
    assert key_count == 0, f"Expected no cache entry due to size limit, got {key_count} keys"


def test_min_query_runs(started_cluster):
    """With query_cache_min_query_runs=2, only the 2nd run writes to cache."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 321"
    # 1st run — must not write (min_query_runs=2)
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    assert r.dbsize() == 0, "Entry must not be written after first run"

    # 2nd run — writes
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    assert r.dbsize() == 1, "Entry must be written after second run"

    # 3rd run — cache hit
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    node1.query("SYSTEM FLUSH LOGS query_log")
    result = node1.query(
        f"""
        SELECT ProfileEvents['QueryCacheHits']
        FROM system.query_log
        WHERE type = 'QueryFinish'
          AND query = '{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2;'
        ORDER BY event_time_microseconds DESC
        LIMIT 1
        """
    )
    assert result.strip() == "1", f"Expected cache hit on 3rd run, got: {result}"


def test_lock_key_cleaned_up_after_write(started_cluster):
    """After a successful write the `:lock` key must not remain in Redis."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT 4242 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    # All keys containing ":lock" should be gone.
    lock_keys = [k for k in r.keys("*") if b":lock" in k]
    assert lock_keys == [], f"Stale lock keys found after write: {lock_keys}"


def test_lock_ttl_expiry_allows_progress(started_cluster):
    """A stale `:lock` key (TTL expired) must not block subsequent queries."""
    r = get_redis_client()
    flush_redis(r)

    # Manually plant a lock key with a very short TTL (500 ms).
    # We use an arbitrary key that won't match any real query, so ClickHouse
    # will never find a result and will degrade to executing the query itself.
    r.set("stale_lock_test:lock", "IN_PROGRESS", px=500)

    # Wait for the TTL to expire.
    time.sleep(1)

    # The lock should be gone now — query must proceed normally without hanging.
    result = node2.query(
        "SELECT 1111 SETTINGS use_query_cache = true",
        timeout=10,
    )
    assert result.strip() == "1111", f"Query should return normally: {result}"


def test_no_stampede_concurrent(started_cluster):
    """Two concurrent nodes must produce only one cache entry (no stampede)."""
    import threading

    r = get_redis_client()
    flush_redis(r)

    # Use a sleep() inside the query to widen the race window so node2 starts
    # while node1 is still computing and holds the lock.
    query = "SELECT sleep(0.5), 9876 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"

    results = {}

    def run(node, name):
        results[name] = node.query(query, timeout=30)

    t1 = threading.Thread(target=run, args=(node1, "node1"))
    t2 = threading.Thread(target=run, args=(node2, "node2"))

    t1.start()
    # Give node1 a 50 ms head start so it acquires the lock first.
    time.sleep(0.05)
    t2.start()

    t1.join()
    t2.join()

    # Both threads must have returned a result.
    assert "node1" in results and "node2" in results, "One of the threads did not finish"

    # Exactly one cache entry should exist (the other node hit the cache or waited).
    # Allow up to 2 keys in case the lock key lingers briefly, but at most 1 data key.
    all_keys = r.keys("*")
    data_keys = [k for k in all_keys if b":lock" not in k]
    assert len(data_keys) == 1, (
        f"Expected exactly 1 data cache entry, found {len(data_keys)}: {data_keys}"
    )
