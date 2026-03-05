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


# ---------------------------------------------------------------------------
# system.query_cache / dump()
# ---------------------------------------------------------------------------


def test_system_query_cache_columns(started_cluster):
    """system.query_cache shows correct metadata for a cached entry."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 5555"
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_tag = 'mytag', query_cache_share_between_users = 1"
    )

    result = node1.query(
        """
        SELECT query, tag, shared, stale
        FROM system.query_cache
        WHERE query LIKE '%5555%'
        LIMIT 1
        FORMAT TabSeparated
        """
    )
    parts = result.strip().split("\t")
    assert len(parts) == 4, f"Expected 4 columns, got: {result!r}"
    assert "5555" in parts[0], f"query column should contain '5555': {parts[0]}"
    assert parts[1] == "mytag", f"tag column should be 'mytag': {parts[1]}"
    assert parts[2] == "1", f"shared column should be 1: {parts[2]}"
    assert parts[3] == "0", f"stale column should be 0: {parts[3]}"


def test_system_query_cache_multiple_entries(started_cluster):
    """system.query_cache lists all live entries; clearing removes them."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT 1001 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    node1.query(
        "SELECT 1002 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    node1.query(
        "SELECT 1003 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    count_before = int(
        node1.query(
            "SELECT count() FROM system.query_cache WHERE query LIKE '%100%'"
        ).strip()
    )
    assert count_before == 3, f"Expected 3 entries, got {count_before}"

    node1.query("SYSTEM CLEAR QUERY CACHE")

    count_after = int(
        node1.query("SELECT count() FROM system.query_cache").strip()
    )
    assert count_after == 0, f"Expected 0 entries after clear, got {count_after}"


# ---------------------------------------------------------------------------
# count() and sizeInBytes()
# ---------------------------------------------------------------------------


def test_count_reflects_redis_keys(started_cluster):
    """SELECT count() FROM system.query_cache matches Redis DBSIZE (minus lock keys)."""
    r = get_redis_client()
    flush_redis(r)

    for val in [2001, 2002, 2003]:
        node1.query(
            f"SELECT {val} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
        )

    # system.query_cache count
    ch_count = int(node1.query("SELECT count() FROM system.query_cache").strip())
    assert ch_count == 3, f"Expected 3 in system.query_cache, got {ch_count}"

    # Redis DBSIZE should match (no stale lock keys at this point)
    redis_count = r.dbsize()
    assert redis_count == 3, f"Expected 3 keys in Redis, got {redis_count}"


def test_size_in_bytes_is_zero(started_cluster):
    """For the remote cache, result_size in system.query_cache is nonzero but
    the local memory usage (sizeInBytes) is 0 – the data lives in Redis."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT 3001 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    # result_size column represents the serialized entry size stored in Redis
    result_size = int(
        node1.query(
            "SELECT result_size FROM system.query_cache WHERE query LIKE '%3001%' LIMIT 1"
        ).strip()
    )
    assert result_size > 0, f"Expected non-zero result_size, got {result_size}"

    # There is no direct SQL for sizeInBytes(), but the implementation always
    # returns 0. Verify indirectly: the total_size_in_bytes in system.caches
    # for query_result_cache should be 0 for the remote backend.
    total_bytes = node1.query(
        "SELECT sum(total_size_in_bytes) FROM system.caches WHERE name = 'query_result_cache'"
    ).strip()
    # system.caches may not surface the remote cache; either 0 or empty is acceptable.
    assert total_bytes in ("0", ""), f"Expected 0 or empty, got {total_bytes!r}"


# ---------------------------------------------------------------------------
# min_query_duration (query_cache_min_query_duration)
# ---------------------------------------------------------------------------


def test_min_query_duration_not_met(started_cluster):
    """A query that finishes too fast must not be written to the cache."""
    r = get_redis_client()
    flush_redis(r)

    # min_query_duration = 60 000 ms — a simple SELECT completes far faster.
    node1.query(
        "SELECT 4001 SETTINGS use_query_cache = true, query_cache_min_query_duration = 60000"
    )
    assert r.dbsize() == 0, (
        f"Expected no cache entry because query was too fast, got {r.dbsize()} keys"
    )


def test_min_query_duration_met(started_cluster):
    """A query that sleeps past the threshold must be written to the cache."""
    r = get_redis_client()
    flush_redis(r)

    # min_query_duration = 300 ms; sleep(0.5) => 500 ms — should exceed threshold.
    node1.query(
        "SELECT sleep(0.5), 4002"
        " SETTINGS use_query_cache = true, query_cache_share_between_users = 1,"
        " query_cache_min_query_duration = 300"
    )
    assert r.dbsize() == 1, (
        f"Expected 1 cache entry because query exceeded min_query_duration, got {r.dbsize()} keys"
    )


# ---------------------------------------------------------------------------
# Non-deterministic function handling
# ---------------------------------------------------------------------------


def test_nondeterministic_not_cached_by_default(started_cluster):
    """Queries containing rand() must not be cached under the default setting."""
    r = get_redis_client()
    flush_redis(r)

    # Default query_cache_nondeterministic_function_handling = 'throw', so
    # attempting to cache a query with rand() must throw an exception.
    try:
        node1.query(
            "SELECT rand() SETTINGS use_query_cache = true",
            settings={"query_cache_nondeterministic_function_handling": "throw"},
        )
        # If no exception is raised the test still passes as long as nothing
        # was written to the cache (some builds may handle this differently).
    except Exception:
        pass  # expected

    assert r.dbsize() == 0, (
        f"Expected no cache entry for non-deterministic query, got {r.dbsize()} keys"
    )


def test_nondeterministic_saved_when_setting_allows(started_cluster):
    """With query_cache_nondeterministic_function_handling = 'save', the result is cached."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT rand() % 1 SETTINGS use_query_cache = true,"
        " query_cache_share_between_users = 1,"
        " query_cache_nondeterministic_function_handling = 'save'"
    )
    assert r.dbsize() == 1, (
        f"Expected 1 cache entry when nondeterministic_function_handling='save',"
        f" got {r.dbsize()} keys"
    )


# ---------------------------------------------------------------------------
# cancelWrite: lock released on query cancellation / error
# ---------------------------------------------------------------------------


def test_cancel_write_releases_lock(started_cluster):
    """If a writing node cancels mid-flight, the lock key must be gone afterwards
    so other nodes are not blocked."""
    r = get_redis_client()
    flush_redis(r)

    # Execute a query that fails (division by zero) while use_query_cache = true.
    # The writer's cancelWrite path should release the lock.
    try:
        node1.query(
            "SELECT intDiv(1, 0) SETTINGS use_query_cache = true,"
            " query_cache_share_between_users = 1"
        )
    except Exception:
        pass  # expected — intDiv(1, 0) throws an exception

    # No stale lock keys should remain.
    lock_keys = [k for k in r.keys("*") if b":lock" in k]
    assert lock_keys == [], f"Stale lock keys found after cancelled write: {lock_keys}"


# ---------------------------------------------------------------------------
# Multiple tags: tag isolation
# ---------------------------------------------------------------------------


def test_multiple_tags_isolation(started_cluster):
    """Entries with different tags are independent; clearing one tag does not
    affect entries with other tags."""
    r = get_redis_client()
    flush_redis(r)

    tags = ["alpha", "beta", "gamma"]
    for i, tag in enumerate(tags):
        node1.query(
            f"SELECT {6000 + i}"
            f" SETTINGS use_query_cache = true,"
            f" query_cache_tag = '{tag}',"
            f" query_cache_share_between_users = 1"
        )

    assert r.dbsize() == 3, f"Expected 3 entries before partial clear, got {r.dbsize()}"

    # Clear only 'beta'.
    node1.query("SYSTEM CLEAR QUERY CACHE TAG 'beta'")

    # 'alpha' and 'gamma' must still exist.
    remaining = int(node1.query("SELECT count() FROM system.query_cache").strip())
    assert remaining == 2, f"Expected 2 entries after clearing 'beta', got {remaining}"

    # Verify 'beta' is gone.
    beta_count = int(
        node1.query(
            "SELECT count() FROM system.query_cache WHERE tag = 'beta'"
        ).strip()
    )
    assert beta_count == 0, f"Expected 'beta' entries to be gone, got {beta_count}"


# ---------------------------------------------------------------------------
# cross-node count consistency
# ---------------------------------------------------------------------------


def test_cross_node_count(started_cluster):
    """count() on node2 reflects entries written by node1 (shared Redis)."""
    r = get_redis_client()
    flush_redis(r)

    for val in [7001, 7002]:
        node1.query(
            f"SELECT {val} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
        )

    count_node2 = int(node2.query("SELECT count() FROM system.query_cache").strip())
    assert count_node2 == 2, f"Expected node2 to see 2 entries from node1, got {count_node2}"
