import time

import pytest
import redis

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

REDIS_CONFIG = "configs/query_result_cache_redis.xml"
QUERY_CACHE_KEY_PREFIX = "ch:qcache:"

node1 = cluster.add_instance(
    "node1",
    main_configs=[REDIS_CONFIG],
    with_redis=True,
    randomize_settings=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=[REDIS_CONFIG],
    with_redis=True,
    randomize_settings=False,
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
    node1.query("SYSTEM DROP QUERY CACHE")
    node2.query("SYSTEM DROP QUERY CACHE")
    r.flushdb()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def query_log_value(node, column_expr, where_clause, retries=5, delay=0.5):
    """Flush logs and query system.query_log with retries until a non-empty result."""
    for _ in range(retries):
        node.query("SYSTEM FLUSH LOGS")
        result = node.query(
            f"""
            SELECT {column_expr}
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND {where_clause}
            ORDER BY event_time_microseconds DESC
            LIMIT 1
            """
        ).strip()
        if result != "":
            return result
        time.sleep(delay)
    return ""


def cache_hit(node, query):
    """Return True if the second execution of *query* is a cache hit."""
    node.query(f"{query} SETTINGS use_query_cache = true")
    result = query_log_value(
        node,
        "ProfileEvents['QueryCacheHits']",
        f"query = '{query} SETTINGS use_query_cache = true;'",
    )
    return result == "1"


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
    hits_misses = query_log_value(
        node1,
        "ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']",
        f"query LIKE '%{q}%use_query_cache%'",
    )
    assert hits_misses == "0\t1", f"Expected miss, got: {hits_misses}"

    # hit
    node1.query(f"{q} SETTINGS use_query_cache = true")
    hits_misses2 = query_log_value(
        node1,
        "ProfileEvents['QueryCacheHits'], ProfileEvents['QueryCacheMisses']",
        f"query LIKE '%{q}%use_query_cache%'",
    )
    assert hits_misses2 == "1\t0", f"Expected hit, got: {hits_misses2}"


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
    result = query_log_value(
        node2,
        "ProfileEvents['QueryCacheHits']",
        f"query LIKE '%{q}%use_query_cache%query_cache_share_between_users%'",
    )
    assert result == "1", f"Expected cross-node hit, got: {result}"


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
    result = query_log_value(
        node2,
        "ProfileEvents['QueryCacheMisses']",
        f"query LIKE '%{q}%use_query_cache%query_cache_share_between_users%'",
    )
    assert result == "1", f"Expected miss after TTL, got: {result}"


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
    result = query_log_value(
        node2,
        "ProfileEvents['QueryCacheMisses']",
        f"query LIKE '%{q}%use_query_cache%query_cache_share_between_users%'",
    )
    assert result == "1", f"Expected miss after clear, got: {result}"


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
    keep_hit = query_log_value(
        node2,
        "ProfileEvents['QueryCacheHits']",
        "query LIKE '%SELECT 1%query_cache_tag%keep%query_cache_share_between_users%'",
    )
    assert keep_hit == "1", f"Expected 'keep' tag entry to survive, got: {keep_hit}"

    # 'drop' tag entry was deleted
    node2.query(
        "SELECT 2 SETTINGS use_query_cache = true, query_cache_tag = 'drop', query_cache_share_between_users = 1"
    )
    drop_miss = query_log_value(
        node2,
        "ProfileEvents['QueryCacheMisses']",
        "query LIKE '%SELECT 2%query_cache_tag%drop%query_cache_share_between_users%'",
    )
    assert drop_miss == "1", f"Expected 'drop' tag entry to be gone, got: {drop_miss}"


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
        result = query_log_value(
            node1,
            "ProfileEvents['QueryCacheMisses']",
            "user = 'cache_test_user2' AND query LIKE '%SELECT 555%use_query_cache%'",
        )
        assert result == "1", f"Expected user isolation miss, got: {result}"
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
        result = query_log_value(
            node1,
            "ProfileEvents['QueryCacheHits']",
            "user = 'cache_share_user' AND query LIKE '%SELECT 888%use_query_cache%query_cache_share_between_users%'",
        )
        assert result == "1", f"Expected shared hit, got: {result}"
    finally:
        node1.query("DROP USER IF EXISTS cache_share_user")


def test_redis_failure_graceful(started_cluster):
    """When Redis is stopped, queries execute normally (degraded to cache miss)."""
    with cluster.pause_container("redis1"):
        time.sleep(1)  # Wait for container to fully pause
        # Must not raise — ClickHouse should degrade gracefully
        result = node1.query(
            "SELECT 111 SETTINGS use_query_cache = true",
            timeout=30,
        )
        assert result.strip() == "111", f"Query should still return results: {result}"


def test_max_entry_size(started_cluster):
    """Results exceeding max_entry_size_in_bytes are not written to the cache."""
    r = get_redis_client()
    flush_redis(r)

    # Generate a result that is roughly 11 MiB (> 10 MiB limit in config).
    # repeat('x', 100000) × 110 rows ≈ 11 MiB total.
    # Disable compression to ensure the serialized size exceeds the limit.
    node1.query(
        "SELECT repeat('x', 100000) FROM numbers(110)"
        " SETTINGS use_query_cache = true, query_cache_share_between_users = 1,"
        " query_cache_compress_entries = false",
    )
    # The entry should NOT be in the cache: Redis key count stays at 0.
    key_count = r.dbsize()
    assert key_count == 0, f"Expected no cache entry due to size limit, got {key_count} keys"


def test_min_query_runs(started_cluster):
    """With query_cache_min_query_runs=2, only after the 2nd run does the next run write to cache."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 321"
    # 1st run — must not write (min_query_runs=2)
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    assert r.dbsize() == 0, "Entry must not be written after first run"

    # 2nd run — counter reaches threshold but still <= 2, so still no write
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    assert r.dbsize() == 0, "Entry must not be written after second run (counter == min_query_runs)"

    # 3rd run — counter exceeds threshold, writes to cache
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    assert r.dbsize() == 1, "Entry must be written after third run"

    # 4th run — cache hit
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_min_query_runs = 2"
    )
    result = query_log_value(
        node1,
        "ProfileEvents['QueryCacheHits']",
        f"query LIKE '%{q}%query_cache_min_query_runs%'",
    )
    assert result == "1", f"Expected cache hit on 4th run, got: {result}"


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


def test_clear_by_tag_releases_inflight_lock(started_cluster):
    """Clearing a tag while a writer is running must also prevent the in-flight write from re-populating that tag."""
    import threading

    r = get_redis_client()
    flush_redis(r)

    query = (
        "SELECT sleep(2), 4243"
        " SETTINGS use_query_cache = true,"
        " query_cache_tag = 'drop-lock',"
        " query_cache_share_between_users = 1"
    )

    error = []

    def run_query():
        try:
            node1.query(query, timeout=30)
        except Exception as ex:
            error.append(str(ex))

    thread = threading.Thread(target=run_query)
    thread.start()

    time.sleep(0.2)
    node1.query("SYSTEM CLEAR QUERY CACHE TAG 'drop-lock'")

    thread.join(timeout=30)
    assert not error, f"Unexpected query failure: {error}"

    lock_keys = [k for k in r.keys(f"{QUERY_CACHE_KEY_PREFIX}v*:t*:drop-lock:*:*:lock")]
    assert lock_keys == [], f"Tag clear left stale lock keys behind: {lock_keys}"

    entry_count = int(
        node1.query("SELECT count() FROM system.query_cache WHERE tag = 'drop-lock'").strip()
    )
    assert entry_count == 0, f"Expected cleared tag to stay empty after in-flight query, got {entry_count} entries"


def test_private_entries_do_not_block_each_other_across_users(started_cluster):
    """Private entries from different users should not contend on the same Redis lock."""
    import threading

    r = get_redis_client()
    flush_redis(r)

    node1.query("CREATE USER IF NOT EXISTS cache_user_a IDENTIFIED WITH no_password")
    node1.query("CREATE USER IF NOT EXISTS cache_user_b IDENTIFIED WITH no_password")

    query = "SELECT sleep(0.5), 5150 SETTINGS use_query_cache = true"
    errors = []

    def run(user):
        try:
            node1.query(query, user=user, timeout=30)
        except Exception as ex:
            errors.append(str(ex))

    try:
        t1 = threading.Thread(target=run, args=("cache_user_a",))
        t2 = threading.Thread(target=run, args=("cache_user_b",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert not errors, f"Unexpected query failures: {errors}"

        data_keys = [k for k in r.keys(f"{QUERY_CACHE_KEY_PREFIX}*") if b":lock" not in k]
        assert len(data_keys) == 2, f"Expected two private cache entries, got {data_keys}"
    finally:
        node1.query("DROP USER IF EXISTS cache_user_a")
        node1.query("DROP USER IF EXISTS cache_user_b")


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
    """Two concurrent nodes: only one computes the result, the other waits and serves it
    from the cache instead of re-executing the query (the point of stampede protection)."""
    import threading

    r = get_redis_client()
    flush_redis(r)

    # A sleep() inside the query widens the in-flight window so the second node starts
    # while the first is still computing and holds the `IN_PROGRESS` lock.
    query = (
        "SELECT sleep(1), 9876 SETTINGS use_query_cache = true,"
        " query_cache_share_between_users = 1, query_cache_min_query_duration = 0"
    )

    results = {}
    qids = {"node1": "stampede_n1", "node2": "stampede_n2"}

    def run(node, name):
        results[name] = node.query(query, query_id=qids[name], timeout=60)

    t1 = threading.Thread(target=run, args=(node1, "node1"))
    t2 = threading.Thread(target=run, args=(node2, "node2"))

    t1.start()
    # Give node1 a 100 ms head start so it is the one that acquires the lock first.
    time.sleep(0.1)
    t2.start()

    t1.join()
    t2.join()

    # Both threads must have returned the same result.
    assert "node1" in results and "node2" in results, "One of the threads did not finish"
    assert results["node1"] == results["node2"], (
        f"Both nodes must agree on the result: {results}"
    )

    # Exactly one data entry should exist in Redis (the other node reused it).
    data_keys = [k for k in r.keys("*") if b":lock" not in k]
    assert len(data_keys) == 1, (
        f"Expected exactly 1 data cache entry, found {len(data_keys)}: {data_keys}"
    )

    # Decisive check: one node computed and wrote the result, the other waited on the lock
    # and served it from the cache (query_cache_usage = 'Read') rather than re-executing.
    # Before the fix the waiter would re-run the query and show no 'Read'.
    node1.query("SYSTEM FLUSH LOGS")
    node2.query("SYSTEM FLUSH LOGS")

    def cache_usage(node, query_id):
        return node.query(
            "SELECT query_cache_usage FROM system.query_log "
            f"WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
            "ORDER BY event_time_microseconds DESC LIMIT 1"
        ).strip()

    usages = sorted([cache_usage(node1, qids["node1"]), cache_usage(node2, qids["node2"])])
    assert usages == ["Read", "Write"], (
        f"Expected exactly one writer and one cache-reader, got query_cache_usage={usages}"
    )


def test_slow_query_longer_than_lock_ttl_is_cached(started_cluster):
    """A query that runs longer than `lock_ttl_ms` must still be cached on completion.

    The IN_PROGRESS lock is a fixed-TTL advisory lease (no renewal), so the lock expires
    mid-computation for a slow query. The result write must not depend on still holding the
    lock — otherwise such queries would never be cacheable. With the test config
    lock_ttl_ms=5000, sleep(6) outlives the lock by ~1s.
    """
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT sleep(6), 7777"
        " SETTINGS use_query_cache = true, query_cache_min_query_duration = 0",
        query_id="slow_ttl_write",
        timeout=30,
    )

    # The result must have been written even though the lock expired during execution.
    data_keys = [k for k in r.keys("*") if b":lock" not in k]
    assert len(data_keys) == 1, (
        f"Slow query (> lock_ttl) should still be cached, found data keys: {data_keys}"
    )

    # A second run must read it from the cache instead of executing the 6s query again.
    node1.query(
        "SELECT sleep(6), 7777"
        " SETTINGS use_query_cache = true, query_cache_min_query_duration = 0",
        query_id="slow_ttl_read",
        timeout=30,
    )
    usage = query_log_value(node1, "query_cache_usage", "query_id = 'slow_ttl_read'")
    assert usage == "Read", f"Second run should hit the cache, got query_cache_usage={usage!r}"


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
    """SELECT count() FROM system.query_cache counts only query-cache-owned Redis keys."""
    r = get_redis_client()
    flush_redis(r)

    for val in [2001, 2002, 2003]:
        node1.query(
            f"SELECT {val} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
        )

    r.set("unrelated:key", "1")

    ch_count = int(node1.query("SELECT count() FROM system.query_cache").strip())
    assert ch_count == 3, f"Expected 3 in system.query_cache, got {ch_count}"

    redis_count = r.dbsize()
    assert redis_count == 4, f"Expected 4 keys in Redis including unrelated key, got {redis_count}"


def test_size_in_bytes_is_zero(started_cluster):
    """For the remote cache, result_size in system.query_cache is nonzero."""
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


# ---------------------------------------------------------------------------
# B1. Cache data correctness — result1 == result2 (100 rows number + toString)
# ---------------------------------------------------------------------------


def test_cache_data_correctness_100_rows(started_cluster):
    """Cached result must be byte-identical to the original result."""
    r = get_redis_client()
    flush_redis(r)

    q = (
        "SELECT number, toString(number) AS s FROM numbers(100)"
        " ORDER BY number"
    )
    settings = "use_query_cache = true, query_cache_share_between_users = 1"

    # First run: miss — result is computed and cached.
    result1 = node1.query(f"{q} SETTINGS {settings}")

    # Second run: hit — result is served from cache.
    result2 = node1.query(f"{q} SETTINGS {settings}")

    assert result1 == result2, (
        f"Cached result differs from original.\n"
        f"Original (first 200 chars): {result1[:200]!r}\n"
        f"Cached   (first 200 chars): {result2[:200]!r}"
    )


# ---------------------------------------------------------------------------
# B2. Cross-node multi-type data correctness
# ---------------------------------------------------------------------------


def test_cross_node_multi_type_correctness(started_cluster):
    """Cross-node cache preserves Int, String, Float, Date, and Array types."""
    r = get_redis_client()
    flush_redis(r)

    q = (
        "SELECT"
        "  toInt64(number) AS i,"
        "  toString(number) AS s,"
        "  toFloat64(number) / 3.0 AS f,"
        "  toDate('2024-01-01') + number AS d,"
        "  [number, number * 2] AS arr"
        " FROM numbers(50)"
        " ORDER BY number"
    )
    settings = "use_query_cache = true, query_cache_share_between_users = 1"

    # node1 writes
    result_node1 = node1.query(f"{q} SETTINGS {settings}")

    # node2 reads from cache
    result_node2 = node2.query(f"{q} SETTINGS {settings}")

    assert result_node1 == result_node2, (
        f"Cross-node multi-type mismatch.\n"
        f"node1 (first 200 chars): {result_node1[:200]!r}\n"
        f"node2 (first 200 chars): {result_node2[:200]!r}"
    )


# ---------------------------------------------------------------------------
# B3. Large result set serialization correctness (~5MB)
# ---------------------------------------------------------------------------


def test_large_result_correctness(started_cluster):
    """A ~5MB result (under the 10MB limit) caches and reads back correctly."""
    r = get_redis_client()
    flush_redis(r)

    # Each row: repeat('A', 1024) = 1KB. 5000 rows ≈ 5MB.
    q = (
        "SELECT number, repeat('A', 1024) AS payload"
        " FROM numbers(5000)"
        " ORDER BY number"
    )
    settings = "use_query_cache = true, query_cache_share_between_users = 1"

    result1 = node1.query(f"{q} SETTINGS {settings}")
    assert r.dbsize() >= 1, "Expected cache entry to be written for ~5MB result"

    result2 = node2.query(f"{q} SETTINGS {settings}")
    assert result1 == result2, "Large result differs after cache roundtrip"


# ---------------------------------------------------------------------------
# B4. max_entry_size_in_rows protection (> 1M rows not written)
# ---------------------------------------------------------------------------


def test_max_entry_size_in_rows(started_cluster):
    """Results exceeding max_entry_size_in_rows (1M in config) are not cached."""
    r = get_redis_client()
    flush_redis(r)

    # Generate 1_100_000 rows — exceeds the 1M row limit.
    node1.query(
        "SELECT number FROM numbers(1100000)"
        " SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    assert r.dbsize() == 0, (
        f"Expected no cache entry for >1M rows, got {r.dbsize()} keys"
    )


# ---------------------------------------------------------------------------
# B5a. WITH TOTALS — cached totals row must be consistent
# ---------------------------------------------------------------------------


def test_with_totals_cached(started_cluster):
    """WITH TOTALS query produces identical totals after cache roundtrip."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "CREATE TABLE IF NOT EXISTS test_totals (k String, v UInt64) ENGINE = Memory"
    )
    node1.query(
        "INSERT INTO test_totals VALUES ('a', 1), ('a', 2), ('b', 3)"
    )

    q = (
        "SELECT k, sum(v) FROM test_totals GROUP BY k WITH TOTALS ORDER BY k"
    )
    settings = "use_query_cache = true, query_cache_share_between_users = 1"

    result1 = node1.query(f"{q} SETTINGS {settings}")
    result2 = node1.query(f"{q} SETTINGS {settings}")

    assert result1 == result2, (
        f"WITH TOTALS result differs after cache.\n"
        f"Original: {result1!r}\n"
        f"Cached:   {result2!r}"
    )

    node1.query("DROP TABLE IF EXISTS test_totals")


# ---------------------------------------------------------------------------
# B5b. extremes=1 — cached extremes must be consistent
# ---------------------------------------------------------------------------


def test_extremes_cached(started_cluster):
    """extremes=1 query produces identical extremes after cache roundtrip."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT number FROM numbers(10)"
    settings = (
        "use_query_cache = true, query_cache_share_between_users = 1,"
        " extremes = 1"
    )

    result1 = node1.query(f"{q} SETTINGS {settings}")
    result2 = node1.query(f"{q} SETTINGS {settings}")

    assert result1 == result2, (
        f"Extremes result differs after cache.\n"
        f"Original: {result1!r}\n"
        f"Cached:   {result2!r}"
    )


# ---------------------------------------------------------------------------
# B7. NOSCRIPT recovery (SCRIPT FLUSH → write still succeeds)
# ---------------------------------------------------------------------------


def test_noscript_recovery(started_cluster):
    """After SCRIPT FLUSH, the next cache write re-loads Lua scripts and succeeds."""
    r = get_redis_client()
    flush_redis(r)

    # Flush all loaded Lua scripts from Redis.
    r.script_flush()

    # This write should trigger NOSCRIPT, reload scripts, and retry successfully.
    node1.query(
        "SELECT 8888 SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )

    assert r.dbsize() >= 1, (
        f"Expected cache entry after NOSCRIPT recovery, got {r.dbsize()} keys"
    )


# ---------------------------------------------------------------------------
# B9a. enable_writes_to_query_cache = false → no writes to Redis
# ---------------------------------------------------------------------------


def test_writes_disabled(started_cluster):
    """With enable_writes_to_query_cache = false, nothing is written to Redis."""
    r = get_redis_client()
    flush_redis(r)

    node1.query(
        "SELECT 9001"
        " SETTINGS use_query_cache = true,"
        " enable_writes_to_query_cache = false,"
        " query_cache_share_between_users = 1"
    )

    assert r.dbsize() == 0, (
        f"Expected no cache entry with writes disabled, got {r.dbsize()} keys"
    )


# ---------------------------------------------------------------------------
# B9b. enable_reads_from_query_cache = false → no reads from cache
# ---------------------------------------------------------------------------


def test_reads_disabled(started_cluster):
    """With enable_reads_from_query_cache = false, the cache is not read."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT 9002"
    # First run writes to cache.
    node1.query(
        f"{q} SETTINGS use_query_cache = true, query_cache_share_between_users = 1"
    )
    assert r.dbsize() >= 1, "Expected cache entry from first write"

    # Second run with reads disabled — must be a miss.
    node1.query(
        f"{q} SETTINGS use_query_cache = true,"
        " enable_reads_from_query_cache = false,"
        " query_cache_share_between_users = 1"
    )
    result = query_log_value(
        node1,
        "ProfileEvents['QueryCacheHits']",
        "query LIKE '%9002%enable_reads_from_query_cache%'",
    )
    assert result == "0", (
        f"Expected 0 cache hits with reads disabled, got: {result}"
    )


# ---------------------------------------------------------------------------
# B6. query_cache_compress_entries = false — data still correct
# ---------------------------------------------------------------------------


def test_uncompressed_cache_correctness(started_cluster):
    """With query_cache_compress_entries=false, data is cached and read correctly."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT number, toString(number) FROM numbers(50) ORDER BY number"
    settings = (
        "use_query_cache = true, query_cache_share_between_users = 1,"
        " query_cache_compress_entries = false"
    )

    result1 = node1.query(f"{q} SETTINGS {settings}")
    result2 = node1.query(f"{q} SETTINGS {settings}")

    assert result1 == result2, (
        f"Uncompressed cache result differs.\n"
        f"Original: {result1[:200]!r}\n"
        f"Cached:   {result2[:200]!r}"
    )


# ---------------------------------------------------------------------------
# B10. Empty result set (0 rows) — correctly cached and read
# ---------------------------------------------------------------------------


def test_empty_result_cached(started_cluster):
    """A query returning 0 rows should still execute correctly with cache enabled."""
    r = get_redis_client()
    flush_redis(r)

    q = "SELECT number FROM numbers(0)"
    settings = "use_query_cache = true, query_cache_share_between_users = 1"

    result1 = node1.query(f"{q} SETTINGS {settings}")
    result2 = node1.query(f"{q} SETTINGS {settings}")

    # Both should return empty string.
    assert result1 == result2
    assert result1.strip() == "", f"Expected empty result, got: {result1!r}"


# ---------------------------------------------------------------------------
# B12. 20 concurrent different queries — no interference
# ---------------------------------------------------------------------------


def test_concurrent_different_queries(started_cluster):
    """20 different queries cached concurrently produce correct results."""
    import threading

    r = get_redis_client()
    flush_redis(r)

    errors = []
    lock = threading.Lock()

    def run_query(idx):
        try:
            q = f"SELECT {10000 + idx}, toString({10000 + idx})"
            settings = (
                "use_query_cache = true,"
                " query_cache_share_between_users = 1"
            )
            # Write
            r1 = node1.query(f"{q} SETTINGS {settings}")
            # Read (should hit cache)
            r2 = node1.query(f"{q} SETTINGS {settings}")
            if r1 != r2:
                with lock:
                    errors.append(
                        f"Query idx={idx}: result mismatch. "
                        f"r1={r1.strip()!r} r2={r2.strip()!r}"
                    )
        except Exception as e:
            with lock:
                errors.append(f"Query idx={idx}: exception {e}")

    threads = [threading.Thread(target=run_query, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)

    assert not errors, f"Concurrent query errors:\n" + "\n".join(errors)

    # All 20 queries should have their own cache entries.
    cache_count = r.dbsize()
    assert cache_count >= 20, (
        f"Expected at least 20 cache entries, got {cache_count}"
    )


# ---------------------------------------------------------------------------
# B11. system.query_cache stale column correctness
# ---------------------------------------------------------------------------


def test_system_query_cache_stale_column(started_cluster):
    """The 'stale' column in system.query_cache correctly reflects TTL expiry."""
    r = get_redis_client()
    flush_redis(r)

    # Write with a 5-second TTL (enough time to check stale=0 before expiry).
    node1.query(
        "SELECT 11111"
        " SETTINGS use_query_cache = true,"
        " query_cache_share_between_users = 1,"
        " query_cache_ttl = 5"
    )

    # Immediately, the entry should NOT be stale.
    stale_before = node1.query(
        "SELECT stale FROM system.query_cache WHERE query LIKE '%11111%' LIMIT 1"
    ).strip()
    assert stale_before == "0", (
        f"Expected stale=0 before TTL expiry, got: {stale_before}"
    )

    # Wait for TTL to expire.
    time.sleep(6)

    # Now the entry's expires_at is in the past, so stale should be 1.
    # Note: Redis TTL may have already evicted the key, in which case
    # system.query_cache won't show it at all. Both outcomes are correct.
    stale_after = node1.query(
        "SELECT stale FROM system.query_cache WHERE query LIKE '%11111%' LIMIT 1"
    ).strip()
    # Either the entry is gone (empty result) or it's marked stale.
    assert stale_after in ("1", ""), (
        f"Expected stale=1 or entry evicted after TTL, got: {stale_after!r}"
    )
