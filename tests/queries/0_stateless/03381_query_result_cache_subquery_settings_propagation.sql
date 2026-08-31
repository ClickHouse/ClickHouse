-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Test 1: No propagation by default
-- Outer use_query_cache=1 should NOT cache subqueries
SELECT number FROM (SELECT number FROM numbers(10)) SETTINGS use_query_cache = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 0

SYSTEM DROP QUERY CACHE;

-- Test 2: Explicit subquery opt-in
SELECT number FROM (SELECT number FROM numbers(10) SETTINGS use_query_cache = true);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM DROP QUERY CACHE;

-- Test 3: query_cache_for_subqueries enables Planner-level caching of subqueries.
-- The top-level node is cached separately by `executeQuery` (is_subquery = 0).
SELECT number FROM (SELECT number FROM numbers(10))
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (only the inner subquery node)

SYSTEM DROP QUERY CACHE;

-- Test 4: Explicit per-subquery opt-out overrides propagation for that subquery
SELECT number FROM (SELECT number FROM numbers(10) SETTINGS use_query_cache = false)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 0 (the only subquery node has been opted out; the outer node is is_subquery = 0)

SYSTEM DROP QUERY CACHE;

-- Test 5: Cache hit on second run (verifies read path works)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true);
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (single cache entry, second run is a hit)

SYSTEM DROP QUERY CACHE;

-- Test 6: Subquery with custom TTL
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, query_cache_ttl = 300);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM DROP QUERY CACHE;

-- Test 7: Multiple subqueries with query_cache_for_subqueries
SELECT * FROM (SELECT number FROM numbers(3)) AS a, (SELECT number FROM numbers(3)) AS b
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: >= 1

SYSTEM DROP QUERY CACHE;

-- Test 8: IN subquery with query_cache_for_subqueries (exercises PreparedSets path)
SELECT number FROM numbers(10) WHERE number IN (SELECT number FROM numbers(5))
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, query_cache_system_table_handling = 'save';
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: >= 1

SYSTEM DROP QUERY CACHE;

-- Test 9: Scalar subquery with query_cache_for_subqueries (exercises scalar subquery path)
SELECT (SELECT avg(number) FROM numbers(100)) AS avg_val
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, query_cache_system_table_handling = 'save';
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: >= 1

SYSTEM DROP QUERY CACHE;

-- Test 10: Nested subqueries with mixed explicit settings
SELECT number FROM (
    SELECT number FROM (
        SELECT number FROM numbers(5) SETTINGS use_query_cache = true
    )
);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM DROP QUERY CACHE;

-- Test 11: Verify no caching when use_query_cache is not set at all (baseline)
SELECT number FROM (SELECT number FROM numbers(5));
SELECT count(*) FROM system.query_cache;
-- Expected: 0

SYSTEM DROP QUERY CACHE;

-- Test 12: Subquery with enable_writes_to_query_cache = false (write disabled, read only)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, enable_writes_to_query_cache = false);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 0 (writes disabled)

SYSTEM DROP QUERY CACHE;

-- Test 13: Subquery with enable_reads_from_query_cache = false (write only, no read)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, enable_reads_from_query_cache = false);
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true, enable_reads_from_query_cache = false);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (written but never read from cache)

SYSTEM DROP QUERY CACHE;

-- Test 14: Explicit subquery opt-in does NOT leak to outer query (regression test)
-- When inner subquery has use_query_cache=true but outer does not, only the subquery
-- should be cached, not the outer query (no context mutation leakage)
SELECT number FROM (SELECT number FROM numbers(5) SETTINGS use_query_cache = true);
SELECT count(*) FROM system.query_cache WHERE is_subquery = 0;
-- Expected: 0 (outer query should NOT be cached)
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (only subquery cached)

SYSTEM DROP QUERY CACHE;
