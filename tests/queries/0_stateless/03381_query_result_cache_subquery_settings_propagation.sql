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

-- Test 3: query_cache_for_subqueries enables propagation (caches both outer and inner nodes)
SELECT number FROM (SELECT number FROM numbers(10))
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 2 (outer node + inner subquery node)

SYSTEM DROP QUERY CACHE;

-- Test 4: Explicit opt-out overrides propagation for that subquery
SELECT number FROM (SELECT number FROM numbers(10) SETTINGS use_query_cache = false)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (outer node cached, inner subquery explicitly opted out)

SYSTEM DROP QUERY CACHE;
