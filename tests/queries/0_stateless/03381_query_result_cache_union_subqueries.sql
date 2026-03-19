-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Test 1: UNION ALL subquery with query_cache_for_subqueries creates cache entries
SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

SYSTEM DROP QUERY CACHE;

-- Test 2: Cache hit on second run
SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (true, second run hits cache)

SYSTEM DROP QUERY CACHE;
