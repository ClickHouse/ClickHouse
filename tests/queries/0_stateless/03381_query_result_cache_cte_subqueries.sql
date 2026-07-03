-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- Test 1: CTE with query_cache_for_subqueries creates cache entry
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

SYSTEM DROP QUERY CACHE;

-- Test 2: CTE cache hit on second run
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1 (true, cache entry exists)

SYSTEM DROP QUERY CACHE;

-- Test 3: Explicit opt-in on CTE subquery without query_cache_for_subqueries
SELECT s FROM (SELECT sum(number) AS s FROM numbers(100) SETTINGS use_query_cache = true);

SELECT count(*) FROM system.query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM DROP QUERY CACHE;
