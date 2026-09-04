
SET query_cache_tag = '03381_query_result_cache_cte_subqueries';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_cte_subqueries';

-- Test 1: CTE with query_cache_for_subqueries creates cache entry
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_cte_subqueries') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_cte_subqueries';

-- Test 2: CTE cache hit on second run
WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

WITH sq AS (SELECT sum(number) AS s FROM numbers(100))
SELECT s FROM sq
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_cte_subqueries') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true, cache entry exists)

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_cte_subqueries';

-- Test 3: Explicit opt-in on CTE subquery without query_cache_for_subqueries
SELECT s FROM (SELECT sum(number) AS s FROM numbers(100) SETTINGS use_query_cache = true);

SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_cte_subqueries') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_cte_subqueries';
