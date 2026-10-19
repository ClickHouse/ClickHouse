
SET query_cache_tag = '03381_query_result_cache_union_subqueries';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_union_subqueries';

-- Test 1: UNION ALL subquery with query_cache_for_subqueries creates cache entries
SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, query_cache_system_table_handling = 'save';

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_union_subqueries') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true)

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_union_subqueries';

-- Test 2: Cache hit on second run
SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, query_cache_system_table_handling = 'save';

SELECT * FROM (SELECT 1 AS x UNION ALL SELECT 2 AS x) ORDER BY x
SETTINGS use_query_cache = true, query_cache_for_subqueries = true, query_cache_system_table_handling = 'save';

SELECT count(*) > 0 FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_union_subqueries') AS test_query_cache WHERE is_subquery = 1;
-- Expected: 1 (true, second run hits cache)

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_union_subqueries';
