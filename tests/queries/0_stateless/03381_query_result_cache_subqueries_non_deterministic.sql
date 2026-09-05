
SET query_cache_tag = '03381_query_result_cache_subqueries_non_deterministic';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_non_deterministic';

-- Throws because of non-deterministic function in main query
SELECT now(), avg FROM (SELECT avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- Throws because of non-deterministic function in sub-query
SELECT * FROM (SELECT now(), avg(number) as avg FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

-- Should be 0 records in system.query_cache
SELECT count(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_non_deterministic') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_non_deterministic';
