
SET query_cache_tag = '03381_query_result_cache_subqueries_exceptions_handling';

SET enable_analyzer = 1;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_exceptions_handling';

-- If an exception is thrown during query execution, results for sub-queries can (not necessarily) appear in QC
SELECT throwIf(1), * FROM (SELECT avg(number) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Zero records
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_exceptions_handling') AS test_query_cache;

-- If an exception is thrown during query execution, results for sub-queries can (not necessarily) appear in QC
SELECT number, (SELECT avg(number) FROM numbers(1, 100)), throwIf(1) FROM numbers(1, 3)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Zero records (exception prevents caching)
SELECT COUNT(*) FROM (SELECT * FROM system.query_cache WHERE tag = '03381_query_result_cache_subqueries_exceptions_handling') AS test_query_cache;

SYSTEM CLEAR QUERY CACHE TAG '03381_query_result_cache_subqueries_exceptions_handling';
