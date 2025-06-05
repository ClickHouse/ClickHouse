-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- If an exception is thrown during query execution, results for sub-queries can (not necessarily) appear in QC
SELECT throwIf(1), * FROM (SELECT avg(number) FROM numbers(1, 100)) SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- Zero records
SELECT COUNT(*) FROM system.query_cache;

-- If an exception is thrown during query execution, results for sub-queries can (not necessarily) appear in QC
SELECT number, (SELECT avg(number) FROM numbers(1, 100)), throwIf(1) FROM numbers(1, 3)
SETTINGS use_query_cache = true, query_cache_for_subqueries = true; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

-- One record for scalar sub-query
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
