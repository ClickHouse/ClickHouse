-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- rand() is non-deterministic, the query is rejected by default
SELECT COUNT(rand(1)) SETTINGS use_query_cache = true; -- { serverError CANNOT_USE_QUERY_CACHE_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

-- Force caching using a setting
SELECT COUNT(RAND(1)) SETTINGS use_query_cache = true, query_cache_store_results_of_queries_with_nondeterministic_functions = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
