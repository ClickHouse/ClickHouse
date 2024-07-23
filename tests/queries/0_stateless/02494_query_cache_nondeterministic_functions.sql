-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SYSTEM DROP QUERY CACHE;

-- rand() is non-deterministic, with default settings no entry in the query cache should be created
SELECT COUNT(rand(1)) SETTINGS use_query_cache = true;
SELECT COUNT(*) FROM system.query_cache;

SELECT '---';

-- But an entry can be forced using a setting
SELECT COUNT(RAND(1)) SETTINGS use_query_cache = true, query_cache_store_results_of_queries_with_nondeterministic_functions = true;
SELECT COUNT(*) FROM system.query_cache;

SYSTEM DROP QUERY CACHE;
