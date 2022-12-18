-- Tags: no-parallel

SYSTEM DROP QUERY RESULT CACHE;

-- rand() is non-deterministic, with default settings no entry in the query result cache should be created
SELECT COUNT(rand(1)) SETTINGS enable_experimental_query_result_cache = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT '---';

-- force an entry
SELECT COUNT(RAND(1)) SETTINGS enable_experimental_query_result_cache = true, query_result_cache_store_results_of_queries_with_nondeterministic_functions = true;
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;
