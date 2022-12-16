-- { echoOn }

SYSTEM DROP QUERY RESULT CACHE;

-- the same query with two different partition keys is written twice into the query result cache

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_partition_key = 'key 1';
SELECT COUNT(*) FROM system.queryresult_cache;

SELECT 1 SETTINGS enable_experimental_query_result_cache = true, query_result_cache_partition_key = 'key 2';
SELECT COUNT(*) FROM system.queryresult_cache;

SYSTEM DROP QUERY RESULT CACHE;

-- { echoOff }
