SYSTEM DROP QUERY CACHE;

-- Cache the query after the 1st query invocation
SELECT 1 SETTINGS use_query_cache = true;

SELECT 'Expect one entry in the query cache', count(query_id) from system.query_cache;

SYSTEM DROP QUERY CACHE;