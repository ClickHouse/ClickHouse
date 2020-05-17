SET log_queries = 1;
SELECT 1 LIMIT 0;
SYSTEM FLUSH LOGS;

SELECT arrayJoin AS kv_key
FROM system.query_log
ARRAY JOIN ProfileEvents.Names AS arrayJoin
PREWHERE has(arrayMap(key -> key, ProfileEvents.Names), 'Query')
WHERE arrayJoin = 'Query'
LIMIT 0;
