SET log_queries = 1;

SELECT reinterpret(toUInt128(1), 'UUID')
SETTINGS log_comment = '04926_reinterpret_query_log_factory_attribution'
FORMAT Null;

SYSTEM FLUSH LOGS query_log;

SELECT has(used_data_type_families, 'UUID')
FROM system.query_log
WHERE event_date >= yesterday()
  AND event_time >= now() - INTERVAL 10 MINUTE
  AND current_database = currentDatabase()
  AND type = 'QueryFinish'
  AND log_comment = '04926_reinterpret_query_log_factory_attribution'
ORDER BY query_start_time DESC
LIMIT 1;
