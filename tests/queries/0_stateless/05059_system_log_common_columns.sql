SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT DISTINCT clickhouse_version = version(), system_processor = (SELECT value FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') FROM system.query_log WHERE current_database = currentDatabase();
