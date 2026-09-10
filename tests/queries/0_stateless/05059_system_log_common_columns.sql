SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT DISTINCT clickhouse_version = version(), system_processor = (SELECT value FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') FROM system.query_log WHERE current_database = currentDatabase();

SELECT table FROM system.columns WHERE database = 'system' AND endsWith(table, '_log') GROUP BY table HAVING countIf(name = 'clickhouse_version') != 1 OR countIf(name = 'system_processor') != 1 ORDER BY table;
