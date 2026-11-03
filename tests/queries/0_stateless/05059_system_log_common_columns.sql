SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;
SELECT DISTINCT clickhouse_version = version(), system_processor = (SELECT value FROM system.build_options WHERE name = 'SYSTEM_PROCESSOR') FROM system.query_log WHERE current_database = currentDatabase();

-- `system.coverage_log` is not a system log: `clickhouse-test` creates it in the `system` database to
-- collect per-test coverage, and only in coverage builds.
SELECT table FROM system.columns WHERE database = 'system' AND endsWith(table, '_log') AND table != 'coverage_log' GROUP BY table HAVING countIf(name = 'clickhouse_version') != 1 OR countIf(name = 'system_processor') != 1 ORDER BY table;
