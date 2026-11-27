SELECT 1 FORMAT Null;
SYSTEM FLUSH LOGS query_log;

-- system tables must have no auto-added statistics (auto_statistics_types is disabled for system tables)
SELECT database, table, name, statistics FROM system.columns WHERE database = 'system' AND statistics != '';
