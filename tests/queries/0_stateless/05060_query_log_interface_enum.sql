SET log_query_threads = 1;
SELECT 1 SETTINGS log_comment = 'query_log_interface_enum';
SYSTEM FLUSH LOGS query_log, query_thread_log;

-- `user_query_log` copies every `query_log` type, so it follows this change without being edited.
SELECT table, name, type
FROM system.columns
WHERE database = 'system' AND table IN ('query_log', 'query_thread_log', 'processes', 'user_query_log')
  AND name IN ('interface', 'http_method')
ORDER BY table, name;

-- The name and the old numeric spelling select the same row.
SELECT interface = 'TCP', interface = 1, toUInt8(interface) = 1
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = 'query_log_interface_enum'
  AND type = 'QueryFinish'
LIMIT 1;
