SELECT 42 SETTINGS log_comment='03277_logging_elapsed_ns';

SYSTEM FLUSH LOGS query_log;

SELECT
    ProfileEvents['LogDebug'] + ProfileEvents['LogTrace'] > 0,
    ProfileEvents['LoggerElapsedNanoseconds'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '03277_logging_elapsed_ns' AND type = 'QueryFinish';
