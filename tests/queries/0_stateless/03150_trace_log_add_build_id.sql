SELECT sleep(1);

SYSTEM FLUSH LOGS;

SELECT COUNT(*) > 1 FROM system.trace_log WHERE build_id IS NOT NULL;

