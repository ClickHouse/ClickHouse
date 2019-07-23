SELECT 1;

SYSTEM FLUSH LOGS;

SELECT count() > 0
FROM system.text_log;
