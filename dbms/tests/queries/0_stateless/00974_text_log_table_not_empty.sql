SELECT 6103;

SYSTEM FLUSH LOGS;

SELECT count() > 0
FROM system.text_log
WHERE position(system.text_log.message, 'SELECT 6103') > 0


