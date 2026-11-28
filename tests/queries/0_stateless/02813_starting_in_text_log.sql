-- Disable force_primary_key_reverse_order: tests system.text_log, output depends on key direction
SET force_primary_key_reverse_order = 0;

SYSTEM FLUSH LOGS text_log;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET max_execution_time = 0; -- text_log can be large, especially under sanitizers
SELECT 1 FROM system.text_log
WHERE event_date >= yesterday()
    AND event_time >= now() - INTERVAL uptime() SECOND - INTERVAL 10 MINUTE
    AND event_time <= now() - INTERVAL uptime() SECOND + INTERVAL 10 MINUTE
    AND logger_name = 'Application'
    AND message LIKE '%Starting ClickHouse%'
LIMIT 1;
