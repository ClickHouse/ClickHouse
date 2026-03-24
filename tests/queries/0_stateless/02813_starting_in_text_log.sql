SYSTEM FLUSH LOGS text_log;
SET max_rows_to_read = 0; -- system.text_log can be really big
SET max_execution_time = 0; -- text_log can be large, especially under sanitizers
SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%Starting ClickHouse%';
