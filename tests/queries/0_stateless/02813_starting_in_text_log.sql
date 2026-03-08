-- Disable force_primary_key_reverse_order: tests system.text_log, output depends on key direction
SET force_primary_key_reverse_order = 0;

SYSTEM FLUSH LOGS text_log;
SET max_rows_to_read = 0; -- system.text_log can be really big
SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%Starting ClickHouse%';
