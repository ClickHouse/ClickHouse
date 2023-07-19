SYSTEM FLUSH LOGS;
SELECT count() > 0 FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%Application: Starting ClickHouse%';
