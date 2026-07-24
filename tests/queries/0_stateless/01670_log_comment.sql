SET log_comment = 'log_comment test', log_queries = 1;
SELECT 1;
SYSTEM FLUSH LOGS query_log;
SELECT type, query FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'log_comment test' AND query LIKE 'SELECT 1%' AND event_date >= yesterday() AND type = 1 ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT type, query FROM system.query_log WHERE current_database = currentDatabase() AND log_comment = 'log_comment test' AND query LIKE 'SELECT 1%' AND event_date >= yesterday() AND type = 2 ORDER BY event_time_microseconds DESC LIMIT 1;
