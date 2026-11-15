SET log_queries = 1;
SELECT 1 LIMIT 0;
SYSTEM FLUSH LOGS query_log;

SELECT * FROM system.query_log
PREWHERE ProfileEvents['Query'] > 0 and current_database = currentDatabase() 

WHERE event_date >= yesterday() AND event_time >= now() - 600
LIMIT 0;
