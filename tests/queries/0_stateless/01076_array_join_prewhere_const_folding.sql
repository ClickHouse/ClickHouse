SET log_queries = 1;
SELECT 1 LIMIT 0;
SYSTEM FLUSH LOGS query_log;

SELECT * FROM system.query_log
PREWHERE ProfileEvents['Query'] > 0 and current_database = currentDatabase() 

LIMIT 0;
