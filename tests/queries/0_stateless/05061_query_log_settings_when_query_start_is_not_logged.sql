-- The changed settings of a query are built at the site that writes the row, so a query whose
-- QUERY_START row is filtered out must still have them on whatever row does get written.

SET log_queries = 1;
SET log_query_settings = 1;
SET max_block_size = 4711;

-- Only the QueryFinish row is written.
SET log_queries_min_type = 'QUERY_FINISH';
SELECT 1 FORMAT Null SETTINGS log_comment = '05061_finish';

-- Only the exception row is written.
SET log_queries_min_type = 'EXCEPTION_WHILE_PROCESSING';
SELECT throwIf(1) FORMAT Null SETTINGS log_comment = '05061_exception'; -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }

SET log_queries_min_type = 'QUERY_START';
SYSTEM FLUSH LOGS query_log;

-- No QueryStart row for either of them, which is what makes the rows below interesting.
SELECT count()
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase() AND type = 'QueryStart'
  AND log_comment IN ('05061_finish', '05061_exception');

SELECT type, Settings['max_block_size'], Settings['log_comment']
FROM system.query_log
WHERE event_date >= yesterday() AND event_time >= now() - 600
  AND current_database = currentDatabase()
  AND log_comment IN ('05061_finish', '05061_exception')
ORDER BY log_comment;
