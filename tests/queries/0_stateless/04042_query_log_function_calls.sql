-- Tags: no-fasttest, no-parallel

SET log_queries = 1;

SELECT length('hello'), upper('world');

SYSTEM FLUSH LOGS;

SELECT
    function_calls['length'].1 AS length_blocks,
    function_calls['length'].2 AS length_rows,
    function_calls['length'].3 > 0 AS length_has_bytes,
    function_calls['upper'].1 AS upper_blocks,
    function_calls['upper'].2 AS upper_rows,
    function_calls['upper'].3 > 0 AS upper_has_bytes
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE '%length(\_hello\_)%upper(\_world\_)%'
    AND type = 'QueryFinish'
    AND event_date >= yesterday()
ORDER BY event_time_microseconds DESC
LIMIT 1;
