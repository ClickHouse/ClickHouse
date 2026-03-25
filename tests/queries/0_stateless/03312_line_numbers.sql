-- Tags: no-fasttest
-- ^ due to the usage of system logs

SELECT 'This is the first query, and it is located on line 4',
1, -- Just random stuff to ensure proper counting of lines.
2, 3;

SELECT 'This is the second query, and it is located on line 8';

SYSTEM FLUSH LOGS query_log;
SELECT type, script_query_number, script_line_number, query FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() AND event_time >= now() - 600 ORDER BY event_time_microseconds, type;
