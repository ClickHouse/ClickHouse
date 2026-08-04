-- Tags: no-fasttest
-- ^ due to the usage of system logs

SELECT 'This is the first query, and it is located on line 4',
1, -- Just random stuff to ensure proper counting of lines.
2, 3;

SELECT 'This is the second query, and it is located on line 8';

SYSTEM FLUSH LOGS query_log, text_log;
SELECT type, script_query_number, script_line_number, query FROM system.query_log WHERE current_database = currentDatabase() AND event_date >= yesterday() ORDER BY event_time_microseconds, type;

SELECT 'Ok' FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%(query 1, line 4)%' AND message LIKE '%This is the first query%' LIMIT 1;
SELECT 'Ok' FROM system.text_log WHERE event_date >= yesterday() AND message LIKE '%(query 2, line 8)%' AND message LIKE '%This is the second query%' LIMIT 1;
