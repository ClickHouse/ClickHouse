-- This file contains tests for the event_time_microseconds field for various tables.
-- Note: Only event_time_microseconds for asynchronous_metric_log table is tested via
-- an integration test as those metrics take 60s by default to be updated.
-- Refer: tests/integration/test_asynchronous_metric_log_table.

SET log_queries = 1;

SELECT '01473_metric_log_table_event_start_time_microseconds_test';
SYSTEM FLUSH LOGS;
-- query assumes that the event_time field is accurate.
WITH (
    (
        SELECT event_time_microseconds
        FROM system.metric_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time_with_microseconds,
    (
        SELECT event_time
        FROM system.metric_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail');

SELECT '01473_trace_log_table_event_start_time_microseconds_test';
SET log_queries = 1;
SET query_profiler_real_time_period_ns = 0;
SET query_profiler_cpu_time_period_ns = 1000000;
-- a long enough query to trigger the query profiler and to record trace log
SELECT sleep(2) FORMAT Null;
SYSTEM FLUSH LOGS;
WITH (
      (
          SELECT event_time_microseconds
          FROM system.trace_log
          ORDER BY event_time DESC
          LIMIT 1
      ) AS time_with_microseconds,
      (
          SELECT event_time
          FROM system.trace_log
          ORDER BY event_time DESC
          LIMIT 1
      ) AS t)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(t)) = 0, 'ok', 'fail'); -- success

SELECT '01473_query_log_table_event_start_time_microseconds_test';
SYSTEM FLUSH LOGS;
WITH (
    (
        SELECT event_time_microseconds
        FROM system.query_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time_with_microseconds,
    (
        SELECT event_time
        FROM system.query_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail'); -- success

SELECT '01473_query_thread_log_table_event_start_time_microseconds_test';
SYSTEM FLUSH LOGS;
WITH (
    (
        SELECT event_time_microseconds
        FROM system.query_thread_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time_with_microseconds,
    (
        SELECT event_time
        FROM system.query_thread_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time)
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail'); -- success
