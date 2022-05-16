-- Tags: no-tsan, no-asan, no-ubsan, no-msan, no-debug, no-cpu-aarch64

-- This file contains tests for the event_time_microseconds field for various tables.
-- Note: Only event_time_microseconds for asynchronous_metric_log table is tested via
-- an integration test as those metrics take 60s by default to be updated.
-- Refer: tests/integration/test_asynchronous_metric_log_table.

SET log_queries = 1;
SET log_query_threads = 1;
SET query_profiler_real_time_period_ns = 100000000;
-- a long enough query to trigger the query profiler and to record trace log
SELECT sleep(2) FORMAT Null;
SET query_profiler_real_time_period_ns = 1000000000;
SYSTEM FLUSH LOGS;

SELECT '01473_metric_log_table_event_start_time_microseconds_test';
-- query assumes that the event_time field is accurate.
WITH (
        SELECT event_time_microseconds, event_time
        FROM system.metric_log
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.1), toDateTime(time.2)) = 0, 'ok', toString(time));

SELECT '01473_trace_log_table_event_start_time_microseconds_test';
WITH (
          SELECT event_time_microseconds, event_time
          FROM system.trace_log
          ORDER BY event_time DESC
          LIMIT 1
      ) AS time
SELECT if(dateDiff('second', toDateTime(time.1), toDateTime(time.2)) = 0, 'ok', toString(time));

SELECT '01473_query_log_table_event_start_time_microseconds_test';
WITH (
        SELECT event_time_microseconds, event_time
        FROM system.query_log
        WHERE current_database = currentDatabase()
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.1), toDateTime(time.2)) = 0, 'ok', toString(time));

SELECT '01473_query_thread_log_table_event_start_time_microseconds_test';
WITH (
        SELECT event_time_microseconds, event_time
        FROM system.query_thread_log
        WHERE current_database = currentDatabase()
        ORDER BY event_time DESC
        LIMIT 1
    ) AS time
SELECT if(dateDiff('second', toDateTime(time.1), toDateTime(time.2)) = 0, 'ok', toString(time));

SELECT '01473_text_log_table_event_start_time_microseconds_test';
WITH (
          SELECT event_time_microseconds, event_time
          FROM system.query_thread_log
          WHERE current_database = currentDatabase()
          ORDER BY event_time DESC
          LIMIT 1
      ) AS time
SELECT if(dateDiff('second', toDateTime(time.1), toDateTime(time.2)) = 0, 'ok', toString(time));
