-- This file contains tests for the event_time_microseconds field for various tables.
-- Note: Only event_time_microseconds for asynchronous_metric_log table is tested via
-- an integration test as those metrics take 60s by default to be updated.
-- Refer: tests/integration/test_asynchronous_metric_log_table.

set log_queries = 1;

select '01473_metric_log_table_event_start_time_microseconds_test';
system flush logs;
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
SELECT if(dateDiff('second', toDateTime(time_with_microseconds), toDateTime(time)) = 0, 'ok', 'fail')
