set log_queries = 1;

select '01473_asynchronous_metric_log_event_start_time_milliseconds_test';
system flush logs;
SELECT If((select count(event_time_microseconds)  from system.asynchronous_metric_log) > 0, 'ok', 'fail'); -- success

select '01473_metric_log_event_start_time_milliseconds_test';
system flush logs;
SELECT If((select count(event_time_microseconds)  from system.metric_log) > 0, 'ok', 'fail'); -- success
