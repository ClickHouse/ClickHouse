set log_queries = 1;

select '01461_query_log_query_start_time_milliseconds_test';
system flush logs;
SELECT If((select count(query_start_time_microseconds)  from system.query_log WHERE query LIKE  '%01461_query_log_query_start_time_milliseconds_test%' AND query NOT LIKE '%system.query_log%') > 0, 'ok', 'fail');
