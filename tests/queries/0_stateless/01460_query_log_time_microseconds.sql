set log_queries = 1;
select count() > 0 from system.query_log;

system flush logs;
describe system.query_log;
SELECT If((select count(query_start_time_microseconds)  from system.query_log WHERE lower(query) LIKE  '%describe system.query_log%') > 0, 'ok', 'fail');
