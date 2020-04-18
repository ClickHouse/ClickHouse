set log_queries=1;

select '01231_log_queries_min_type/QUERY_START';
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute;

set log_queries_min_type='EXCEPTION_BEFORE_START';
select '01231_log_queries_min_type/EXCEPTION_BEFORE_START';
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute;

set log_queries_min_type='EXCEPTION_WHILE_PROCESSING';
select '01231_log_queries_min_type/', max(number) from system.numbers limit 1e6 settings max_rows_to_read='100K'; -- { serverError 158; }
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute;
