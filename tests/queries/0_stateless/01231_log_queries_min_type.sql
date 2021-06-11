set log_queries=1;

select '01231_log_queries_min_type/QUERY_START';
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/QUERY_START%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute;

set log_queries_min_type='EXCEPTION_BEFORE_START';
select '01231_log_queries_min_type/EXCEPTION_BEFORE_START';
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/EXCEPTION_BEFORE_START%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute;

set max_rows_to_read='100K';
set log_queries_min_type='EXCEPTION_WHILE_PROCESSING';
select '01231_log_queries_min_type/EXCEPTION_WHILE_PROCESSING', max(number) from system.numbers limit 1e6; -- { serverError 158; }
system flush logs;
select count() from system.query_log where query like '%01231_log_queries_min_type/EXCEPTION_WHILE_PROCESSING%' and query not like '%system.query_log%' and event_date = today() and event_time >= now() - interval 1 minute and type = 'ExceptionWhileProcessing';

select '01231_log_queries_min_type w/ Settings/EXCEPTION_WHILE_PROCESSING', max(number) from system.numbers limit 1e6; -- { serverError 158; }
system flush logs;
select count() from system.query_log where
    query like '%01231_log_queries_min_type w/ Settings/EXCEPTION_WHILE_PROCESSING%' and
    query not like '%system.query_log%' and
    event_date = today() and
    event_time >= now() - interval 1 minute and
    type = 'ExceptionWhileProcessing' and
    has(Settings.Names, 'max_rows_to_read');
