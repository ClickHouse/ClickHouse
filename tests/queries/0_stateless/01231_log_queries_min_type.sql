set log_queries=1;

select '01231_log_queries_min_type/QUERY_START';
system flush logs;
select count() from system.query_log where current_database = currentDatabase()
    and query like 'select \'01231_log_queries_min_type/QUERY_START%'
    and event_date >= yesterday();

set log_queries_min_type='EXCEPTION_BEFORE_START';
select '01231_log_queries_min_type/EXCEPTION_BEFORE_START';
system flush logs;
select count() from system.query_log where current_database = currentDatabase()
    and query like 'select \'01231_log_queries_min_type/EXCEPTION_BEFORE_START%'
    and event_date >= yesterday();

set max_rows_to_read='100K';
set log_queries_min_type='EXCEPTION_WHILE_PROCESSING';
select '01231_log_queries_min_type/EXCEPTION_WHILE_PROCESSING', max(number) from system.numbers limit 1e6; -- { serverError TOO_MANY_ROWS }
set max_rows_to_read=0;
system flush logs;
select count() from system.query_log where current_database = currentDatabase()
    and query like 'select \'01231_log_queries_min_type/EXCEPTION_WHILE_PROCESSING%'
    and event_date >= yesterday() and type = 'ExceptionWhileProcessing';

set max_rows_to_read='100K';
select '01231_log_queries_min_type w/ Settings/EXCEPTION_WHILE_PROCESSING', max(number) from system.numbers limit 1e6; -- { serverError TOO_MANY_ROWS }
system flush logs;
set max_rows_to_read=0;
select count() from system.query_log where
    current_database = currentDatabase() and
    query like 'select \'01231_log_queries_min_type w/ Settings/EXCEPTION_WHILE_PROCESSING%' and
    query not like '%system.query_log%' and
    event_date >= yesterday() and
    type = 'ExceptionWhileProcessing' and
    Settings['max_rows_to_read'] != '';
