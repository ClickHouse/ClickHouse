set log_query_threads=1;
set log_queries_min_type='QUERY_FINISH';
set log_queries=1;
select '01548_query_log_query_execution_ms', sleep(0.4) format Null;
set log_queries=0;
set log_query_threads=0;

system flush logs;

select count()
from system.query_log
where
    query like '%01548_query_log_query_execution_ms%'
    and current_database = currentDatabase()
    and query_duration_ms between 100 and 800
    and event_date = today()
    and event_time >= now() - interval 1 minute;

-- at least two threads for processing
-- (but one just waits for another, sigh)
select count() == 2
from system.query_thread_log
where
    query like '%01548_query_log_query_execution_ms%'
    and current_database = currentDatabase()
    and query_duration_ms between 100 and 800
    and event_date = today()
    and event_time >= now() - interval 1 minute;
