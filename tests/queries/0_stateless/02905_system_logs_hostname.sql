SELECT 'test hostname in system log tables';

set log_query_threads=1;
set log_queries_min_type='QUERY_FINISH';
set log_queries=1;
select '02095_system_logs_hostname' from system.one format Null;
set log_queries=0;
set log_query_threads=0;

system flush logs;

select hostname
from system.query_log
where
    query like 'select \'02095_system_logs_hostname%'
    and current_database = currentDatabase()
    and event_date >= yesterday() LIMIT 1 FORMAT Null;


select hostName(), hostname
from system.query_thread_log
where
    query like 'select \'02095_system_logs_hostname%'
    and current_database = currentDatabase()
    and event_date >= yesterday() LIMIT 1 FORMAT Null;

