--
-- This is a cleaner approach for writing a test that relies on system.query_log/query_thread_log.
--
-- It uses current database, and since clickhouse-test will generate random for
-- each run you can run the test multiple times without worrying about
-- overlaps.
--
-- There is still event_date/event_time filter for better performance
-- (even though this is not relevant for runs on CI)
--

set log_query_threads=1;
set log_queries_min_type='QUERY_FINISH';
set log_queries=1;
select '01547_query_log_current_database' from system.one format Null;
set log_queries=0;
set log_query_threads=0;

system flush logs query_log, query_thread_log;

select count()
from system.query_log
where
    query like 'select \'01547_query_log_current_database%'
    and current_database = currentDatabase()
    and event_date >= yesterday();

-- at least two threads for processing
-- (but one just waits for another, sigh)
select count() == 2
from system.query_thread_log
where
    query like 'select \'01547\_query\_log\_current\_database%'
    and current_database = currentDatabase()
    and event_date >= yesterday()
