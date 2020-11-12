set log_queries_min_query_duration_ms=300000;
set log_query_threads=1;
set log_queries=1;

--
-- fast -- no logging
--
select '01546_log_queries_min_query_duration_ms-fast' format Null;
system flush logs;

-- No logging, since the query is fast enough.
select count()
from system.query_log
where
    query like '%01546_log_queries_min_query_duration_ms-fast%'
    and query not like '%system.query_log%'
    and current_database = currentDatabase()
    and event_date = today()
    and event_time >= now() - interval 1 minute;
select count()
from system.query_thread_log
where
    query like '%01546_log_queries_min_query_duration_ms-fast%'
    and query not like '%system.query_thread_log%'
    and current_database = currentDatabase()
    and event_date = today()
    and event_time >= now() - interval 1 minute;

--
-- slow -- query logged
--
set log_queries_min_query_duration_ms=300;
select '01546_log_queries_min_query_duration_ms-slow', sleep(0.4) format Null;
system flush logs;

-- With the limit on minimum execution time, "query start" and "exception before start" events are not logged, only query finish.
select count()
from system.query_log
where
    query like '%01546_log_queries_min_query_duration_ms-slow%'
    and query not like '%system.query_log%'
    and current_database = currentDatabase()
    and event_date = today()
    and event_time >= now() - interval 1 minute;
-- There at least two threads involved in a simple query
-- (one thread just waits another, sigh)
select count() == 2
from system.query_thread_log
where
    query like '%01546_log_queries_min_query_duration_ms-slow%'
    and query not like '%system.query_thread_log%'
    and current_database = currentDatabase()
    and event_date = today()
    and event_time >= now() - interval 1 minute;
