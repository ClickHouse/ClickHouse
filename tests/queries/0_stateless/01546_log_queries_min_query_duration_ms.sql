set log_queries_min_query_duration_ms=300000;
set log_query_threads=1;
set log_queries=1;

--
-- fast -- no logging
--
select '01546_log_queries_min_query_duration_ms-fast' format Null;
system flush logs query_log, query_thread_log;

-- No logging, since the query is fast enough.
select count()
from system.query_log
where
    query like 'select \'01546_log_queries_min_query_duration_ms-fast%'
    and current_database = currentDatabase()
    and event_date >= yesterday();
select count()
from system.query_thread_log
where
    query like 'select \'01546_log_queries_min_query_duration_ms-fast%'
    and current_database = currentDatabase()
    and event_date >= yesterday();

--
-- slow -- query logged
--
set log_queries_min_query_duration_ms=300;
select '01546_log_queries_min_query_duration_ms-slow', sleep(0.4) format Null;
system flush logs query_log, query_thread_log;

-- With the limit on minimum execution time, "query start" and "exception before start" events are not logged, only query finish.
select count()
from system.query_log
where
    query like 'select \'01546_log_queries_min_query_duration_ms-slow%'
    and current_database = currentDatabase()
    and event_date >= yesterday();
-- There at least two threads involved in a simple query
-- (one thread just waits another, sigh)
select if(count() == 2, 'OK', 'Fail: ' || toString(count()))
from system.query_thread_log
where
    query like 'select \'01546_log_queries_min_query_duration_ms-slow%'
    and current_database = currentDatabase()
    and event_date >= yesterday();
