-- Tags: no-parallel, no-fasttest
-- no-parallel: it checks the number of threads, which can be lowered in presence of other queries

set log_queries = 1;
set max_threads = 16;
set prefer_localhost_replica = 1;

select sum(number) from remote('127.0.0.{1|2}', numbers_mt(1000000)) group by number % 2 order by number % 2;

system flush logs query_log;
select length(thread_ids) >= 1 from system.query_log where current_database = currentDatabase() and event_date >= today() - 1 and lower(query) like '%select sum(number) from remote(_127.0.0.{1|2}_, numbers_mt(1000000)) group by number %' and type = 'QueryFinish' order by query_start_time desc limit 1;
