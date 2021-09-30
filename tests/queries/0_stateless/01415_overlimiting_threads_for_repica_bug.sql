set log_queries = 1;
set max_threads = 16;

select sum(number) from remote('127.0.0.{1|2}', numbers_mt(1000000)) group by number % 2 order by number % 2;

system flush logs;
select length(thread_ids) >= 16 from system.query_log where current_database = currentDatabase() and event_date >= today() - 1 and lower(query) like '%select sum(number) from remote(_127.0.0.{1|2}_, numbers_mt(1000000)) group by number %' and type = 'QueryFinish' order by query_start_time desc limit 1;
