set log_queries = 1;
set max_threads = 16;

SELECT count() FROM (SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100 UNION ALL SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100 UNION ALL SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100);

system flush logs;
select length(thread_ids) >= 16 from system.query_log where event_date >= today() - 1 and query like '%SELECT count() FROM (SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100 UNION ALL SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100 UNION ALL SELECT number FROM numbers_mt(1000000) ORDER BY number DESC LIMIT 100)%' and type = 'QueryFinish' order by query_start_time desc limit 1;
