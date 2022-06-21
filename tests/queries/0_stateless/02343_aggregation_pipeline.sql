-- { echoOn }

explain pipeline select * from (select * from numbers_mt(1e8) group by number) group by number settings max_threads = 16;

explain pipeline select * from (select * from numbers_mt(1e8) group by number) order by number settings max_threads = 16;

explain pipeline select number from remote('127.0.0.{1,2,3}', system, numbers_mt) group by number settings max_threads = 16, prefer_localhost_replica = 1, distributed_aggregation_memory_efficient = 1;

explain pipeline select number from remote('127.0.0.{1,2,3}', system, numbers_mt) group by number settings max_threads = 16, prefer_localhost_replica = 1, distributed_aggregation_memory_efficient = 0;
