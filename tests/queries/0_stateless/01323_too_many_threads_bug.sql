drop table if exists table_01323_many_parts;

set remote_filesystem_read_method='read';

create table table_01323_many_parts (x UInt64) engine = MergeTree order by x partition by x % 100;
set max_partitions_per_insert_block = 100;
insert into table_01323_many_parts select number from numbers(100000);

set max_threads = 16;
set log_queries = 1;
select x from table_01323_many_parts limit 10 format Null;

system flush logs;
select arrayUniq(thread_ids) <= 4 from system.query_log where current_database = currentDatabase() AND event_date >= today() - 1 and query ilike '%select x from table_01323_many_parts%' and query not like '%system.query_log%' and type = 'QueryFinish' order by query_start_time desc limit 1;

select x from table_01323_many_parts order by x limit 10 format Null;

system flush logs;
select arrayUniq(thread_ids) <= 36 from system.query_log where current_database = currentDatabase() AND event_date >= today() - 1 and query ilike '%select x from table_01323_many_parts order by x%' and query not like '%system.query_log%' and type = 'QueryFinish' order by query_start_time desc limit 1;

drop table if exists table_01323_many_parts;
