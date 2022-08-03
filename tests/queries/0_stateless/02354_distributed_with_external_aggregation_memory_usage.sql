-- Tags: long, no-tsan, no-msan, no-asan, no-ubsan

create table t_2354_dist_with_external_aggr(a UInt64, b String, c FixedString(100)) engine = MergeTree order by tuple();

insert into t_2354_dist_with_external_aggr select number, toString(number) as s, toFixedString(s, 100) from numbers_mt(5e7);

set max_bytes_before_external_group_by = '2G',
    max_threads = 16,
    aggregation_memory_efficient_merge_threads = 16,
    distributed_aggregation_memory_efficient = 1,
    prefer_localhost_replica = 1,
    group_by_two_level_threshold = 100000;

select a, b, c, sum(a) as s
from remote('127.0.0.{1,2}', currentDatabase(), t_2354_dist_with_external_aggr)
group by a, b, c
format Null;

system flush logs;

select memory_usage < 4 * 1024 * 1024 * 1024 -- whole aggregation state of local aggregation uncompressed is 5.8G
from system.query_log
where event_time >= now() - interval '15 minute' and type = 'QueryFinish' and is_initial_query and query like '%t_2354_dist_with_external_aggr%group_by%' and current_database = currentDatabase();
