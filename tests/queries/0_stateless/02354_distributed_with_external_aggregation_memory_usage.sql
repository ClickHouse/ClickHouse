-- Tags: long, no-tsan, no-msan, no-asan, no-ubsan, no-debug, no-s3-storage

create table t_2354_dist_with_external_aggr(a UInt64, b String, c FixedString(100)) engine = MergeTree order by tuple();

insert into t_2354_dist_with_external_aggr select number, toString(number) as s, toFixedString(s, 100) from numbers_mt(5e7);

set max_bytes_before_external_group_by = '2G',
    max_threads = 16,
    aggregation_memory_efficient_merge_threads = 16,
    distributed_aggregation_memory_efficient = 1,
    prefer_localhost_replica = 1,
    group_by_two_level_threshold = 100000,
    group_by_two_level_threshold_bytes = 1000000,
    max_block_size = 65505;

-- whole aggregation state of local aggregation uncompressed is 5.8G
-- it is hard to provide an accurate estimation for memory usage, so 4G is just the actual value taken from the logs + delta
select a, b, c, sum(a) as s
from remote('127.0.0.{1,2}', currentDatabase(), t_2354_dist_with_external_aggr)
group by a, b, c
format Null
settings max_memory_usage = '4Gi';
