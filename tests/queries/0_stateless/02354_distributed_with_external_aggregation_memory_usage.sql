-- Tags: long, no-tsan, no-msan, no-asan, no-ubsan, no-debug, no-object-storage

SET max_rows_to_read = '101M';

DROP TABLE IF EXISTS t_2354_dist_with_external_aggr;

create table t_2354_dist_with_external_aggr(a UInt64, b String, c FixedString(100)) engine = MergeTree order by tuple() SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';

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
-- it is hard to provide an accurate estimation for memory usage, so 5G is just the actual value taken from the logs + delta
-- also avoid using localhost, so the queries will go over separate connections
-- (otherwise the memory usage for merge will be counted together with the localhost query)
select a, b, c, sum(a) as s
from remote('127.0.0.{2,3}', currentDatabase(), t_2354_dist_with_external_aggr)
group by a, b, c
format Null
settings max_memory_usage = '5Gi', max_result_rows = 0, max_result_bytes = 0;

DROP TABLE t_2354_dist_with_external_aggr;
