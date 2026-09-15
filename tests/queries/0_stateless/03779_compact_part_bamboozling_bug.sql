-- Tags: long, no-asan, no-msan, no-tsan, no-ubsan

drop table if exists t_dio;

-- Slow down the test
SET min_bytes_to_use_direct_io=0, local_filesystem_read_prefetch=0, use_uncompressed_cache=0;

SET max_block_size=16342;
SET max_threads=1;

set optimize_read_in_order=0, optimize_distinct_in_order=1;
set enable_parallel_replicas=1, max_parallel_replicas=3, cluster_for_parallel_replicas='test_cluster_1_shard_3_replicas_1_unavailable', parallel_replicas_for_non_replicated_merge_tree=1, parallel_replicas_local_plan=1;

create table t_dio (a int, b int, c int) engine=MergeTree() order by () settings index_granularity=1, compact_parts_max_granules_to_buffer=59;
insert into t_dio select 1, number % 5, number % 10 from numbers(1,34759);

select min(a), max(a), min(b), max(b), min(c), max(c) from t_dio;

drop table if exists t_dio;
