-- Tags: long
-- - long - slow in private

-- Test that `distributed_index_analysis_min_indexes_bytes_to_activate` threshold
-- is evaluated against index sizes of parts surviving partition pruning,
-- not the total index size across all parts in the table.

set allow_experimental_parallel_reading_from_replicas=0;
set cluster_for_parallel_replicas='test_cluster_one_shard_two_replicas';

drop table if exists dist_idx_partition_pruning;
create table dist_idx_partition_pruning (
    part_key UInt8,
    key String,
    value String
) engine=MergeTree()
partition by part_key
order by (key, value)
settings
    index_granularity=200,
    min_bytes_for_wide_part=0,
    index_granularity_bytes=10e6,
    distributed_index_analysis_min_parts_to_activate=0,
    distributed_index_analysis_min_indexes_bytes_to_activate=200000;

system stop merges dist_idx_partition_pruning;
insert into dist_idx_partition_pruning select number % 20, number::String, repeat('a', 100) from numbers(1e6);

-- All partitions: total uncompressed primary index size (~560KB) exceeds the 200KB threshold
select key from dist_idx_partition_pruning settings distributed_index_analysis=1 format Null;

-- Single partition: per-partition uncompressed primary index size (~27KB) is below the 200KB threshold
select key from dist_idx_partition_pruning where part_key = 0 settings distributed_index_analysis=1 format Null;

drop table dist_idx_partition_pruning;

system flush logs query_log;
select
    query like '%part_key%' as has_partition_filter,
    ProfileEvents['DistributedIndexAnalysisMicroseconds'] > 0 as used_distributed_analysis
from system.query_log
where
    current_database = currentDatabase()
    and event_date >= yesterday() AND event_time >= now() - 600
    and type = 'QueryFinish'
    and query_kind = 'Select'
    and is_initial_query
    and has(Settings, 'distributed_index_analysis')
    and endsWith(log_comment, '-' || currentDatabase())
order by event_time_microseconds;
