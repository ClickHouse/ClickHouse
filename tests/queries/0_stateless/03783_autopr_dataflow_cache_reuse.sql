DROP TABLE IF EXISTS t;

CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization=1.0, prefer_fetch_merged_part_size_threshold=10737418240, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=95, allow_vertical_merges_from_compact_to_wide_parts=0, min_merge_bytes_to_use_direct_io=2846243473, index_granularity_bytes=28777047, merge_max_block_size=708, index_granularity=16024, min_bytes_for_wide_part=0, marks_compress_block_size=94485, primary_key_compress_block_size=76241, replace_long_file_name_to_hash=1, max_file_name_length=0, min_bytes_for_full_part_storage=0, compact_parts_max_bytes_to_buffer=499528437, compact_parts_max_granules_to_buffer=95, compact_parts_merge_max_bytes_to_prefetch_part=21822102, cache_populated_by_fetch=0, concurrent_part_removal_threshold=26, old_parts_lifetime=56, prewarm_mark_cache=1, use_const_adaptive_granularity=0, enable_index_granularity_compression=0, enable_block_number_column=0, enable_block_offset_column=1, prewarm_primary_key_cache=1, object_serialization_version='v2', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map', object_shared_data_buckets_for_compact_part=1, object_shared_data_buckets_for_wide_part=3, dynamic_serialization_version='v2', serialization_info_version='basic', string_serialization_version='single_stream', nullable_serialization_version='allow_sparse', enable_shared_storage_snapshot_in_query=0;

SET local_filesystem_read_prefetch=0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0, local_filesystem_read_method='pread_threadpool', use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=2;

-- Don't apply -> don't apply -> apply
INSERT INTO t SELECT toString(number), number FROM numbers(1e3);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_0'; -- empty cache, don't apply optimization, collect stats

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_1'; -- stats available, don't apply since no benefit
set send_logs_level='none';

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(1e5);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_2'; -- stats available, but we have to recollect since data grew, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_3'; -- stats available, apply
set send_logs_level='none';

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(1e5 + 10000);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_4'; -- stats available, but we have to recollect since data grew, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_5'; -- stats available, apply
set send_logs_level='none';

-- Apply -> apply -> don't apply
TRUNCATE TABLE t;

INSERT INTO t SELECT toString(number), number FROM numbers(1e3);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize';
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_6'; -- stats available, but we have to recollect since data shrinked, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_7'; -- stats available, don't apply since no benefit
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

