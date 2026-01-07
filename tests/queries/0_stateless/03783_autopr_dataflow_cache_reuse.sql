
SET max_insert_threads=1, group_by_two_level_threshold=1000000, group_by_two_level_threshold_bytes=1, distributed_aggregation_memory_efficient=0, fsync_metadata=0, output_format_parallel_formatting=1, input_format_parallel_parsing=0, min_chunk_bytes_for_parallel_parsing=15199142, max_read_buffer_size=933438, prefer_localhost_replica=1, max_block_size=91322, max_joined_block_size_rows=58663, joined_block_split_single_row=0, join_output_by_rowlist_perkey_rows_threshold=505, max_threads=1, optimize_append_index=0, use_hedged_requests=0, optimize_if_chain_to_multiif=0, optimize_if_transform_strings_to_enum=1, optimize_read_in_order=1, optimize_or_like_chain=1, optimize_substitute_columns=1, enable_multiple_prewhere_read_steps=1, read_in_order_two_level_merge_threshold=30, optimize_aggregation_in_order=0, aggregation_in_order_max_block_bytes=37350193, use_uncompressed_cache=1, min_bytes_to_use_direct_io=2544312942, min_bytes_to_use_mmap_io=10737418240, local_filesystem_read_method='read', remote_filesystem_read_method='read', local_filesystem_read_prefetch=1, filesystem_cache_segments_batch_size=50, read_from_filesystem_cache_if_exists_otherwise_bypass_cache=1, throw_on_error_from_cache_on_write_operations=0, remote_filesystem_read_prefetch=0, allow_prefetched_read_pool_for_remote_filesystem=0, filesystem_prefetch_max_memory_usage='128Mi', filesystem_prefetches_limit=0, filesystem_prefetch_min_bytes_for_single_read_task='1Mi', filesystem_prefetch_step_marks=50, filesystem_prefetch_step_bytes='100Mi', compile_expressions=0, compile_aggregate_expressions=1, compile_sort_description=1, merge_tree_coarse_index_granularity=6, optimize_distinct_in_order=0, max_bytes_before_remerge_sort=1259644964, min_compress_block_size=1269774, max_compress_block_size=1156320, merge_tree_compact_parts_min_granules_to_multibuffer_read=84, optimize_sorting_by_input_stream_properties=1, http_response_buffer_size=7357860, http_wait_end_of_query='True', enable_memory_bound_merging_of_aggregation_results=1, min_count_to_compile_expression=3, min_count_to_compile_aggregate_expression=3, min_count_to_compile_sort_description=0, session_timezone='Africa/Juba', use_page_cache_for_disks_without_file_cache='True', page_cache_inject_eviction='True', merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.02, prefer_external_sort_block_bytes=1, cross_join_min_rows_to_compress=0, cross_join_min_bytes_to_compress=100000000, min_external_table_block_size_bytes=1, max_parsing_threads=0, optimize_functions_to_subcolumns=0, parallel_replicas_local_plan=0, query_plan_join_swap_table='auto', enable_vertical_final=0, optimize_extract_common_expressions=0, use_async_executor_for_materialized_views=1, use_query_condition_cache=0, secondary_indices_enable_bulk_filtering=0, use_skip_indexes_if_final=1, use_skip_indexes_on_data_read=1, optimize_rewrite_like_perfect_affix=1, input_format_parquet_use_native_reader_v3=1, enable_lazy_columns_replication=1, allow_special_serialization_kinds_in_output_formats=0, temporary_files_buffer_size=1097725, max_bytes_before_external_sort=0, max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_sort=0.15, max_bytes_ratio_before_external_group_by=0.2, use_skip_indexes_if_final_exact_mode=1;

DROP TABLE IF EXISTS t;

CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization=1.0, prefer_fetch_merged_part_size_threshold=10737418240, vertical_merge_algorithm_min_rows_to_activate=1, vertical_merge_algorithm_min_columns_to_activate=95, allow_vertical_merges_from_compact_to_wide_parts=0, min_merge_bytes_to_use_direct_io=2846243473, index_granularity_bytes=28777047, merge_max_block_size=708, index_granularity=16024, min_bytes_for_wide_part=0, marks_compress_block_size=94485, primary_key_compress_block_size=76241, replace_long_file_name_to_hash=1, max_file_name_length=0, min_bytes_for_full_part_storage=0, compact_parts_max_bytes_to_buffer=499528437, compact_parts_max_granules_to_buffer=95, compact_parts_merge_max_bytes_to_prefetch_part=21822102, cache_populated_by_fetch=0, concurrent_part_removal_threshold=26, old_parts_lifetime=56, prewarm_mark_cache=1, use_const_adaptive_granularity=0, enable_index_granularity_compression=0, enable_block_number_column=0, enable_block_offset_column=1, prewarm_primary_key_cache=1, object_serialization_version='v2', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map', object_shared_data_buckets_for_compact_part=1, object_shared_data_buckets_for_wide_part=3, dynamic_serialization_version='v2', auto_statistics_types='countmin', serialization_info_version='basic', string_serialization_version='single_stream', nullable_serialization_version='allow_sparse', enable_shared_storage_snapshot_in_query=0;

SET local_filesystem_read_prefetch=0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0, local_filesystem_read_method='pread_threadpool', use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

-- Reading of aggregation states from disk will affect `ReadCompressedBytes`
SET max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_group_by=0;

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
SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_6'; -- stats available, but we have to recollect since data grew, don't apply

SELECT key, SUM(value) FROM t GROUP BY key FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_7'; -- stats available, don't apply since no benefit
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0, ProfileEvents['ParallelReplicasUsedCount'] > 0
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment;

