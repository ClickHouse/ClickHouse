SET max_insert_threads=1, group_by_two_level_threshold=44022, group_by_two_level_threshold_bytes=14573514, distributed_aggregation_memory_efficient=1, fsync_metadata=1, output_format_parallel_formatting=1, input_format_parallel_parsing=1, min_chunk_bytes_for_parallel_parsing=12337749, max_read_buffer_size=677605, prefer_localhost_replica=0, max_block_size=25674, max_joined_block_size_rows=24191, joined_block_split_single_row=0, join_output_by_rowlist_perkey_rows_threshold=0, max_threads=3, optimize_append_index=1, use_hedged_requests=0, optimize_if_chain_to_multiif=0, optimize_if_transform_strings_to_enum=0, optimize_read_in_order=0, optimize_or_like_chain=0, optimize_substitute_columns=1, enable_multiple_prewhere_read_steps=0, read_in_order_two_level_merge_threshold=54, optimize_aggregation_in_order=1, aggregation_in_order_max_block_bytes=1689218, use_uncompressed_cache=1, min_bytes_to_use_direct_io=7553693339, min_bytes_to_use_mmap_io=1380134967, local_filesystem_read_method='mmap', remote_filesystem_read_method='read', local_filesystem_read_prefetch=1, filesystem_cache_segments_batch_size=0, read_from_filesystem_cache_if_exists_otherwise_bypass_cache=1, throw_on_error_from_cache_on_write_operations=1, remote_filesystem_read_prefetch=1, allow_prefetched_read_pool_for_remote_filesystem=1, filesystem_prefetch_max_memory_usage='64Mi', filesystem_prefetches_limit=10, filesystem_prefetch_min_bytes_for_single_read_task='16Mi', filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes='100Mi', compile_expressions=1, compile_aggregate_expressions=1, compile_sort_description=1, merge_tree_coarse_index_granularity=23, optimize_distinct_in_order=0, max_bytes_before_remerge_sort=2005824367, min_compress_block_size=2244746, max_compress_block_size=1067200, merge_tree_compact_parts_min_granules_to_multibuffer_read=29, optimize_sorting_by_input_stream_properties=1, http_response_buffer_size=914573, http_wait_end_of_query='False', enable_memory_bound_merging_of_aggregation_results=0, min_count_to_compile_expression=3, min_count_to_compile_aggregate_expression=0, min_count_to_compile_sort_description=0, session_timezone='Mexico/BajaSur', use_page_cache_for_disks_without_file_cache='True', page_cache_inject_eviction='False', merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.15, prefer_external_sort_block_bytes=1, cross_join_min_rows_to_compress=0, cross_join_min_bytes_to_compress=100000000, min_external_table_block_size_bytes=1, max_parsing_threads=1, optimize_functions_to_subcolumns=1, parallel_replicas_local_plan=1, query_plan_join_swap_table='false', enable_vertical_final=1, optimize_extract_common_expressions=0, use_async_executor_for_materialized_views=1, use_query_condition_cache=1, secondary_indices_enable_bulk_filtering=1, use_skip_indexes_if_final=0, use_skip_indexes_on_data_read=0, optimize_rewrite_like_perfect_affix=0, input_format_parquet_use_native_reader_v3=1, enable_lazy_columns_replication=0, allow_special_serialization_kinds_in_output_formats=0, automatic_parallel_replicas_mode=0, temporary_files_buffer_size=864449, query_plan_optimize_join_order_algorithm='greedy,dpsize', max_bytes_before_external_sort=0, max_bytes_before_external_group_by=0, max_bytes_ratio_before_external_sort=0.0, max_bytes_ratio_before_external_group_by=0.2, use_skip_indexes_if_final_exact_mode=0;

DROP TABLE IF EXISTS t;

CREATE TABLE t(key String, value UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS  index_granularity=8192, ratio_of_defaults_for_sparse_serialization=0.024313723905809548, prefer_fetch_merged_part_size_threshold=10737418240, vertical_merge_algorithm_min_rows_to_activate=1000000, vertical_merge_algorithm_min_columns_to_activate=2, allow_vertical_merges_from_compact_to_wide_parts=1, min_merge_bytes_to_use_direct_io=1, index_granularity_bytes=4124094, merge_max_block_size=20255, min_bytes_for_wide_part=0, marks_compress_block_size=48870, primary_key_compress_block_size=88164, replace_long_file_name_to_hash=1, max_file_name_length=117, min_bytes_for_full_part_storage=286678319, compact_parts_max_bytes_to_buffer=174483179, compact_parts_max_granules_to_buffer=185, compact_parts_merge_max_bytes_to_prefetch_part=12741058, cache_populated_by_fetch=0, concurrent_part_removal_threshold=100, old_parts_lifetime=480, prewarm_mark_cache=1, use_const_adaptive_granularity=0, enable_index_granularity_compression=0, enable_block_number_column=0, enable_block_offset_column=0, use_primary_key_cache=0, prewarm_primary_key_cache=1, object_serialization_version='v3', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map_with_buckets', object_shared_data_buckets_for_compact_part=28, object_shared_data_buckets_for_wide_part=3, dynamic_serialization_version='v3', auto_statistics_types='uniq,tdigest', serialization_info_version='with_types', string_serialization_version='single_stream', nullable_serialization_version='allow_sparse', enable_shared_storage_snapshot_in_query=1, min_columns_to_activate_adaptive_write_buffer=580;

SET local_filesystem_read_prefetch=0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0, local_filesystem_read_method='pread_threadpool', use_uncompressed_cache=0;

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=1, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET max_threads=2;

-- For runs with the old analyzer
SET parallel_replicas_only_with_analyzer=0;

SET use_query_condition_cache=1;
SET automatic_parallel_replicas_min_bytes_per_replica='1Mi';

INSERT INTO t SELECT 'ololokekkekkek' || toString(number % 10), number FROM numbers(1e6);

--set send_logs_level='trace', send_logs_source_regexp = 'optimize|SelectExecutor';
SELECT SUM(value) FROM t FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_0'; -- empty cache, don't apply optimization, collect stats

SELECT SUM(value) FROM t FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_1'; -- stats available, apply

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_2'; -- empty cache, don't apply optimization, collect stats

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_3'; -- stats available, but we have to recollect since data shrinked (due to pruning by QCC), don't apply

SELECT SUM(value) FROM t WHERE value = 42 FORMAT Null SETTINGS log_comment='03783_autopr_dataflow_cache_reuse_query_4'; -- stats available, don't apply since no benefit
set send_logs_level='none';

SET enable_parallel_replicas=0, automatic_parallel_replicas_mode=0;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment query, ProfileEvents['RuntimeDataflowStatisticsInputBytes'] > 0 stats_collected, ProfileEvents['ParallelReplicasUsedCount'] > 0 pr_used
FROM system.query_log
WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE '03783_autopr_dataflow_cache_reuse_query_%') AND (type = 'QueryFinish')
ORDER BY log_comment
FORMAT TSVWithNames;

