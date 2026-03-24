-- Regression test: CAST(Nullable(String), 'Int32') must not be placed as the first (eagerly
-- evaluated) argument of the prewhere AND before `IS NOT NULL` when statistics are enabled.

SET max_insert_threads=2, group_by_two_level_threshold=875081, group_by_two_level_threshold_bytes=17497290, distributed_aggregation_memory_efficient=1, fsync_metadata=1, output_format_parallel_formatting=1, input_format_parallel_parsing=1, min_chunk_bytes_for_parallel_parsing=4237185, max_read_buffer_size=713116, prefer_localhost_replica=1, max_block_size=84196, max_joined_block_size_rows=24714, joined_block_split_single_row=0, join_output_by_rowlist_perkey_rows_threshold=313, max_threads=2, optimize_append_index=1, use_hedged_requests=1, optimize_if_chain_to_multiif=0, optimize_if_transform_strings_to_enum=1, optimize_read_in_order=1, optimize_or_like_chain=0, optimize_substitute_columns=1, enable_multiple_prewhere_read_steps=0, read_in_order_two_level_merge_threshold=35, optimize_aggregation_in_order=1, aggregation_in_order_max_block_bytes=24653640, use_uncompressed_cache=0, min_bytes_to_use_direct_io=1, min_bytes_to_use_mmap_io=9171884813, local_filesystem_read_method='pread', remote_filesystem_read_method='threadpool', local_filesystem_read_prefetch=1, filesystem_cache_segments_batch_size=0, read_from_filesystem_cache_if_exists_otherwise_bypass_cache=0, throw_on_error_from_cache_on_write_operations=0, remote_filesystem_read_prefetch=0, distributed_cache_discard_connection_if_unread_data=1, distributed_cache_use_clients_cache_for_write=1, distributed_cache_use_clients_cache_for_read=1, allow_prefetched_read_pool_for_remote_filesystem=0, filesystem_prefetch_max_memory_usage='64Mi', filesystem_prefetches_limit=10, filesystem_prefetch_min_bytes_for_single_read_task='8Mi', filesystem_prefetch_step_marks=0, filesystem_prefetch_step_bytes=0, enable_filesystem_cache=1, enable_filesystem_cache_on_write_operations=1, compile_expressions=0, compile_aggregate_expressions=1, compile_sort_description=1, merge_tree_coarse_index_granularity=16, optimize_distinct_in_order=0, max_bytes_before_remerge_sort=282895694, min_compress_block_size=2322682, max_compress_block_size=2031930, merge_tree_compact_parts_min_granules_to_multibuffer_read=106, optimize_sorting_by_input_stream_properties=1, http_response_buffer_size=533862, http_wait_end_of_query='True', enable_memory_bound_merging_of_aggregation_results=0, min_count_to_compile_expression=3, min_count_to_compile_aggregate_expression=0, min_count_to_compile_sort_description=0, session_timezone='America/Mazatlan', use_page_cache_for_disks_without_file_cache='False', use_page_cache_for_local_disks='False', use_page_cache_for_object_storage='False', page_cache_inject_eviction='False', merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0.64, prefer_external_sort_block_bytes=1, cross_join_min_rows_to_compress=100000000, cross_join_min_bytes_to_compress=1, min_external_table_block_size_bytes=1, max_parsing_threads=0, optimize_functions_to_subcolumns=1, parallel_replicas_local_plan=0, query_plan_join_swap_table='false', enable_vertical_final=1, optimize_extract_common_expressions=0, optimize_rewrite_array_exists_to_has='True', optimize_skip_unused_shards='True', optimize_trivial_approximate_count_query='False', optimize_trivial_insert_select='True', optimize_using_constraints='True', optimize_syntax_fuse_functions='True', optimize_aggregators_of_group_by_keys='True', optimize_and_compare_chain='True', optimize_arithmetic_operations_in_aggregate_functions='True', optimize_count_from_files='True', optimize_distributed_group_by_sharding_key='True', optimize_empty_string_comparisons='True', optimize_group_by_constant_keys='True', optimize_group_by_function_keys='True', optimize_injective_functions_in_group_by='True', optimize_injective_functions_inside_uniq='True', optimize_inverse_dictionary_lookup='True', optimize_move_to_prewhere='True', optimize_multiif_to_if='True', optimize_normalize_count_variants='True', optimize_on_insert='True', optimize_redundant_functions_in_order_by='True', optimize_respect_aliases='True', optimize_rewrite_aggregate_function_with_if='True', optimize_rewrite_regexp_functions='True';

DROP TABLE IF EXISTS test_prewhere_cast_nullable;

CREATE TABLE test_prewhere_cast_nullable (
    id UInt32,
    s  Nullable(String),
    n  Int32
) ENGINE = MergeTree() ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization=0.8254166871841369, prefer_fetch_merged_part_size_threshold=2582375590, vertical_merge_algorithm_min_rows_to_activate=1000000, vertical_merge_algorithm_min_columns_to_activate=1, allow_vertical_merges_from_compact_to_wide_parts=0, min_merge_bytes_to_use_direct_io=5113513236, index_granularity_bytes=25669886, merge_max_block_size=21769, index_granularity=33034, min_bytes_for_wide_part=0, marks_compress_block_size=80135, primary_key_compress_block_size=48251, replace_long_file_name_to_hash=0, max_file_name_length=128, min_bytes_for_full_part_storage=536870912, compact_parts_max_bytes_to_buffer=96154478, compact_parts_max_granules_to_buffer=182, compact_parts_merge_max_bytes_to_prefetch_part=30014705, cache_populated_by_fetch=0, concurrent_part_removal_threshold=70, old_parts_lifetime=10, prewarm_mark_cache=1, use_const_adaptive_granularity=0, enable_index_granularity_compression=0, enable_block_number_column=0, enable_block_offset_column=0, use_primary_key_cache=1, prewarm_primary_key_cache=1, object_serialization_version='v2', object_shared_data_serialization_version='map', object_shared_data_serialization_version_for_zero_level_parts='map', object_shared_data_buckets_for_compact_part=28, object_shared_data_buckets_for_wide_part=26, dynamic_serialization_version='v2', auto_statistics_types='countmin,tdigest,minmax', serialization_info_version='basic', string_serialization_version='with_size_stream', nullable_serialization_version='allow_sparse', enable_shared_storage_snapshot_in_query=0, min_columns_to_activate_adaptive_write_buffer=735, reduce_blocking_parts_sleep_ms=1319, shared_merge_tree_outdated_parts_group_size=2, shared_merge_tree_max_outdated_parts_to_process_at_once=10;

INSERT INTO test_prewhere_cast_nullable VALUES (1, NULL, 99), (2, '50', 97), (3, '200', 98);

ALTER TABLE test_prewhere_cast_nullable MODIFY STATISTICS n TYPE minmax;
ALTER TABLE test_prewhere_cast_nullable MATERIALIZE STATISTICS n;

-- Without statistics: IS NOT NULL is AND arg 0 (original WHERE order preserved) → safe
SELECT id
FROM test_prewhere_cast_nullable
WHERE s IS NOT NULL AND CAST(s, 'Int32') > 0 AND n > 0
ORDER BY id
SETTINGS use_statistics = 0, allow_experimental_statistics = 1;

-- With statistics: must also work — IS NOT NULL must still precede CAST in the prewhere AND.
-- Previously threw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN because the statistics estimator
-- ranked CAST(s,'Int32')>0 as more selective than IS NOT NULL, placing it first in the
-- prewhere AND (where it is eagerly evaluated on all rows including NULL ones).
SELECT id
FROM test_prewhere_cast_nullable
WHERE s IS NOT NULL AND CAST(s, 'Int32') > 0 AND n > 0
ORDER BY id
SETTINGS use_statistics = 1, allow_experimental_statistics = 1;

DROP TABLE test_prewhere_cast_nullable;
