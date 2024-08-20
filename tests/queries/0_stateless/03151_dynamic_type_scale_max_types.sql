SET allow_experimental_dynamic_type=1;
set min_compress_block_size = 585572, max_compress_block_size = 373374, max_block_size = 60768, max_joined_block_size_rows = 18966, max_insert_threads = 5, max_threads = 50, max_read_buffer_size = 708232, connect_timeout_with_failover_ms = 2000, connect_timeout_with_failover_secure_ms = 3000, idle_connection_timeout = 36000, use_uncompressed_cache = true, stream_like_engine_allow_direct_select = true, replication_wait_for_inactive_replica_timeout = 30, compile_aggregate_expressions = false, min_count_to_compile_aggregate_expression = 0, compile_sort_description = false, group_by_two_level_threshold = 1000000, group_by_two_level_threshold_bytes = 12610083, enable_memory_bound_merging_of_aggregation_results = false, min_chunk_bytes_for_parallel_parsing = 18769830, merge_tree_coarse_index_granularity = 12, min_bytes_to_use_direct_io = 10737418240, min_bytes_to_use_mmap_io = 10737418240, log_queries = true, insert_quorum_timeout = 60000, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.05000000074505806, http_response_buffer_size = 294986, fsync_metadata = true, http_send_timeout = 60., http_receive_timeout = 60., opentelemetry_start_trace_probability = 0.10000000149011612, max_bytes_before_external_group_by = 1, max_bytes_before_external_sort = 10737418240, max_bytes_before_remerge_sort = 1326536545, max_untracked_memory = 1048576, memory_profiler_step = 1048576, log_comment = '03151_dynamic_type_scale_max_types.sql', send_logs_level = 'fatal', prefer_localhost_replica = false, optimize_read_in_order = false, optimize_aggregation_in_order = true, aggregation_in_order_max_block_bytes = 27069500, read_in_order_two_level_merge_threshold = 75, allow_introspection_functions = true, database_atomic_wait_for_drop_and_detach_synchronously = true, remote_filesystem_read_method = 'read', local_filesystem_read_prefetch = true, remote_filesystem_read_prefetch = false, merge_tree_compact_parts_min_granules_to_multibuffer_read = 119, async_insert_busy_timeout_max_ms = 5000, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true, filesystem_cache_segments_batch_size = 10, use_page_cache_for_disks_without_file_cache = true, page_cache_inject_eviction = true, allow_prefetched_read_pool_for_remote_filesystem = false, filesystem_prefetch_step_marks = 50, filesystem_prefetch_min_bytes_for_single_read_task = 16777216, filesystem_prefetch_max_memory_usage = 134217728, filesystem_prefetches_limit = 10, optimize_sorting_by_input_stream_properties = false, allow_experimental_dynamic_type = true, session_timezone = 'Africa/Khartoum', prefer_warmed_unmerged_parts_seconds = 2;

drop table if exists to_table;

CREATE TABLE to_table
(
    n1 UInt8,
    n2 Dynamic(max_types=2)
)
ENGINE = MergeTree ORDER BY n1;

INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=5);
INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

select '';
ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=0);
INSERT INTO to_table ( n1, n2 ) VALUES (1, '2024-01-01'), (2, toDateTime64('2024-01-01', 3, 'Asia/Istanbul')), (3, toFloat32(1)), (4, toFloat64(2));
SELECT *, dynamicType(n2), isDynamicElementInSharedData(n2) FROM to_table ORDER BY ALL;

ALTER TABLE to_table MODIFY COLUMN n2 Dynamic(max_types=500); -- { serverError UNEXPECTED_AST_STRUCTURE }
