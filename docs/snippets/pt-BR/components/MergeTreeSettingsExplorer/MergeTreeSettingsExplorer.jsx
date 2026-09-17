const MergeTreeSettingsExplorer = ({ href: baseRoute }) => {
  // Mintlify's production renderer evaluates the exported component without
  // preserving module-scope bindings. Lazy state keeps the generated data in
  // that evaluation scope while constructing it only once per mount.
  const [entries] = useState(() => [
    {
      label: "add_minmax_*",
      count: 5,
      settings: [
        { name: "add_minmax_index_for_block_number_column", path: "/add-minmax#add_minmax_index_for_block_number_column", default: "0" },
        { name: "add_minmax_index_for_block_offset_column", path: "/add-minmax#add_minmax_index_for_block_offset_column", default: "0" },
        { name: "add_minmax_index_for_numeric_columns", path: "/add-minmax#add_minmax_index_for_numeric_columns", default: "0" },
        { name: "add_minmax_index_for_string_columns", path: "/add-minmax#add_minmax_index_for_string_columns", default: "0" },
        { name: "add_minmax_index_for_temporal_columns", path: "/add-minmax#add_minmax_index_for_temporal_columns", default: "0" }
      ],
      children: []
    },
    {
      label: "allow_*",
      count: 13,
      settings: [
        { name: "allow_coalescing_columns_in_partition_or_order_key", path: "/allow#allow_coalescing_columns_in_partition_or_order_key", default: "0" },
        { name: "allow_commit_order_projection", path: "/allow#allow_commit_order_projection", default: "0" },
        { name: "allow_dimensions_outside_sorting_key", path: "/allow#allow_dimensions_outside_sorting_key", default: "0" },
        { name: "allow_floating_point_partition_key", path: "/allow#allow_floating_point_partition_key", default: "0" },
        { name: "allow_minmax_index_for_json", path: "/allow#allow_minmax_index_for_json", default: "0" },
        { name: "allow_nullable_key", path: "/allow#allow_nullable_key", default: "0" },
        { name: "allow_part_offset_column_in_projections", path: "/allow#allow_part_offset_column_in_projections", default: "1" },
        { name: "allow_reduce_blocking_parts_task", path: "/allow#allow_reduce_blocking_parts_task", default: "1" },
        { name: "allow_remote_fs_zero_copy_replication", path: "/allow#allow_remote_fs_zero_copy_replication", default: "0" },
        { name: "allow_summing_columns_in_partition_or_order_key", path: "/allow#allow_summing_columns_in_partition_or_order_key", default: "0" },
        { name: "allow_suspicious_indices", path: "/allow#allow_suspicious_indices", default: "0" },
        { name: "allow_tuple_element_aggregation", path: "/allow#allow_tuple_element_aggregation", default: "0" },
        { name: "allow_vertical_merges_from_compact_to_wide_parts", path: "/allow#allow_vertical_merges_from_compact_to_wide_parts", default: "1" }
      ],
      children: []
    },
    {
      label: "allow_experimental_*",
      count: 4,
      settings: [
        { name: "allow_experimental_adaptive_codec_selection", path: "/allow-experimental#allow_experimental_adaptive_codec_selection", default: "0" },
        { name: "allow_experimental_replacing_merge_with_cleanup", path: "/allow-experimental#allow_experimental_replacing_merge_with_cleanup", default: "0" },
        { name: "allow_experimental_reverse_key", path: "/allow-experimental#allow_experimental_reverse_key", default: "0" },
        { name: "allow_experimental_text_index_phrase_search", path: "/allow-experimental#allow_experimental_text_index_phrase_search", default: "0" }
      ],
      children: []
    },
    {
      label: "always_fetch_*",
      count: 2,
      settings: [
        { name: "always_fetch_merged_part", path: "/always-fetch#always_fetch_merged_part", default: "0" },
        { name: "always_fetch_mutated_part", path: "/always-fetch#always_fetch_mutated_part", default: "0" }
      ],
      children: []
    },
    {
      label: "async_*",
      count: 2,
      settings: [
        { name: "async_block_ids_cache_update_wait_ms", path: "/async#async_block_ids_cache_update_wait_ms", default: "100" },
        { name: "async_insert", path: "/async#async_insert", default: "0" }
      ],
      children: []
    },
    {
      label: "cache_populated_by_fetch_*",
      count: 2,
      settings: [
        { name: "cache_populated_by_fetch", path: "/cache-populated-by-fetch#cache_populated_by_fetch", default: "0" },
        { name: "cache_populated_by_fetch_filename_regexp", path: "/cache-populated-by-fetch#cache_populated_by_fetch_filename_regexp", default: '""' }
      ],
      children: []
    },
    {
      label: "check_*",
      count: 2,
      settings: [
        { name: "check_delay_period", path: "/check#check_delay_period", default: "60" },
        { name: "check_sample_column_is_correct", path: "/check#check_sample_column_is_correct", default: "1" }
      ],
      children: []
    },
    {
      label: "cleanup_*",
      count: 2,
      settings: [
        { name: "cleanup_thread_preferred_points_per_iteration", path: "/cleanup#cleanup_thread_preferred_points_per_iteration", default: "150" },
        { name: "cleanup_threads", path: "/cleanup#cleanup_threads", default: "128" }
      ],
      children: []
    },
    {
      label: "cleanup_delay_period_*",
      count: 2,
      settings: [
        { name: "cleanup_delay_period", path: "/cleanup-delay-period#cleanup_delay_period", default: "30" },
        { name: "cleanup_delay_period_random_add", path: "/cleanup-delay-period#cleanup_delay_period_random_add", default: "10" }
      ],
      children: []
    },
    {
      label: "columns_*",
      count: 2,
      settings: [
        { name: "columns_and_secondary_indices_sizes_lazy_calculation", path: "/columns#columns_and_secondary_indices_sizes_lazy_calculation", default: "1" },
        { name: "columns_to_prewarm_mark_cache", path: "/columns#columns_to_prewarm_mark_cache", default: '""' }
      ],
      children: []
    },
    {
      label: "compact_parts_*",
      count: 3,
      settings: [
        { name: "compact_parts_max_bytes_to_buffer", path: "/compact-parts#compact_parts_max_bytes_to_buffer", default: "134217728" },
        { name: "compact_parts_max_granules_to_buffer", path: "/compact-parts#compact_parts_max_granules_to_buffer", default: "128" },
        { name: "compact_parts_merge_max_bytes_to_prefetch_part", path: "/compact-parts#compact_parts_merge_max_bytes_to_prefetch_part", default: "16777216" }
      ],
      children: []
    },
    {
      label: "compress_*",
      count: 3,
      settings: [
        { name: "compress_marks", path: "/compress#compress_marks", default: "1" },
        { name: "compress_per_column_in_compact_parts", path: "/compress#compress_per_column_in_compact_parts", default: "1" },
        { name: "compress_primary_key", path: "/compress#compress_primary_key", default: "1" }
      ],
      children: []
    },
    {
      label: "concurrent_part_removal_threshold_*",
      count: 2,
      settings: [
        { name: "concurrent_part_removal_threshold", path: "/concurrent-part-removal-threshold#concurrent_part_removal_threshold", default: "100" },
        { name: "concurrent_part_removal_threshold_for_remote_disk", path: "/concurrent-part-removal-threshold#concurrent_part_removal_threshold_for_remote_disk", default: "16" }
      ],
      children: []
    },
    {
      label: "dead_blobs_*",
      count: 2,
      settings: [
        { name: "dead_blobs_to_delay_insert", path: "/dead-blobs#dead_blobs_to_delay_insert", default: "100000" },
        { name: "dead_blobs_to_throw_insert", path: "/dead-blobs#dead_blobs_to_throw_insert", default: "1000000" }
      ],
      children: []
    },
    {
      label: "detach_*",
      count: 2,
      settings: [
        { name: "detach_not_byte_identical_parts", path: "/detach#detach_not_byte_identical_parts", default: "0" },
        { name: "detach_old_local_parts_when_cloning_replica", path: "/detach#detach_old_local_parts_when_cloning_replica", default: "1" }
      ],
      children: []
    },
    {
      label: "disable_*",
      count: 3,
      settings: [
        { name: "disable_detach_partition_for_zero_copy_replication", path: "/disable#disable_detach_partition_for_zero_copy_replication", default: "1" },
        { name: "disable_fetch_partition_for_zero_copy_replication", path: "/disable#disable_fetch_partition_for_zero_copy_replication", default: "1" },
        { name: "disable_freeze_partition_for_zero_copy_replication", path: "/disable#disable_freeze_partition_for_zero_copy_replication", default: "1" }
      ],
      children: []
    },
    {
      label: "distributed_index_*",
      count: 2,
      settings: [
        { name: "distributed_index_analysis_min_indexes_bytes_to_activate", path: "/distributed-index#distributed_index_analysis_min_indexes_bytes_to_activate", default: "1073741824" },
        { name: "distributed_index_analysis_min_parts_to_activate", path: "/distributed-index#distributed_index_analysis_min_parts_to_activate", default: "10" }
      ],
      children: []
    },
    {
      label: "enable_*",
      count: 6,
      settings: [
        { name: "enable_index_granularity_compression", path: "/enable#enable_index_granularity_compression", default: "1" },
        { name: "enable_max_bytes_limit_for_min_age_to_force_merge", path: "/enable#enable_max_bytes_limit_for_min_age_to_force_merge", default: "1" },
        { name: "enable_mixed_granularity_parts", path: "/enable#enable_mixed_granularity_parts", default: "1" },
        { name: "enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", path: "/enable#enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", default: "0" },
        { name: "enable_the_endpoint_id_with_zookeeper_name_prefix", path: "/enable#enable_the_endpoint_id_with_zookeeper_name_prefix", default: "0" },
        { name: "enable_vertical_merge_algorithm", path: "/enable#enable_vertical_merge_algorithm", default: "1" }
      ],
      children: []
    },
    {
      label: "enable_block_*",
      count: 2,
      settings: [
        { name: "enable_block_number_column", path: "/enable-block#enable_block_number_column", default: "0" },
        { name: "enable_block_offset_column", path: "/enable-block#enable_block_offset_column", default: "0" }
      ],
      children: []
    },
    {
      label: "escape_*",
      count: 2,
      settings: [
        { name: "escape_index_filenames", path: "/escape#escape_index_filenames", default: "1" },
        { name: "escape_variant_subcolumn_filenames", path: "/escape#escape_variant_subcolumn_filenames", default: "1" }
      ],
      children: []
    },
    {
      label: "exclude_*",
      count: 2,
      settings: [
        { name: "exclude_deleted_rows_for_part_size_in_merge", path: "/exclude#exclude_deleted_rows_for_part_size_in_merge", default: "0" },
        { name: "exclude_materialize_skip_indexes_on_merge", path: "/exclude#exclude_materialize_skip_indexes_on_merge", default: '""' }
      ],
      children: []
    },
    {
      label: "fault_probability_*",
      count: 2,
      settings: [
        { name: "fault_probability_after_part_commit", path: "/fault-probability#fault_probability_after_part_commit", default: "0" },
        { name: "fault_probability_before_part_commit", path: "/fault-probability#fault_probability_before_part_commit", default: "0" }
      ],
      children: []
    },
    {
      label: "fsync_*",
      count: 2,
      settings: [
        { name: "fsync_after_insert", path: "/fsync#fsync_after_insert", default: "0" },
        { name: "fsync_part_directory", path: "/fsync#fsync_part_directory", default: "0" }
      ],
      children: []
    },
    {
      label: "in_memory_*",
      count: 2,
      settings: [
        { name: "in_memory_parts_enable_wal", path: "/in-memory#in_memory_parts_enable_wal", default: "1" },
        { name: "in_memory_parts_insert_sync", path: "/in-memory#in_memory_parts_insert_sync", default: "0" }
      ],
      children: []
    },
    {
      label: "inactive_parts_*",
      count: 2,
      settings: [
        { name: "inactive_parts_to_delay_insert", path: "/inactive-parts#inactive_parts_to_delay_insert", default: "0" },
        { name: "inactive_parts_to_throw_insert", path: "/inactive-parts#inactive_parts_to_throw_insert", default: "0" }
      ],
      children: []
    },
    {
      label: "index_granularity_*",
      count: 2,
      settings: [
        { name: "index_granularity", path: "/index-granularity#index_granularity", default: "8192" },
        { name: "index_granularity_bytes", path: "/index-granularity#index_granularity_bytes", default: "10485760" }
      ],
      children: []
    },
    {
      label: "kill_delay_period_*",
      count: 2,
      settings: [
        { name: "kill_delay_period", path: "/kill-delay-period#kill_delay_period", default: "30" },
        { name: "kill_delay_period_random_add", path: "/kill-delay-period#kill_delay_period_random_add", default: "10" }
      ],
      children: []
    },
    {
      label: "map_buckets_*",
      count: 3,
      settings: [
        { name: "map_buckets_coefficient", path: "/map-buckets#map_buckets_coefficient", default: "1" },
        { name: "map_buckets_min_avg_size", path: "/map-buckets#map_buckets_min_avg_size", default: "32" },
        { name: "map_buckets_strategy", path: "/map-buckets#map_buckets_strategy", default: "sqrt" }
      ],
      children: []
    },
    {
      label: "map_serialization_version_*",
      count: 2,
      settings: [
        { name: "map_serialization_version", path: "/map-serialization-version#map_serialization_version", default: "basic" },
        { name: "map_serialization_version_for_zero_level_parts", path: "/map-serialization-version#map_serialization_version_for_zero_level_parts", default: "basic" }
      ],
      children: []
    },
    {
      label: "marks_*",
      count: 2,
      settings: [
        { name: "marks_compress_block_size", path: "/marks#marks_compress_block_size", default: "65536" },
        { name: "marks_compression_codec", path: "/marks#marks_compression_codec", default: "ZSTD(3)" }
      ],
      children: []
    },
    {
      label: "materialize_*",
      count: 3,
      settings: [
        { name: "materialize_skip_indexes_on_merge", path: "/materialize#materialize_skip_indexes_on_merge", default: "1" },
        { name: "materialize_statistics_on_merge", path: "/materialize#materialize_statistics_on_merge", default: "1" },
        { name: "materialize_ttl_recalculate_only", path: "/materialize#materialize_ttl_recalculate_only", default: "0" }
      ],
      children: []
    },
    {
      label: "materialize_projections_*",
      count: 2,
      settings: [
        { name: "materialize_projections_on_insert", path: "/materialize-projections#materialize_projections_on_insert", default: "1" },
        { name: "materialize_projections_on_merge", path: "/materialize-projections#materialize_projections_on_merge", default: "0" }
      ],
      children: []
    },
    {
      label: "max_*",
      count: 10,
      settings: [
        { name: "max_avg_part_size_for_too_many_parts", path: "/max#max_avg_part_size_for_too_many_parts", default: "1073741824" },
        { name: "max_buckets_in_map", path: "/max#max_buckets_in_map", default: "32" },
        { name: "max_cleanup_delay_period", path: "/max#max_cleanup_delay_period", default: "300" },
        { name: "max_compress_block_size", path: "/max#max_compress_block_size", default: "0" },
        { name: "max_concurrent_queries", path: "/max#max_concurrent_queries", default: "0" },
        { name: "max_digestion_size_per_segment", path: "/max#max_digestion_size_per_segment", default: "268435456" },
        { name: "max_file_name_length", path: "/max#max_file_name_length", default: "127" },
        { name: "max_partitions_to_read", path: "/max#max_partitions_to_read", default: "-1" },
        { name: "max_projections", path: "/max#max_projections", default: "25" },
        { name: "max_uncompressed_bytes_in_patches", path: "/max#max_uncompressed_bytes_in_patches", default: "32212254720" }
      ],
      children: []
    },
    {
      label: "max_bytes_*",
      count: 2,
      settings: [
        { name: "max_bytes_to_merge_at_max_space_in_pool", path: "/max-bytes#max_bytes_to_merge_at_max_space_in_pool", default: "161061273600" },
        { name: "max_bytes_to_merge_at_min_space_in_pool", path: "/max-bytes#max_bytes_to_merge_at_min_space_in_pool", default: "1048576" }
      ],
      children: []
    },
    {
      label: "max_delay_*",
      count: 2,
      settings: [
        { name: "max_delay_to_insert", path: "/max-delay#max_delay_to_insert", default: "1" },
        { name: "max_delay_to_mutate_ms", path: "/max-delay#max_delay_to_mutate_ms", default: "1000" }
      ],
      children: []
    },
    {
      label: "max_files_*",
      count: 2,
      settings: [
        { name: "max_files_to_modify_in_alter_columns", path: "/max-files#max_files_to_modify_in_alter_columns", default: "75" },
        { name: "max_files_to_remove_in_alter_columns", path: "/max-files#max_files_to_remove_in_alter_columns", default: "50" }
      ],
      children: []
    },
    {
      label: "max_merge_*",
      count: 2,
      settings: [
        { name: "max_merge_delayed_streams_for_parallel_write", path: "/max-merge#max_merge_delayed_streams_for_parallel_write", default: "40" },
        { name: "max_merge_selecting_sleep_ms", path: "/max-merge#max_merge_selecting_sleep_ms", default: "60000" }
      ],
      children: []
    },
    {
      label: "max_number_*",
      count: 2,
      settings: [
        { name: "max_number_of_merges_with_ttl_in_pool", path: "/max-number#max_number_of_merges_with_ttl_in_pool", default: "2" },
        { name: "max_number_of_mutations_for_replica", path: "/max-number#max_number_of_mutations_for_replica", default: "0" }
      ],
      children: []
    },
    {
      label: "max_part_*",
      count: 2,
      settings: [
        { name: "max_part_loading_threads", path: "/max-part#max_part_loading_threads", default: "auto(16)" },
        { name: "max_part_removal_threads", path: "/max-part#max_part_removal_threads", default: "auto(16)" }
      ],
      children: []
    },
    {
      label: "max_parts_*",
      count: 2,
      settings: [
        { name: "max_parts_in_total", path: "/max-parts#max_parts_in_total", default: "100000" },
        { name: "max_parts_to_merge_at_once", path: "/max-parts#max_parts_to_merge_at_once", default: "100" }
      ],
      children: []
    },
    {
      label: "max_postpone_*",
      count: 4,
      settings: [
        { name: "max_postpone_time_for_failed_mutations_ms", path: "/max-postpone#max_postpone_time_for_failed_mutations_ms", default: "300000" },
        { name: "max_postpone_time_for_failed_replicated_fetches_ms", path: "/max-postpone#max_postpone_time_for_failed_replicated_fetches_ms", default: "60000" },
        { name: "max_postpone_time_for_failed_replicated_merges_ms", path: "/max-postpone#max_postpone_time_for_failed_replicated_merges_ms", default: "60000" },
        { name: "max_postpone_time_for_failed_replicated_tasks_ms", path: "/max-postpone#max_postpone_time_for_failed_replicated_tasks_ms", default: "300000" }
      ],
      children: []
    },
    {
      label: "max_replicated_*",
      count: 6,
      settings: [
        { name: "max_replicated_fetches_network_bandwidth", path: "/max-replicated#max_replicated_fetches_network_bandwidth", default: "0" },
        { name: "max_replicated_logs_to_keep", path: "/max-replicated#max_replicated_logs_to_keep", default: "1000" },
        { name: "max_replicated_merges_in_queue", path: "/max-replicated#max_replicated_merges_in_queue", default: "1000" },
        { name: "max_replicated_merges_with_ttl_in_queue", path: "/max-replicated#max_replicated_merges_with_ttl_in_queue", default: "1" },
        { name: "max_replicated_mutations_in_queue", path: "/max-replicated#max_replicated_mutations_in_queue", default: "8" },
        { name: "max_replicated_sends_network_bandwidth", path: "/max-replicated#max_replicated_sends_network_bandwidth", default: "0" }
      ],
      children: []
    },
    {
      label: "max_suspicious_broken_parts_*",
      count: 2,
      settings: [
        { name: "max_suspicious_broken_parts", path: "/max-suspicious-broken-parts#max_suspicious_broken_parts", default: "100" },
        { name: "max_suspicious_broken_parts_bytes", path: "/max-suspicious-broken-parts#max_suspicious_broken_parts_bytes", default: "1073741824" }
      ],
      children: []
    },
    {
      label: "merge_*",
      count: 3,
      settings: [
        { name: "merge_total_max_bytes_to_prewarm_cache", path: "/merge#merge_total_max_bytes_to_prewarm_cache", default: "16106127360" },
        { name: "merge_use_batch_sorting_queue", path: "/merge#merge_use_batch_sorting_queue", default: "0" },
        { name: "merge_workload", path: "/merge#merge_workload", default: '""' }
      ],
      children: []
    },
    {
      label: "merge_max_*",
      count: 5,
      settings: [
        { name: "merge_max_block_size", path: "/merge-max#merge_max_block_size", default: "8192" },
        { name: "merge_max_block_size_bytes", path: "/merge-max#merge_max_block_size_bytes", default: "10485760" },
        { name: "merge_max_bytes_to_prewarm_cache", path: "/merge-max#merge_max_bytes_to_prewarm_cache", default: "1073741824" },
        { name: "merge_max_dynamic_subcolumns_in_compact_part", path: "/merge-max#merge_max_dynamic_subcolumns_in_compact_part", default: "auto" },
        { name: "merge_max_dynamic_subcolumns_in_wide_part", path: "/merge-max#merge_max_dynamic_subcolumns_in_wide_part", default: "auto" }
      ],
      children: []
    },
    {
      label: "merge_selecting_*",
      count: 2,
      settings: [
        { name: "merge_selecting_sleep_ms", path: "/merge-selecting#merge_selecting_sleep_ms", default: "5000" },
        { name: "merge_selecting_sleep_slowdown_factor", path: "/merge-selecting#merge_selecting_sleep_slowdown_factor", default: "1.2" }
      ],
      children: []
    },
    {
      label: "merge_selector_*",
      count: 7,
      settings: [
        { name: "merge_selector_algorithm", path: "/merge-selector#merge_selector_algorithm", default: "Simple" },
        { name: "merge_selector_base", path: "/merge-selector#merge_selector_base", default: "5" },
        { name: "merge_selector_blurry_base_scale_factor", path: "/merge-selector#merge_selector_blurry_base_scale_factor", default: "0" },
        { name: "merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once", path: "/merge-selector#merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once", default: "1" },
        { name: "merge_selector_enable_heuristic_to_remove_small_parts_at_right", path: "/merge-selector#merge_selector_enable_heuristic_to_remove_small_parts_at_right", default: "1" },
        { name: "merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent", path: "/merge-selector#merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent", default: "5" },
        { name: "merge_selector_window_size", path: "/merge-selector#merge_selector_window_size", default: "1000" }
      ],
      children: []
    },
    {
      label: "merge_tree_*",
      count: 4,
      settings: [
        { name: "merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds", path: "/merge-tree#merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds", default: "2592000" },
        { name: "merge_tree_clear_old_parts_interval_seconds", path: "/merge-tree#merge_tree_clear_old_parts_interval_seconds", default: "1" },
        { name: "merge_tree_clear_old_temporary_directories_interval_seconds", path: "/merge-tree#merge_tree_clear_old_temporary_directories_interval_seconds", default: "60" },
        { name: "merge_tree_enable_clear_old_broken_detached", path: "/merge-tree#merge_tree_enable_clear_old_broken_detached", default: "0" }
      ],
      children: []
    },
    {
      label: "merge_with_*",
      count: 2,
      settings: [
        { name: "merge_with_recompression_ttl_timeout", path: "/merge-with#merge_with_recompression_ttl_timeout", default: "14400" },
        { name: "merge_with_ttl_timeout", path: "/merge-with#merge_with_ttl_timeout", default: "14400" }
      ],
      children: []
    },
    {
      label: "min_*",
      count: 8,
      settings: [
        { name: "min_absolute_delay_to_close", path: "/min#min_absolute_delay_to_close", default: "0" },
        { name: "min_columns_to_activate_adaptive_write_buffer", path: "/min#min_columns_to_activate_adaptive_write_buffer", default: "500" },
        { name: "min_compress_block_size", path: "/min#min_compress_block_size", default: "0" },
        { name: "min_index_granularity_bytes", path: "/min#min_index_granularity_bytes", default: "1024" },
        { name: "min_marks_to_honor_max_concurrent_queries", path: "/min#min_marks_to_honor_max_concurrent_queries", default: "0" },
        { name: "min_merge_bytes_to_use_direct_io", path: "/min#min_merge_bytes_to_use_direct_io", default: "10737418240" },
        { name: "min_parts_to_merge_at_once", path: "/min#min_parts_to_merge_at_once", default: "0" },
        { name: "min_replicated_logs_to_keep", path: "/min#min_replicated_logs_to_keep", default: "10" }
      ],
      children: []
    },
    {
      label: "min_age_*",
      count: 2,
      settings: [
        { name: "min_age_to_force_merge_on_partition_only", path: "/min-age#min_age_to_force_merge_on_partition_only", default: "0" },
        { name: "min_age_to_force_merge_seconds", path: "/min-age#min_age_to_force_merge_seconds", default: "0" }
      ],
      children: []
    },
    {
      label: "min_bytes_*",
      count: 5,
      settings: [
        { name: "min_bytes_for_compact_part", path: "/min-bytes#min_bytes_for_compact_part", default: "0" },
        { name: "min_bytes_for_full_part_storage", path: "/min-bytes#min_bytes_for_full_part_storage", default: "0" },
        { name: "min_bytes_for_wide_part", path: "/min-bytes#min_bytes_for_wide_part", default: "10485760" },
        { name: "min_bytes_to_prewarm_caches", path: "/min-bytes#min_bytes_to_prewarm_caches", default: "0" },
        { name: "min_bytes_to_rebalance_partition_over_jbod", path: "/min-bytes#min_bytes_to_rebalance_partition_over_jbod", default: "0" }
      ],
      children: []
    },
    {
      label: "min_compressed_*",
      count: 2,
      settings: [
        { name: "min_compressed_bytes_to_fsync_after_fetch", path: "/min-compressed#min_compressed_bytes_to_fsync_after_fetch", default: "0" },
        { name: "min_compressed_bytes_to_fsync_after_merge", path: "/min-compressed#min_compressed_bytes_to_fsync_after_merge", default: "0" }
      ],
      children: []
    },
    {
      label: "min_delay_*",
      count: 2,
      settings: [
        { name: "min_delay_to_insert_ms", path: "/min-delay#min_delay_to_insert_ms", default: "10" },
        { name: "min_delay_to_mutate_ms", path: "/min-delay#min_delay_to_mutate_ms", default: "10" }
      ],
      children: []
    },
    {
      label: "min_free_*",
      count: 2,
      settings: [
        { name: "min_free_disk_bytes_to_perform_insert", path: "/min-free#min_free_disk_bytes_to_perform_insert", default: "0" },
        { name: "min_free_disk_ratio_to_perform_insert", path: "/min-free#min_free_disk_ratio_to_perform_insert", default: "0" }
      ],
      children: []
    },
    {
      label: "min_level_*",
      count: 2,
      settings: [
        { name: "min_level_for_full_part_storage", path: "/min-level#min_level_for_full_part_storage", default: "0" },
        { name: "min_level_for_wide_part", path: "/min-level#min_level_for_wide_part", default: "0" }
      ],
      children: []
    },
    {
      label: "min_relative_*",
      count: 3,
      settings: [
        { name: "min_relative_delay_to_close", path: "/min-relative#min_relative_delay_to_close", default: "300" },
        { name: "min_relative_delay_to_measure", path: "/min-relative#min_relative_delay_to_measure", default: "120" },
        { name: "min_relative_delay_to_yield_leadership", path: "/min-relative#min_relative_delay_to_yield_leadership", default: "120" }
      ],
      children: []
    },
    {
      label: "min_rows_*",
      count: 4,
      settings: [
        { name: "min_rows_for_compact_part", path: "/min-rows#min_rows_for_compact_part", default: "0" },
        { name: "min_rows_for_full_part_storage", path: "/min-rows#min_rows_for_full_part_storage", default: "0" },
        { name: "min_rows_for_wide_part", path: "/min-rows#min_rows_for_wide_part", default: "0" },
        { name: "min_rows_to_fsync_after_merge", path: "/min-rows#min_rows_to_fsync_after_merge", default: "0" }
      ],
      children: []
    },
    {
      label: "number_of_*",
      count: 6,
      settings: [
        { name: "number_of_free_entries_in_pool_to_execute_mutation", path: "/number-of#number_of_free_entries_in_pool_to_execute_mutation", default: "20" },
        { name: "number_of_free_entries_in_pool_to_execute_optimize_entire_partition", path: "/number-of#number_of_free_entries_in_pool_to_execute_optimize_entire_partition", default: "25" },
        { name: "number_of_free_entries_in_pool_to_lower_max_size_of_merge", path: "/number-of#number_of_free_entries_in_pool_to_lower_max_size_of_merge", default: "8" },
        { name: "number_of_mutations_to_delay", path: "/number-of#number_of_mutations_to_delay", default: "500" },
        { name: "number_of_mutations_to_throw", path: "/number-of#number_of_mutations_to_throw", default: "1000" },
        { name: "number_of_partitions_to_consider_for_merge", path: "/number-of#number_of_partitions_to_consider_for_merge", default: "10" }
      ],
      children: []
    },
    {
      label: "object_shared_*",
      count: 4,
      settings: [
        { name: "object_shared_data_buckets_for_compact_part", path: "/object-shared#object_shared_data_buckets_for_compact_part", default: "8" },
        { name: "object_shared_data_buckets_for_wide_part", path: "/object-shared#object_shared_data_buckets_for_wide_part", default: "32" },
        { name: "object_shared_data_serialization_version", path: "/object-shared#object_shared_data_serialization_version", default: "advanced" },
        { name: "object_shared_data_serialization_version_for_zero_level_parts", path: "/object-shared#object_shared_data_serialization_version_for_zero_level_parts", default: "map_with_buckets" }
      ],
      children: []
    },
    {
      label: "part_moves_*",
      count: 2,
      settings: [
        { name: "part_moves_between_shards_delay_seconds", path: "/part-moves#part_moves_between_shards_delay_seconds", default: "30" },
        { name: "part_moves_between_shards_enable", path: "/part-moves#part_moves_between_shards_enable", default: "0" }
      ],
      children: []
    },
    {
      label: "parts_to_*",
      count: 2,
      settings: [
        { name: "parts_to_delay_insert", path: "/parts-to#parts_to_delay_insert", default: "1000" },
        { name: "parts_to_throw_insert", path: "/parts-to#parts_to_throw_insert", default: "3000" }
      ],
      children: []
    },
    {
      label: "prefer_fetch_*",
      count: 2,
      settings: [
        { name: "prefer_fetch_merged_part_size_threshold", path: "/prefer-fetch#prefer_fetch_merged_part_size_threshold", default: "10737418240" },
        { name: "prefer_fetch_merged_part_time_threshold", path: "/prefer-fetch#prefer_fetch_merged_part_time_threshold", default: "3600" }
      ],
      children: []
    },
    {
      label: "prewarm_*",
      count: 2,
      settings: [
        { name: "prewarm_mark_cache", path: "/prewarm#prewarm_mark_cache", default: "0" },
        { name: "prewarm_primary_key_cache", path: "/prewarm#prewarm_primary_key_cache", default: "0" }
      ],
      children: []
    },
    {
      label: "primary_key_*",
      count: 4,
      settings: [
        { name: "primary_key_compress_block_size", path: "/primary-key#primary_key_compress_block_size", default: "65536" },
        { name: "primary_key_compression_codec", path: "/primary-key#primary_key_compression_codec", default: "ZSTD(3)" },
        { name: "primary_key_lazy_load", path: "/primary-key#primary_key_lazy_load", default: "1" },
        { name: "primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", path: "/primary-key#primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns", default: "0.9" }
      ],
      children: []
    },
    {
      label: "refresh_*",
      count: 2,
      settings: [
        { name: "refresh_parts_interval", path: "/refresh#refresh_parts_interval", default: "0" },
        { name: "refresh_statistics_interval", path: "/refresh#refresh_statistics_interval", default: "300" }
      ],
      children: []
    },
    {
      label: "remote_fs_*",
      count: 3,
      settings: [
        { name: "remote_fs_execute_merges_on_single_replica_time_threshold", path: "/remote-fs#remote_fs_execute_merges_on_single_replica_time_threshold", default: "10800" },
        { name: "remote_fs_zero_copy_path_compatible_mode", path: "/remote-fs#remote_fs_zero_copy_path_compatible_mode", default: "0" },
        { name: "remote_fs_zero_copy_zookeeper_path", path: "/remote-fs#remote_fs_zero_copy_zookeeper_path", default: "/clickhouse/zero_copy" }
      ],
      children: []
    },
    {
      label: "remove_*",
      count: 3,
      settings: [
        { name: "remove_empty_parts", path: "/remove#remove_empty_parts", default: "1" },
        { name: "remove_rolled_back_parts_immediately", path: "/remove#remove_rolled_back_parts_immediately", default: "1" },
        { name: "remove_unused_patch_parts", path: "/remove#remove_unused_patch_parts", default: "1" }
      ],
      children: []
    },
    {
      label: "replicated_deduplication_window_*",
      count: 4,
      settings: [
        { name: "replicated_deduplication_window", path: "/replicated-deduplication-window#replicated_deduplication_window", default: "10000" },
        { name: "replicated_deduplication_window_for_async_inserts", path: "/replicated-deduplication-window#replicated_deduplication_window_for_async_inserts", default: "10000" },
        { name: "replicated_deduplication_window_seconds", path: "/replicated-deduplication-window#replicated_deduplication_window_seconds", default: "3600" },
        { name: "replicated_deduplication_window_seconds_for_async_inserts", path: "/replicated-deduplication-window#replicated_deduplication_window_seconds_for_async_inserts", default: "604800" }
      ],
      children: []
    },
    {
      label: "replicated_fetches_*",
      count: 5,
      settings: [
        { name: "replicated_fetches_http_connection_timeout", path: "/replicated-fetches#replicated_fetches_http_connection_timeout", default: "0" },
        { name: "replicated_fetches_http_receive_timeout", path: "/replicated-fetches#replicated_fetches_http_receive_timeout", default: "0" },
        { name: "replicated_fetches_http_send_timeout", path: "/replicated-fetches#replicated_fetches_http_send_timeout", default: "0" },
        { name: "replicated_fetches_min_part_level", path: "/replicated-fetches#replicated_fetches_min_part_level", default: "0" },
        { name: "replicated_fetches_min_part_level_timeout_seconds", path: "/replicated-fetches#replicated_fetches_min_part_level_timeout_seconds", default: "300" }
      ],
      children: []
    },
    {
      label: "replicated_max_*",
      count: 7,
      settings: [
        { name: "replicated_max_mutations_in_one_entry", path: "/replicated-max#replicated_max_mutations_in_one_entry", default: "10000" },
        { name: "replicated_max_parallel_fetches", path: "/replicated-max#replicated_max_parallel_fetches", default: "0" },
        { name: "replicated_max_parallel_fetches_for_host", path: "/replicated-max#replicated_max_parallel_fetches_for_host", default: "15" },
        { name: "replicated_max_parallel_fetches_for_table", path: "/replicated-max#replicated_max_parallel_fetches_for_table", default: "0" },
        { name: "replicated_max_parallel_sends", path: "/replicated-max#replicated_max_parallel_sends", default: "0" },
        { name: "replicated_max_parallel_sends_for_table", path: "/replicated-max#replicated_max_parallel_sends_for_table", default: "0" },
        { name: "replicated_max_ratio_of_wrong_parts", path: "/replicated-max#replicated_max_ratio_of_wrong_parts", default: "0.5" }
      ],
      children: []
    },
    {
      label: "shared_merge_*",
      count: 53,
      settings: [
        { name: "shared_merge_tree_activate_coordinated_merges_tasks", path: "/shared-merge#shared_merge_tree_activate_coordinated_merges_tasks", default: "0" },
        { name: "shared_merge_tree_blobs_list_inline_file_max_bytes", path: "/shared-merge#shared_merge_tree_blobs_list_inline_file_max_bytes", default: "0" },
        { name: "shared_merge_tree_create_per_replica_metadata_nodes", path: "/shared-merge#shared_merge_tree_create_per_replica_metadata_nodes", default: "0" },
        { name: "shared_merge_tree_disable_merges_and_mutations_assignment", path: "/shared-merge#shared_merge_tree_disable_merges_and_mutations_assignment", default: "0" },
        { name: "shared_merge_tree_empty_partition_lifetime", path: "/shared-merge#shared_merge_tree_empty_partition_lifetime", default: "86400" },
        { name: "shared_merge_tree_enable_automatic_empty_partitions_cleanup", path: "/shared-merge#shared_merge_tree_enable_automatic_empty_partitions_cleanup", default: "1" },
        { name: "shared_merge_tree_enable_coordinated_merges", path: "/shared-merge#shared_merge_tree_enable_coordinated_merges", default: "1" },
        { name: "shared_merge_tree_enable_keeper_parts_extra_data", path: "/shared-merge#shared_merge_tree_enable_keeper_parts_extra_data", default: "1" },
        { name: "shared_merge_tree_enable_outdated_parts_check", path: "/shared-merge#shared_merge_tree_enable_outdated_parts_check", default: "1" },
        { name: "shared_merge_tree_idle_parts_update_seconds", path: "/shared-merge#shared_merge_tree_idle_parts_update_seconds", default: "3600" },
        { name: "shared_merge_tree_inactive_replica_cutoff_seconds", path: "/shared-merge#shared_merge_tree_inactive_replica_cutoff_seconds", default: "0" },
        { name: "shared_merge_tree_initial_parts_update_backoff_ms", path: "/shared-merge#shared_merge_tree_initial_parts_update_backoff_ms", default: "50" },
        { name: "shared_merge_tree_interserver_http_connection_timeout_ms", path: "/shared-merge#shared_merge_tree_interserver_http_connection_timeout_ms", default: "100" },
        { name: "shared_merge_tree_interserver_http_timeout_ms", path: "/shared-merge#shared_merge_tree_interserver_http_timeout_ms", default: "10000" },
        { name: "shared_merge_tree_leader_update_period_random_add_seconds", path: "/shared-merge#shared_merge_tree_leader_update_period_random_add_seconds", default: "10" },
        { name: "shared_merge_tree_leader_update_period_seconds", path: "/shared-merge#shared_merge_tree_leader_update_period_seconds", default: "30" },
        { name: "shared_merge_tree_max_outdated_parts_to_process_at_once", path: "/shared-merge#shared_merge_tree_max_outdated_parts_to_process_at_once", default: "1000" },
        { name: "shared_merge_tree_max_parts_update_backoff_ms", path: "/shared-merge#shared_merge_tree_max_parts_update_backoff_ms", default: "5000" },
        { name: "shared_merge_tree_max_parts_update_leaders_in_total", path: "/shared-merge#shared_merge_tree_max_parts_update_leaders_in_total", default: "6" },
        { name: "shared_merge_tree_max_parts_update_leaders_per_az", path: "/shared-merge#shared_merge_tree_max_parts_update_leaders_per_az", default: "2" },
        { name: "shared_merge_tree_max_replicas_for_parts_deletion", path: "/shared-merge#shared_merge_tree_max_replicas_for_parts_deletion", default: "10" },
        { name: "shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range", path: "/shared-merge#shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range", default: "5" },
        { name: "shared_merge_tree_max_suspicious_broken_parts", path: "/shared-merge#shared_merge_tree_max_suspicious_broken_parts", default: "0" },
        { name: "shared_merge_tree_max_suspicious_broken_parts_bytes", path: "/shared-merge#shared_merge_tree_max_suspicious_broken_parts_bytes", default: "0" },
        { name: "shared_merge_tree_memo_ids_remove_timeout_seconds", path: "/shared-merge#shared_merge_tree_memo_ids_remove_timeout_seconds", default: "1800" },
        { name: "shared_merge_tree_merge_coordinator_distribution_algorithm", path: "/shared-merge#shared_merge_tree_merge_coordinator_distribution_algorithm", default: "sainte_lague" },
        { name: "shared_merge_tree_merge_coordinator_election_check_period_ms", path: "/shared-merge#shared_merge_tree_merge_coordinator_election_check_period_ms", default: "30000" },
        { name: "shared_merge_tree_merge_coordinator_factor", path: "/shared-merge#shared_merge_tree_merge_coordinator_factor", default: "1.1" },
        { name: "shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms", path: "/shared-merge#shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms", default: "10000" },
        { name: "shared_merge_tree_merge_coordinator_max_merge_request_size", path: "/shared-merge#shared_merge_tree_merge_coordinator_max_merge_request_size", default: "20" },
        { name: "shared_merge_tree_merge_coordinator_max_period_ms", path: "/shared-merge#shared_merge_tree_merge_coordinator_max_period_ms", default: "10000" },
        { name: "shared_merge_tree_merge_coordinator_merges_prepare_count", path: "/shared-merge#shared_merge_tree_merge_coordinator_merges_prepare_count", default: "auto" },
        { name: "shared_merge_tree_merge_coordinator_min_period_ms", path: "/shared-merge#shared_merge_tree_merge_coordinator_min_period_ms", default: "1" },
        { name: "shared_merge_tree_merge_worker_fast_timeout_ms", path: "/shared-merge#shared_merge_tree_merge_worker_fast_timeout_ms", default: "100" },
        { name: "shared_merge_tree_merge_worker_regular_timeout_ms", path: "/shared-merge#shared_merge_tree_merge_worker_regular_timeout_ms", default: "10000" },
        { name: "shared_merge_tree_outdated_parts_group_size", path: "/shared-merge#shared_merge_tree_outdated_parts_group_size", default: "2" },
        {
          name: "shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations",
          path: "/shared-merge#shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations",
          default: "0.5"
        },
        { name: "shared_merge_tree_parts_load_batch_size", path: "/shared-merge#shared_merge_tree_parts_load_batch_size", default: "32" },
        { name: "shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms", path: "/shared-merge#shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms", default: "0" },
        {
          name: "shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold",
          path: "/shared-merge#shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold",
          default: "1000000"
        },
        { name: "shared_merge_tree_range_for_merge_window_size", path: "/shared-merge#shared_merge_tree_range_for_merge_window_size", default: "10" },
        { name: "shared_merge_tree_read_virtual_parts_from_leader", path: "/shared-merge#shared_merge_tree_read_virtual_parts_from_leader", default: "1" },
        { name: "shared_merge_tree_replica_set_max_lifetime_seconds", path: "/shared-merge#shared_merge_tree_replica_set_max_lifetime_seconds", default: "1800" },
        { name: "shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", path: "/shared-merge#shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", default: "0" },
        {
          name: "shared_merge_tree_try_fetch_part_in_memory_data_from_replicas_on_startup",
          path: "/shared-merge#shared_merge_tree_try_fetch_part_in_memory_data_from_replicas_on_startup",
          default: "0"
        },
        { name: "shared_merge_tree_update_replica_flags_delay_ms", path: "/shared-merge#shared_merge_tree_update_replica_flags_delay_ms", default: "30000" },
        { name: "shared_merge_tree_use_blobs_list_for_parts", path: "/shared-merge#shared_merge_tree_use_blobs_list_for_parts", default: "0" },
        { name: "shared_merge_tree_use_metadata_hints_cache", path: "/shared-merge#shared_merge_tree_use_metadata_hints_cache", default: "1" },
        { name: "shared_merge_tree_use_outdated_parts_compact_format", path: "/shared-merge#shared_merge_tree_use_outdated_parts_compact_format", default: "1" },
        { name: "shared_merge_tree_use_too_many_parts_count_from_virtual_parts", path: "/shared-merge#shared_merge_tree_use_too_many_parts_count_from_virtual_parts", default: "0" },
        { name: "shared_merge_tree_use_zookeeper_connection_pool", path: "/shared-merge#shared_merge_tree_use_zookeeper_connection_pool", default: "0" },
        { name: "shared_merge_tree_virtual_parts_discovery_batch", path: "/shared-merge#shared_merge_tree_virtual_parts_discovery_batch", default: "1" },
        { name: "shared_merge_tree_virtual_parts_partition_atomic_discovery", path: "/shared-merge#shared_merge_tree_virtual_parts_partition_atomic_discovery", default: "1" }
      ],
      children: []
    },
    {
      label: "sleep_before_*",
      count: 2,
      settings: [
        { name: "sleep_before_commit_local_part_in_replicated_table_ms", path: "/sleep-before#sleep_before_commit_local_part_in_replicated_table_ms", default: "0" },
        { name: "sleep_before_loading_outdated_parts_ms", path: "/sleep-before#sleep_before_loading_outdated_parts_ms", default: "0" }
      ],
      children: []
    },
    {
      label: "table_*",
      count: 2,
      settings: [
        { name: "table_disk", path: "/table#table_disk", default: "0" },
        { name: "table_readonly", path: "/table#table_readonly", default: "0" }
      ],
      children: []
    },
    {
      label: "text_index_*",
      count: 7,
      settings: [
        { name: "text_index_dictionary_block_frontcoding_compression", path: "/text-index#text_index_dictionary_block_frontcoding_compression", default: "1" },
        { name: "text_index_dictionary_block_size", path: "/text-index#text_index_dictionary_block_size", default: "512" },
        { name: "text_index_max_memory_usage_before_flush", path: "/text-index#text_index_max_memory_usage_before_flush", default: "1073741824" },
        { name: "text_index_max_processed_tokens_before_flush", path: "/text-index#text_index_max_processed_tokens_before_flush", default: "100000000" },
        { name: "text_index_posting_list_block_size", path: "/text-index#text_index_posting_list_block_size", default: "1048576" },
        { name: "text_index_posting_list_codec", path: "/text-index#text_index_posting_list_codec", default: "none" },
        { name: "text_index_serialization_version", path: "/text-index#text_index_serialization_version", default: "v2_with_positions" }
      ],
      children: []
    },
    {
      label: "use_*",
      count: 6,
      settings: [
        { name: "use_adaptive_write_buffer_for_dynamic_subcolumns", path: "/use#use_adaptive_write_buffer_for_dynamic_subcolumns", default: "1" },
        { name: "use_async_block_ids_cache", path: "/use#use_async_block_ids_cache", default: "1" },
        { name: "use_compact_variant_discriminators_serialization", path: "/use#use_compact_variant_discriminators_serialization", default: "1" },
        { name: "use_const_adaptive_granularity", path: "/use#use_const_adaptive_granularity", default: "0" },
        { name: "use_metadata_cache", path: "/use#use_metadata_cache", default: "0" },
        { name: "use_primary_key_cache", path: "/use#use_primary_key_cache", default: "0" }
      ],
      children: []
    },
    {
      label: "use_minimalistic_*",
      count: 2,
      settings: [
        { name: "use_minimalistic_checksums_in_zookeeper", path: "/use-minimalistic#use_minimalistic_checksums_in_zookeeper", default: "1" },
        { name: "use_minimalistic_part_header_in_zookeeper", path: "/use-minimalistic#use_minimalistic_part_header_in_zookeeper", default: "1" }
      ],
      children: []
    },
    {
      label: "vertical_merge_*",
      count: 6,
      settings: [
        { name: "vertical_merge_algorithm_min_bytes_to_activate", path: "/vertical-merge#vertical_merge_algorithm_min_bytes_to_activate", default: "0" },
        { name: "vertical_merge_algorithm_min_columns_to_activate", path: "/vertical-merge#vertical_merge_algorithm_min_columns_to_activate", default: "11" },
        { name: "vertical_merge_algorithm_min_rows_to_activate", path: "/vertical-merge#vertical_merge_algorithm_min_rows_to_activate", default: "131072" },
        { name: "vertical_merge_optimize_lightweight_delete", path: "/vertical-merge#vertical_merge_optimize_lightweight_delete", default: "1" },
        { name: "vertical_merge_optimize_ttl_delete", path: "/vertical-merge#vertical_merge_optimize_ttl_delete", default: "1" },
        { name: "vertical_merge_remote_filesystem_prefetch", path: "/vertical-merge#vertical_merge_remote_filesystem_prefetch", default: "1" }
      ],
      children: []
    },
    {
      label: "write_*",
      count: 2,
      settings: [
        { name: "write_final_mark", path: "/write#write_final_mark", default: "1" },
        { name: "write_marks_for_substreams_in_compact_parts", path: "/write#write_marks_for_substreams_in_compact_parts", default: "1" }
      ],
      children: []
    },
    {
      label: "write_ahead_*",
      count: 3,
      settings: [
        { name: "write_ahead_log_bytes_to_fsync", path: "/write-ahead#write_ahead_log_bytes_to_fsync", default: "104857600" },
        { name: "write_ahead_log_interval_ms_to_fsync", path: "/write-ahead#write_ahead_log_interval_ms_to_fsync", default: "100" },
        { name: "write_ahead_log_max_bytes", path: "/write-ahead#write_ahead_log_max_bytes", default: "1073741824" }
      ],
      children: []
    },
    {
      label: "zero_copy_*",
      count: 4,
      settings: [
        { name: "zero_copy_concurrent_part_removal_max_postpone_ratio", path: "/zero-copy#zero_copy_concurrent_part_removal_max_postpone_ratio", default: "0.05" },
        { name: "zero_copy_concurrent_part_removal_max_split_times", path: "/zero-copy#zero_copy_concurrent_part_removal_max_split_times", default: "5" },
        { name: "zero_copy_merge_mutation_min_parts_size_sleep_before_lock", path: "/zero-copy#zero_copy_merge_mutation_min_parts_size_sleep_before_lock", default: "1073741824" },
        { name: "zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock", path: "/zero-copy#zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock", default: "0" }
      ],
      children: []
    },
    {
      label: "Outros",
      count: 52,
      settings: [
        { name: "adaptive_write_buffer_initial_size", path: "/other#adaptive_write_buffer_initial_size", default: "16384" },
        { name: "add_implicit_sign_column_constraint_for_collapsing_engine", path: "/other#add_implicit_sign_column_constraint_for_collapsing_engine", default: "0" },
        { name: "alter_column_secondary_index_mode", path: "/other#alter_column_secondary_index_mode", default: "rebuild" },
        { name: "always_use_copy_instead_of_hardlinks", path: "/other#always_use_copy_instead_of_hardlinks", default: "0" },
        { name: "apply_patches_on_merge", path: "/other#apply_patches_on_merge", default: "1" },
        { name: "assign_part_uuids", path: "/other#assign_part_uuids", default: "0" },
        { name: "auto_statistics_types", path: "/other#auto_statistics_types", default: "basic, uniq_v2" },
        { name: "background_task_preferred_step_execution_time_ms", path: "/other#background_task_preferred_step_execution_time_ms", default: "50" },
        { name: "clean_deleted_rows", path: "/other#clean_deleted_rows", default: "Never" },
        { name: "clone_replica_zookeeper_create_get_part_batch_size", path: "/other#clone_replica_zookeeper_create_get_part_batch_size", default: "100" },
        { name: "compatibility_allow_sampling_expression_not_in_primary_key", path: "/other#compatibility_allow_sampling_expression_not_in_primary_key", default: "0" },
        { name: "compute_exact_num_defaults_for_sparse_columns", path: "/other#compute_exact_num_defaults_for_sparse_columns", default: "1" },
        { name: "deduplicate_merge_projection_mode", path: "/other#deduplicate_merge_projection_mode", default: "throw" },
        { name: "deduplication_hashes_cache_update_wait_ms", path: "/other#deduplication_hashes_cache_update_wait_ms", default: "100" },
        { name: "default_compression_codec", path: "/other#default_compression_codec", default: '""' },
        { name: "disk", path: "/other#disk", default: '""' },
        { name: "dynamic_serialization_version", path: "/other#dynamic_serialization_version", default: "v3" },
        { name: "enforce_index_structure_match_on_partition_manipulation", path: "/other#enforce_index_structure_match_on_partition_manipulation", default: "0" },
        { name: "execute_merges_on_single_replica_time_threshold", path: "/other#execute_merges_on_single_replica_time_threshold", default: "0" },
        { name: "finished_mutations_to_keep", path: "/other#finished_mutations_to_keep", default: "100" },
        { name: "force_read_through_cache_for_merges", path: "/other#force_read_through_cache_for_merges", default: "0" },
        { name: "initialization_retry_period", path: "/other#initialization_retry_period", default: "60" },
        { name: "kill_threads", path: "/other#kill_threads", default: "128" },
        { name: "lightweight_mutation_projection_mode", path: "/other#lightweight_mutation_projection_mode", default: "throw" },
        { name: "load_existing_rows_count_for_old_parts", path: "/other#load_existing_rows_count_for_old_parts", default: "0" },
        { name: "lock_acquire_timeout_for_background_operations", path: "/other#lock_acquire_timeout_for_background_operations", default: "120" },
        { name: "mutation_workload", path: "/other#mutation_workload", default: '""' },
        { name: "non_replicated_deduplication_window", path: "/other#non_replicated_deduplication_window", default: "0" },
        { name: "notify_newest_block_number", path: "/other#notify_newest_block_number", default: "0" },
        { name: "nullable_serialization_version", path: "/other#nullable_serialization_version", default: "basic" },
        { name: "object_serialization_version", path: "/other#object_serialization_version", default: "v3" },
        { name: "old_parts_lifetime", path: "/other#old_parts_lifetime", default: "480" },
        { name: "optimize_row_order", path: "/other#optimize_row_order", default: "0" },
        { name: "packed_skip_index_max_bytes", path: "/other#packed_skip_index_max_bytes", default: "1048576" },
        { name: "part_minmax_index_columns", path: "/other#part_minmax_index_columns", default: "partition_key_only" },
        { name: "patch_parts_version", path: "/other#patch_parts_version", default: "v2" },
        { name: "propagate_types_serialization_versions_to_nested_types", path: "/other#propagate_types_serialization_versions_to_nested_types", default: "1" },
        { name: "ratio_of_defaults_for_sparse_serialization", path: "/other#ratio_of_defaults_for_sparse_serialization", default: "0.9375" },
        { name: "reduce_blocking_parts_sleep_ms", path: "/other#reduce_blocking_parts_sleep_ms", default: "5000" },
        { name: "replace_long_file_name_to_hash", path: "/other#replace_long_file_name_to_hash", default: "1" },
        { name: "replicated_can_become_leader", path: "/other#replicated_can_become_leader", default: "1" },
        { name: "search_orphaned_parts_disks", path: "/other#search_orphaned_parts_disks", default: "any" },
        { name: "serialization_info_version", path: "/other#serialization_info_version", default: "with_types" },
        { name: "share_nested_offsets", path: "/other#share_nested_offsets", default: "1" },
        { name: "simultaneous_parts_removal_limit", path: "/other#simultaneous_parts_removal_limit", default: "0" },
        { name: "storage_policy", path: "/other#storage_policy", default: "default" },
        { name: "string_serialization_version", path: "/other#string_serialization_version", default: "with_size_stream" },
        { name: "temporary_directories_lifetime", path: "/other#temporary_directories_lifetime", default: "86400" },
        { name: "try_fetch_recompressed_part_timeout", path: "/other#try_fetch_recompressed_part_timeout", default: "7200" },
        { name: "ttl_only_drop_parts", path: "/other#ttl_only_drop_parts", default: "0" },
        { name: "wait_for_unique_parts_send_before_shutdown_ms", path: "/other#wait_for_unique_parts_send_before_shutdown_ms", default: "0" },
        { name: "zookeeper_session_expiration_check_period", path: "/other#zookeeper_session_expiration_check_period", default: "60" }
      ],
      children: []
    }
  ])
  const [allGroupKeys] = useState(() => {
    const collectGroupKeys = (items, path = []) =>
      items.flatMap((entry) => {
        const key = [...path, entry.label].join("/")
        return [key, ...collectGroupKeys(entry.children, [...path, entry.label])]
      })
    return collectGroupKeys(entries)
  })
  const [expandedGroups, setExpandedGroups] = useState(() => new Set())
  const [searchTerm, setSearchTerm] = useState("")
  const normalizedSearch = searchTerm.trim().toLowerCase()
  const toPlainSearchTerms = (value) =>
    value
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((term) => term.length > 1)
      .map((term) => (term.length > 3 && term.endsWith("s") ? term.slice(0, -1) : term))
  const usesWildcard = normalizedSearch.includes("%")
  const plainSearchTerms = toPlainSearchTerms(searchTerm)
  const isSearching = usesWildcard ? normalizedSearch.replaceAll("%", "").trim().length > 0 : plainSearchTerms.length > 0

  const matchesSearch = (value) => {
    const candidate = value.toLowerCase()
    if (!isSearching) return true
    if (!usesWildcard) {
      const candidateTerms = toPlainSearchTerms(value)
      return plainSearchTerms.every((searchTerm) => candidateTerms.some((candidateTerm) => candidateTerm.startsWith(searchTerm)))
    }

    const parts = normalizedSearch.split("%")
    let position = 0
    for (let index = 0; index < parts.length; index += 1) {
      const part = parts[index]
      if (!part) continue
      const matchPosition = candidate.indexOf(part, position)
      if (matchPosition < 0) return false
      if (index === 0 && !normalizedSearch.startsWith("%") && matchPosition !== 0) {
        return false
      }
      position = matchPosition + part.length
    }

    const lastPart = parts[parts.length - 1]
    return normalizedSearch.endsWith("%") || !lastPart || position === candidate.length
  }

  const filterEntry = (entry) => {
    const settings = entry.settings.filter((setting) => matchesSearch(setting.name))
    const children = entry.children.map(filterEntry).filter(Boolean)
    const count = settings.length + children.reduce((total, child) => total + child.count, 0)
    if (!count) return null
    return { ...entry, count, settings, children }
  }

  const filteredEntries = isSearching ? entries.map(filterEntry).filter(Boolean) : entries
  const matchingCount = filteredEntries.reduce((total, entry) => total + entry.count, 0)
  const allGroupsExpanded = allGroupKeys.length > 0 && allGroupKeys.every((key) => expandedGroups.has(key))

  const toggleGroup = (key) => {
    setExpandedGroups((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const toggleAllGroups = () => {
    setExpandedGroups((current) => {
      const shouldCollapse = allGroupKeys.every((key) => current.has(key))
      return shouldCollapse ? new Set() : new Set(allGroupKeys)
    })
  }

  const branchPrefix = (continuations, isLast) => {
    const prefix = continuations.map((continued) => (continued ? "│  " : "   ")).join("")
    return `${prefix}${isLast ? "└─ " : "├─ "}`
  }

  const branch = (value) => (
    <span aria-hidden="true" className="shrink-0 select-none text-gray-400 dark:text-gray-600" style={{ whiteSpace: "pre" }}>
      {value}
    </span>
  )

  const renderGroup = (entry, continuations = [], isLast = false, path = []) => {
    const key = [...path, entry.label].join("/")
    const isOpen = isSearching || expandedGroups.has(key)
    const items = [...entry.settings.map((setting) => ({ type: "setting", value: setting })), ...entry.children.map((child) => ({ type: "group", value: child }))]
    const countLabel = `${entry.count} ${entry.count === 1 ? "configuração" : "configurações"}`

    return (
      <div key={key} className="min-w-max">
        <button
          type="button"
          aria-expanded={isOpen}
          disabled={isSearching}
          onClick={() => toggleGroup(key)}
          className="flex min-w-max items-baseline whitespace-nowrap text-left"
          style={{
            appearance: "none",
            background: "transparent",
            border: 0,
            color: "inherit",
            cursor: isSearching ? "default" : "pointer",
            font: "inherit",
            lineHeight: "inherit",
            padding: 0
          }}
        >
          <span aria-hidden="true" style={{ display: "inline-block", width: "1rem" }}>
            {isOpen ? "▾" : "▸"}
          </span>
          {branch(branchPrefix(continuations, isLast))}
          <span className="font-medium">{entry.label}</span>
          <span className="ml-3 text-xs text-gray-500 dark:text-gray-400">{countLabel}</span>
        </button>
        {isOpen &&
          items.map((item, index) => {
            const itemIsLast = index === items.length - 1
            const childContinuations = [...continuations, !isLast]
            if (item.type === "group") {
              return renderGroup(item.value, childContinuations, itemIsLast, [...path, entry.label])
            }
            return (
              <div key={item.value.name} className="grid min-w-max items-start gap-x-3 whitespace-nowrap" style={{ gridTemplateColumns: "44ch max-content" }}>
                <span className="flex min-w-0 items-start">
                  <span aria-hidden="true" className="w-4 shrink-0" />
                  {branch(branchPrefix(childContinuations, itemIsLast))}
                  <a href={`https://clickhouse.com/docs${baseRoute}${item.value.path}`} className="min-w-0 whitespace-normal no-underline hover:underline" style={{ overflowWrap: "anywhere" }}>
                    {item.value.name.split("_").map((part, index, parts) => (
                      <span key={`${part}-${index}`}>
                        {part}
                        {index < parts.length - 1 ? "_" : ""}
                        {index < parts.length - 1 && <wbr />}
                      </span>
                    ))}
                  </a>
                </span>
                {item.value.default !== undefined && (
                  <span title="Valor padrão" className="whitespace-nowrap text-gray-500 dark:text-gray-400">
                    (padrão: {item.value.default})
                  </span>
                )}
              </div>
            )
          })}
      </div>
    )
  }

  return (
    <div className="not-prose my-6 w-full">
      <div className="relative w-full">
        <svg
          aria-hidden="true"
          viewBox="0 0 24 24"
          fill="none"
          stroke="currentColor"
          strokeWidth="2"
          strokeLinecap="round"
          strokeLinejoin="round"
          className="pointer-events-none absolute left-3 h-4 w-4 text-gray-500 dark:text-gray-400"
          style={{ top: "50%", transform: "translateY(-50%)" }}
        >
          <circle cx="11" cy="11" r="8" />
          <path d="m21 21-4.3-4.3" />
        </svg>
        <input
          aria-label="Buscar configurações"
          type="search"
          value={searchTerm}
          onChange={(event) => setSearchTerm(event.target.value)}
          placeholder="Buscar configurações, ex.: parallel replicas ou %materialized%"
          className="w-full rounded-lg border border-gray-500 bg-gray-50 py-2 pl-9 pr-3 text-sm text-gray-900 placeholder:text-gray-600 focus:border-gray-600 focus:outline-0 focus-visible:outline-0 dark:border-white/30 dark:bg-white/5 dark:text-white dark:placeholder:text-gray-400 dark:focus:border-[#fdff75]"
        />
      </div>
      {isSearching && (
        <div className="mt-2 text-right text-xs text-gray-500 dark:text-gray-400">
          <span>
            {matchingCount} {matchingCount === 1 ? "configuração encontrada" : "configurações encontradas"}
          </span>
        </div>
      )}
      <div className="mt-3 w-full overflow-x-auto rounded-xl border border-gray-200 bg-gray-50/50 px-4 py-3 font-mono text-sm leading-6 dark:border-white/10 dark:bg-transparent">
        <div className="flex min-w-full items-center justify-between gap-4">
          <div className="min-w-max font-semibold">/merge-tree-settings</div>
          <button
            type="button"
            aria-label={allGroupsExpanded ? "Recolher tudo" : "Expandir tudo"}
            aria-pressed={allGroupsExpanded}
            disabled={isSearching}
            onClick={toggleAllGroups}
            className="inline-flex shrink-0 items-center gap-1 whitespace-nowrap rounded border-0 bg-transparent px-1 py-0.5 font-sans text-xs font-medium text-gray-600 hover:text-gray-900 focus:outline-0 focus-visible:text-gray-900 disabled:cursor-not-allowed disabled:opacity-50 dark:text-gray-400 dark:hover:text-[#fdff75] dark:focus-visible:text-[#fdff75]"
          >
            <svg aria-hidden="true" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="h-3 w-3">
              {allGroupsExpanded ? <path d="m6 9 6 6 6-6" /> : <path d="m9 18 6-6-6-6" />}
            </svg>
            <span>{allGroupsExpanded ? "Recolher tudo" : "Expandir tudo"}</span>
          </button>
        </div>
        {filteredEntries.length > 0 ? (
          filteredEntries.map((entry, index) => renderGroup(entry, [], index === filteredEntries.length - 1))
        ) : (
          <div className="py-2 text-gray-500 dark:text-gray-400">Nenhuma configuração encontrada</div>
        )}
      </div>
    </div>
  )
}

export default MergeTreeSettingsExplorer;