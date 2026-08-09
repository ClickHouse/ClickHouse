const MergeTreeSettingsExplorer = () => {
  // Mintlify's production renderer evaluates the exported component without
  // preserving module-scope bindings. Lazy state keeps the generated data in
  // that evaluation scope while constructing it only once per mount.
  const [entries] = useState(() => [
    {
      label: "add_minmax_*",
      count: 5,
      settings: [
        { name: "add_minmax_index_for_block_number_column", href: "/ru/reference/settings/merge-tree-settings/add-minmax#add_minmax_index_for_block_number_column" },
        { name: "add_minmax_index_for_block_offset_column", href: "/ru/reference/settings/merge-tree-settings/add-minmax#add_minmax_index_for_block_offset_column" },
        { name: "add_minmax_index_for_numeric_columns", href: "/ru/reference/settings/merge-tree-settings/add-minmax#add_minmax_index_for_numeric_columns" },
        { name: "add_minmax_index_for_string_columns", href: "/ru/reference/settings/merge-tree-settings/add-minmax#add_minmax_index_for_string_columns" },
        { name: "add_minmax_index_for_temporal_columns", href: "/ru/reference/settings/merge-tree-settings/add-minmax#add_minmax_index_for_temporal_columns" }
      ],
      children: []
    },
    {
      label: "allow_*",
      count: 13,
      settings: [
        { name: "allow_coalescing_columns_in_partition_or_order_key", href: "/ru/reference/settings/merge-tree-settings/allow#allow_coalescing_columns_in_partition_or_order_key" },
        { name: "allow_commit_order_projection", href: "/ru/reference/settings/merge-tree-settings/allow#allow_commit_order_projection" },
        { name: "allow_dimensions_outside_sorting_key", href: "/ru/reference/settings/merge-tree-settings/allow#allow_dimensions_outside_sorting_key" },
        { name: "allow_floating_point_partition_key", href: "/ru/reference/settings/merge-tree-settings/allow#allow_floating_point_partition_key" },
        { name: "allow_minmax_index_for_json", href: "/ru/reference/settings/merge-tree-settings/allow#allow_minmax_index_for_json" },
        { name: "allow_nullable_key", href: "/ru/reference/settings/merge-tree-settings/allow#allow_nullable_key" },
        { name: "allow_part_offset_column_in_projections", href: "/ru/reference/settings/merge-tree-settings/allow#allow_part_offset_column_in_projections" },
        { name: "allow_reduce_blocking_parts_task", href: "/ru/reference/settings/merge-tree-settings/allow#allow_reduce_blocking_parts_task" },
        { name: "allow_remote_fs_zero_copy_replication", href: "/ru/reference/settings/merge-tree-settings/allow#allow_remote_fs_zero_copy_replication" },
        { name: "allow_summing_columns_in_partition_or_order_key", href: "/ru/reference/settings/merge-tree-settings/allow#allow_summing_columns_in_partition_or_order_key" },
        { name: "allow_suspicious_indices", href: "/ru/reference/settings/merge-tree-settings/allow#allow_suspicious_indices" },
        { name: "allow_tuple_element_aggregation", href: "/ru/reference/settings/merge-tree-settings/allow#allow_tuple_element_aggregation" },
        { name: "allow_vertical_merges_from_compact_to_wide_parts", href: "/ru/reference/settings/merge-tree-settings/allow#allow_vertical_merges_from_compact_to_wide_parts" }
      ],
      children: []
    },
    {
      label: "allow_experimental_*",
      count: 3,
      settings: [
        { name: "allow_experimental_replacing_merge_with_cleanup", href: "/ru/reference/settings/merge-tree-settings/allow-experimental#allow_experimental_replacing_merge_with_cleanup" },
        { name: "allow_experimental_reverse_key", href: "/ru/reference/settings/merge-tree-settings/allow-experimental#allow_experimental_reverse_key" },
        { name: "allow_experimental_text_index_phrase_search", href: "/ru/reference/settings/merge-tree-settings/allow-experimental#allow_experimental_text_index_phrase_search" }
      ],
      children: []
    },
    {
      label: "always_*",
      count: 2,
      settings: [
        { name: "always_fetch_merged_part", href: "/ru/reference/settings/merge-tree-settings/always#always_fetch_merged_part" },
        { name: "always_use_copy_instead_of_hardlinks", href: "/ru/reference/settings/merge-tree-settings/always#always_use_copy_instead_of_hardlinks" }
      ],
      children: []
    },
    {
      label: "async_*",
      count: 2,
      settings: [
        { name: "async_block_ids_cache_update_wait_ms", href: "/ru/reference/settings/merge-tree-settings/async#async_block_ids_cache_update_wait_ms" },
        { name: "async_insert", href: "/ru/reference/settings/merge-tree-settings/async#async_insert" }
      ],
      children: []
    },
    {
      label: "cache_populated_by_fetch_*",
      count: 2,
      settings: [
        { name: "cache_populated_by_fetch", href: "/ru/reference/settings/merge-tree-settings/cache-populated-by-fetch#cache_populated_by_fetch" },
        { name: "cache_populated_by_fetch_filename_regexp", href: "/ru/reference/settings/merge-tree-settings/cache-populated-by-fetch#cache_populated_by_fetch_filename_regexp" }
      ],
      children: []
    },
    {
      label: "check_*",
      count: 2,
      settings: [
        { name: "check_delay_period", href: "/ru/reference/settings/merge-tree-settings/check#check_delay_period" },
        { name: "check_sample_column_is_correct", href: "/ru/reference/settings/merge-tree-settings/check#check_sample_column_is_correct" }
      ],
      children: []
    },
    {
      label: "cleanup_*",
      count: 2,
      settings: [
        { name: "cleanup_thread_preferred_points_per_iteration", href: "/ru/reference/settings/merge-tree-settings/cleanup#cleanup_thread_preferred_points_per_iteration" },
        { name: "cleanup_threads", href: "/ru/reference/settings/merge-tree-settings/cleanup#cleanup_threads" }
      ],
      children: []
    },
    {
      label: "cleanup_delay_period_*",
      count: 2,
      settings: [
        { name: "cleanup_delay_period", href: "/ru/reference/settings/merge-tree-settings/cleanup-delay-period#cleanup_delay_period" },
        { name: "cleanup_delay_period_random_add", href: "/ru/reference/settings/merge-tree-settings/cleanup-delay-period#cleanup_delay_period_random_add" }
      ],
      children: []
    },
    {
      label: "columns_*",
      count: 2,
      settings: [
        { name: "columns_and_secondary_indices_sizes_lazy_calculation", href: "/ru/reference/settings/merge-tree-settings/columns#columns_and_secondary_indices_sizes_lazy_calculation" },
        { name: "columns_to_prewarm_mark_cache", href: "/ru/reference/settings/merge-tree-settings/columns#columns_to_prewarm_mark_cache" }
      ],
      children: []
    },
    {
      label: "compact_parts_*",
      count: 3,
      settings: [
        { name: "compact_parts_max_bytes_to_buffer", href: "/ru/reference/settings/merge-tree-settings/compact-parts#compact_parts_max_bytes_to_buffer" },
        { name: "compact_parts_max_granules_to_buffer", href: "/ru/reference/settings/merge-tree-settings/compact-parts#compact_parts_max_granules_to_buffer" },
        { name: "compact_parts_merge_max_bytes_to_prefetch_part", href: "/ru/reference/settings/merge-tree-settings/compact-parts#compact_parts_merge_max_bytes_to_prefetch_part" }
      ],
      children: []
    },
    {
      label: "compress_*",
      count: 3,
      settings: [
        { name: "compress_marks", href: "/ru/reference/settings/merge-tree-settings/compress#compress_marks" },
        { name: "compress_per_column_in_compact_parts", href: "/ru/reference/settings/merge-tree-settings/compress#compress_per_column_in_compact_parts" },
        { name: "compress_primary_key", href: "/ru/reference/settings/merge-tree-settings/compress#compress_primary_key" }
      ],
      children: []
    },
    {
      label: "concurrent_part_removal_threshold_*",
      count: 2,
      settings: [
        { name: "concurrent_part_removal_threshold", href: "/ru/reference/settings/merge-tree-settings/concurrent-part-removal-threshold#concurrent_part_removal_threshold" },
        {
          name: "concurrent_part_removal_threshold_for_remote_disk",
          href: "/ru/reference/settings/merge-tree-settings/concurrent-part-removal-threshold#concurrent_part_removal_threshold_for_remote_disk"
        }
      ],
      children: []
    },
    {
      label: "dead_blobs_*",
      count: 2,
      settings: [
        { name: "dead_blobs_to_delay_insert", href: "/ru/reference/settings/merge-tree-settings/dead-blobs#dead_blobs_to_delay_insert" },
        { name: "dead_blobs_to_throw_insert", href: "/ru/reference/settings/merge-tree-settings/dead-blobs#dead_blobs_to_throw_insert" }
      ],
      children: []
    },
    {
      label: "detach_*",
      count: 2,
      settings: [
        { name: "detach_not_byte_identical_parts", href: "/ru/reference/settings/merge-tree-settings/detach#detach_not_byte_identical_parts" },
        { name: "detach_old_local_parts_when_cloning_replica", href: "/ru/reference/settings/merge-tree-settings/detach#detach_old_local_parts_when_cloning_replica" }
      ],
      children: []
    },
    {
      label: "disable_*",
      count: 3,
      settings: [
        { name: "disable_detach_partition_for_zero_copy_replication", href: "/ru/reference/settings/merge-tree-settings/disable#disable_detach_partition_for_zero_copy_replication" },
        { name: "disable_fetch_partition_for_zero_copy_replication", href: "/ru/reference/settings/merge-tree-settings/disable#disable_fetch_partition_for_zero_copy_replication" },
        { name: "disable_freeze_partition_for_zero_copy_replication", href: "/ru/reference/settings/merge-tree-settings/disable#disable_freeze_partition_for_zero_copy_replication" }
      ],
      children: []
    },
    {
      label: "distributed_index_*",
      count: 2,
      settings: [
        {
          name: "distributed_index_analysis_min_indexes_bytes_to_activate",
          href: "/ru/reference/settings/merge-tree-settings/distributed-index#distributed_index_analysis_min_indexes_bytes_to_activate"
        },
        { name: "distributed_index_analysis_min_parts_to_activate", href: "/ru/reference/settings/merge-tree-settings/distributed-index#distributed_index_analysis_min_parts_to_activate" }
      ],
      children: []
    },
    {
      label: "enable_*",
      count: 6,
      settings: [
        { name: "enable_index_granularity_compression", href: "/ru/reference/settings/merge-tree-settings/enable#enable_index_granularity_compression" },
        { name: "enable_max_bytes_limit_for_min_age_to_force_merge", href: "/ru/reference/settings/merge-tree-settings/enable#enable_max_bytes_limit_for_min_age_to_force_merge" },
        { name: "enable_mixed_granularity_parts", href: "/ru/reference/settings/merge-tree-settings/enable#enable_mixed_granularity_parts" },
        {
          name: "enable_replacing_merge_with_cleanup_for_min_age_to_force_merge",
          href: "/ru/reference/settings/merge-tree-settings/enable#enable_replacing_merge_with_cleanup_for_min_age_to_force_merge"
        },
        { name: "enable_the_endpoint_id_with_zookeeper_name_prefix", href: "/ru/reference/settings/merge-tree-settings/enable#enable_the_endpoint_id_with_zookeeper_name_prefix" },
        { name: "enable_vertical_merge_algorithm", href: "/ru/reference/settings/merge-tree-settings/enable#enable_vertical_merge_algorithm" }
      ],
      children: []
    },
    {
      label: "enable_block_*",
      count: 2,
      settings: [
        { name: "enable_block_number_column", href: "/ru/reference/settings/merge-tree-settings/enable-block#enable_block_number_column" },
        { name: "enable_block_offset_column", href: "/ru/reference/settings/merge-tree-settings/enable-block#enable_block_offset_column" }
      ],
      children: []
    },
    {
      label: "escape_*",
      count: 2,
      settings: [
        { name: "escape_index_filenames", href: "/ru/reference/settings/merge-tree-settings/escape#escape_index_filenames" },
        { name: "escape_variant_subcolumn_filenames", href: "/ru/reference/settings/merge-tree-settings/escape#escape_variant_subcolumn_filenames" }
      ],
      children: []
    },
    {
      label: "exclude_*",
      count: 2,
      settings: [
        { name: "exclude_deleted_rows_for_part_size_in_merge", href: "/ru/reference/settings/merge-tree-settings/exclude#exclude_deleted_rows_for_part_size_in_merge" },
        { name: "exclude_materialize_skip_indexes_on_merge", href: "/ru/reference/settings/merge-tree-settings/exclude#exclude_materialize_skip_indexes_on_merge" }
      ],
      children: []
    },
    {
      label: "fault_probability_*",
      count: 2,
      settings: [
        { name: "fault_probability_after_part_commit", href: "/ru/reference/settings/merge-tree-settings/fault-probability#fault_probability_after_part_commit" },
        { name: "fault_probability_before_part_commit", href: "/ru/reference/settings/merge-tree-settings/fault-probability#fault_probability_before_part_commit" }
      ],
      children: []
    },
    {
      label: "fsync_*",
      count: 2,
      settings: [
        { name: "fsync_after_insert", href: "/ru/reference/settings/merge-tree-settings/fsync#fsync_after_insert" },
        { name: "fsync_part_directory", href: "/ru/reference/settings/merge-tree-settings/fsync#fsync_part_directory" }
      ],
      children: []
    },
    {
      label: "in_memory_*",
      count: 2,
      settings: [
        { name: "in_memory_parts_enable_wal", href: "/ru/reference/settings/merge-tree-settings/in-memory#in_memory_parts_enable_wal" },
        { name: "in_memory_parts_insert_sync", href: "/ru/reference/settings/merge-tree-settings/in-memory#in_memory_parts_insert_sync" }
      ],
      children: []
    },
    {
      label: "inactive_parts_*",
      count: 2,
      settings: [
        { name: "inactive_parts_to_delay_insert", href: "/ru/reference/settings/merge-tree-settings/inactive-parts#inactive_parts_to_delay_insert" },
        { name: "inactive_parts_to_throw_insert", href: "/ru/reference/settings/merge-tree-settings/inactive-parts#inactive_parts_to_throw_insert" }
      ],
      children: []
    },
    {
      label: "index_granularity_*",
      count: 2,
      settings: [
        { name: "index_granularity", href: "/ru/reference/settings/merge-tree-settings/index-granularity#index_granularity" },
        { name: "index_granularity_bytes", href: "/ru/reference/settings/merge-tree-settings/index-granularity#index_granularity_bytes" }
      ],
      children: []
    },
    {
      label: "kill_delay_period_*",
      count: 2,
      settings: [
        { name: "kill_delay_period", href: "/ru/reference/settings/merge-tree-settings/kill-delay-period#kill_delay_period" },
        { name: "kill_delay_period_random_add", href: "/ru/reference/settings/merge-tree-settings/kill-delay-period#kill_delay_period_random_add" }
      ],
      children: []
    },
    {
      label: "map_buckets_*",
      count: 3,
      settings: [
        { name: "map_buckets_coefficient", href: "/ru/reference/settings/merge-tree-settings/map-buckets#map_buckets_coefficient" },
        { name: "map_buckets_min_avg_size", href: "/ru/reference/settings/merge-tree-settings/map-buckets#map_buckets_min_avg_size" },
        { name: "map_buckets_strategy", href: "/ru/reference/settings/merge-tree-settings/map-buckets#map_buckets_strategy" }
      ],
      children: []
    },
    {
      label: "map_serialization_version_*",
      count: 2,
      settings: [
        { name: "map_serialization_version", href: "/ru/reference/settings/merge-tree-settings/map-serialization-version#map_serialization_version" },
        { name: "map_serialization_version_for_zero_level_parts", href: "/ru/reference/settings/merge-tree-settings/map-serialization-version#map_serialization_version_for_zero_level_parts" }
      ],
      children: []
    },
    {
      label: "marks_*",
      count: 2,
      settings: [
        { name: "marks_compress_block_size", href: "/ru/reference/settings/merge-tree-settings/marks#marks_compress_block_size" },
        { name: "marks_compression_codec", href: "/ru/reference/settings/merge-tree-settings/marks#marks_compression_codec" }
      ],
      children: []
    },
    {
      label: "materialize_*",
      count: 3,
      settings: [
        { name: "materialize_skip_indexes_on_merge", href: "/ru/reference/settings/merge-tree-settings/materialize#materialize_skip_indexes_on_merge" },
        { name: "materialize_statistics_on_merge", href: "/ru/reference/settings/merge-tree-settings/materialize#materialize_statistics_on_merge" },
        { name: "materialize_ttl_recalculate_only", href: "/ru/reference/settings/merge-tree-settings/materialize#materialize_ttl_recalculate_only" }
      ],
      children: []
    },
    {
      label: "materialize_projections_*",
      count: 2,
      settings: [
        { name: "materialize_projections_on_insert", href: "/ru/reference/settings/merge-tree-settings/materialize-projections#materialize_projections_on_insert" },
        { name: "materialize_projections_on_merge", href: "/ru/reference/settings/merge-tree-settings/materialize-projections#materialize_projections_on_merge" }
      ],
      children: []
    },
    {
      label: "max_*",
      count: 10,
      settings: [
        { name: "max_avg_part_size_for_too_many_parts", href: "/ru/reference/settings/merge-tree-settings/max#max_avg_part_size_for_too_many_parts" },
        { name: "max_buckets_in_map", href: "/ru/reference/settings/merge-tree-settings/max#max_buckets_in_map" },
        { name: "max_cleanup_delay_period", href: "/ru/reference/settings/merge-tree-settings/max#max_cleanup_delay_period" },
        { name: "max_compress_block_size", href: "/ru/reference/settings/merge-tree-settings/max#max_compress_block_size" },
        { name: "max_concurrent_queries", href: "/ru/reference/settings/merge-tree-settings/max#max_concurrent_queries" },
        { name: "max_digestion_size_per_segment", href: "/ru/reference/settings/merge-tree-settings/max#max_digestion_size_per_segment" },
        { name: "max_file_name_length", href: "/ru/reference/settings/merge-tree-settings/max#max_file_name_length" },
        { name: "max_partitions_to_read", href: "/ru/reference/settings/merge-tree-settings/max#max_partitions_to_read" },
        { name: "max_projections", href: "/ru/reference/settings/merge-tree-settings/max#max_projections" },
        { name: "max_uncompressed_bytes_in_patches", href: "/ru/reference/settings/merge-tree-settings/max#max_uncompressed_bytes_in_patches" }
      ],
      children: []
    },
    {
      label: "max_bytes_*",
      count: 2,
      settings: [
        { name: "max_bytes_to_merge_at_max_space_in_pool", href: "/ru/reference/settings/merge-tree-settings/max-bytes#max_bytes_to_merge_at_max_space_in_pool" },
        { name: "max_bytes_to_merge_at_min_space_in_pool", href: "/ru/reference/settings/merge-tree-settings/max-bytes#max_bytes_to_merge_at_min_space_in_pool" }
      ],
      children: []
    },
    {
      label: "max_delay_*",
      count: 2,
      settings: [
        { name: "max_delay_to_insert", href: "/ru/reference/settings/merge-tree-settings/max-delay#max_delay_to_insert" },
        { name: "max_delay_to_mutate_ms", href: "/ru/reference/settings/merge-tree-settings/max-delay#max_delay_to_mutate_ms" }
      ],
      children: []
    },
    {
      label: "max_files_*",
      count: 2,
      settings: [
        { name: "max_files_to_modify_in_alter_columns", href: "/ru/reference/settings/merge-tree-settings/max-files#max_files_to_modify_in_alter_columns" },
        { name: "max_files_to_remove_in_alter_columns", href: "/ru/reference/settings/merge-tree-settings/max-files#max_files_to_remove_in_alter_columns" }
      ],
      children: []
    },
    {
      label: "max_merge_*",
      count: 2,
      settings: [
        { name: "max_merge_delayed_streams_for_parallel_write", href: "/ru/reference/settings/merge-tree-settings/max-merge#max_merge_delayed_streams_for_parallel_write" },
        { name: "max_merge_selecting_sleep_ms", href: "/ru/reference/settings/merge-tree-settings/max-merge#max_merge_selecting_sleep_ms" }
      ],
      children: []
    },
    {
      label: "max_number_*",
      count: 2,
      settings: [
        { name: "max_number_of_merges_with_ttl_in_pool", href: "/ru/reference/settings/merge-tree-settings/max-number#max_number_of_merges_with_ttl_in_pool" },
        { name: "max_number_of_mutations_for_replica", href: "/ru/reference/settings/merge-tree-settings/max-number#max_number_of_mutations_for_replica" }
      ],
      children: []
    },
    {
      label: "max_part_*",
      count: 2,
      settings: [
        { name: "max_part_loading_threads", href: "/ru/reference/settings/merge-tree-settings/max-part#max_part_loading_threads" },
        { name: "max_part_removal_threads", href: "/ru/reference/settings/merge-tree-settings/max-part#max_part_removal_threads" }
      ],
      children: []
    },
    {
      label: "max_parts_*",
      count: 2,
      settings: [
        { name: "max_parts_in_total", href: "/ru/reference/settings/merge-tree-settings/max-parts#max_parts_in_total" },
        { name: "max_parts_to_merge_at_once", href: "/ru/reference/settings/merge-tree-settings/max-parts#max_parts_to_merge_at_once" }
      ],
      children: []
    },
    {
      label: "max_postpone_*",
      count: 4,
      settings: [
        { name: "max_postpone_time_for_failed_mutations_ms", href: "/ru/reference/settings/merge-tree-settings/max-postpone#max_postpone_time_for_failed_mutations_ms" },
        { name: "max_postpone_time_for_failed_replicated_fetches_ms", href: "/ru/reference/settings/merge-tree-settings/max-postpone#max_postpone_time_for_failed_replicated_fetches_ms" },
        { name: "max_postpone_time_for_failed_replicated_merges_ms", href: "/ru/reference/settings/merge-tree-settings/max-postpone#max_postpone_time_for_failed_replicated_merges_ms" },
        { name: "max_postpone_time_for_failed_replicated_tasks_ms", href: "/ru/reference/settings/merge-tree-settings/max-postpone#max_postpone_time_for_failed_replicated_tasks_ms" }
      ],
      children: []
    },
    {
      label: "max_replicated_*",
      count: 6,
      settings: [
        { name: "max_replicated_fetches_network_bandwidth", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_fetches_network_bandwidth" },
        { name: "max_replicated_logs_to_keep", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_logs_to_keep" },
        { name: "max_replicated_merges_in_queue", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_merges_in_queue" },
        { name: "max_replicated_merges_with_ttl_in_queue", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_merges_with_ttl_in_queue" },
        { name: "max_replicated_mutations_in_queue", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_mutations_in_queue" },
        { name: "max_replicated_sends_network_bandwidth", href: "/ru/reference/settings/merge-tree-settings/max-replicated#max_replicated_sends_network_bandwidth" }
      ],
      children: []
    },
    {
      label: "max_suspicious_broken_parts_*",
      count: 2,
      settings: [
        { name: "max_suspicious_broken_parts", href: "/ru/reference/settings/merge-tree-settings/max-suspicious-broken-parts#max_suspicious_broken_parts" },
        { name: "max_suspicious_broken_parts_bytes", href: "/ru/reference/settings/merge-tree-settings/max-suspicious-broken-parts#max_suspicious_broken_parts_bytes" }
      ],
      children: []
    },
    {
      label: "merge_*",
      count: 2,
      settings: [
        { name: "merge_total_max_bytes_to_prewarm_cache", href: "/ru/reference/settings/merge-tree-settings/merge#merge_total_max_bytes_to_prewarm_cache" },
        { name: "merge_workload", href: "/ru/reference/settings/merge-tree-settings/merge#merge_workload" }
      ],
      children: []
    },
    {
      label: "merge_max_*",
      count: 5,
      settings: [
        { name: "merge_max_block_size", href: "/ru/reference/settings/merge-tree-settings/merge-max#merge_max_block_size" },
        { name: "merge_max_block_size_bytes", href: "/ru/reference/settings/merge-tree-settings/merge-max#merge_max_block_size_bytes" },
        { name: "merge_max_bytes_to_prewarm_cache", href: "/ru/reference/settings/merge-tree-settings/merge-max#merge_max_bytes_to_prewarm_cache" },
        { name: "merge_max_dynamic_subcolumns_in_compact_part", href: "/ru/reference/settings/merge-tree-settings/merge-max#merge_max_dynamic_subcolumns_in_compact_part" },
        { name: "merge_max_dynamic_subcolumns_in_wide_part", href: "/ru/reference/settings/merge-tree-settings/merge-max#merge_max_dynamic_subcolumns_in_wide_part" }
      ],
      children: []
    },
    {
      label: "merge_selecting_*",
      count: 2,
      settings: [
        { name: "merge_selecting_sleep_ms", href: "/ru/reference/settings/merge-tree-settings/merge-selecting#merge_selecting_sleep_ms" },
        { name: "merge_selecting_sleep_slowdown_factor", href: "/ru/reference/settings/merge-tree-settings/merge-selecting#merge_selecting_sleep_slowdown_factor" }
      ],
      children: []
    },
    {
      label: "merge_selector_*",
      count: 7,
      settings: [
        { name: "merge_selector_algorithm", href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_algorithm" },
        { name: "merge_selector_base", href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_base" },
        { name: "merge_selector_blurry_base_scale_factor", href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_blurry_base_scale_factor" },
        {
          name: "merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once",
          href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once"
        },
        {
          name: "merge_selector_enable_heuristic_to_remove_small_parts_at_right",
          href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_enable_heuristic_to_remove_small_parts_at_right"
        },
        {
          name: "merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent",
          href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent"
        },
        { name: "merge_selector_window_size", href: "/ru/reference/settings/merge-tree-settings/merge-selector#merge_selector_window_size" }
      ],
      children: []
    },
    {
      label: "merge_tree_*",
      count: 4,
      settings: [
        {
          name: "merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds",
          href: "/ru/reference/settings/merge-tree-settings/merge-tree#merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds"
        },
        { name: "merge_tree_clear_old_parts_interval_seconds", href: "/ru/reference/settings/merge-tree-settings/merge-tree#merge_tree_clear_old_parts_interval_seconds" },
        { name: "merge_tree_clear_old_temporary_directories_interval_seconds", href: "/ru/reference/settings/merge-tree-settings/merge-tree#merge_tree_clear_old_temporary_directories_interval_seconds" },
        { name: "merge_tree_enable_clear_old_broken_detached", href: "/ru/reference/settings/merge-tree-settings/merge-tree#merge_tree_enable_clear_old_broken_detached" }
      ],
      children: []
    },
    {
      label: "merge_with_*",
      count: 2,
      settings: [
        { name: "merge_with_recompression_ttl_timeout", href: "/ru/reference/settings/merge-tree-settings/merge-with#merge_with_recompression_ttl_timeout" },
        { name: "merge_with_ttl_timeout", href: "/ru/reference/settings/merge-tree-settings/merge-with#merge_with_ttl_timeout" }
      ],
      children: []
    },
    {
      label: "min_*",
      count: 8,
      settings: [
        { name: "min_absolute_delay_to_close", href: "/ru/reference/settings/merge-tree-settings/min#min_absolute_delay_to_close" },
        { name: "min_columns_to_activate_adaptive_write_buffer", href: "/ru/reference/settings/merge-tree-settings/min#min_columns_to_activate_adaptive_write_buffer" },
        { name: "min_compress_block_size", href: "/ru/reference/settings/merge-tree-settings/min#min_compress_block_size" },
        { name: "min_index_granularity_bytes", href: "/ru/reference/settings/merge-tree-settings/min#min_index_granularity_bytes" },
        { name: "min_marks_to_honor_max_concurrent_queries", href: "/ru/reference/settings/merge-tree-settings/min#min_marks_to_honor_max_concurrent_queries" },
        { name: "min_merge_bytes_to_use_direct_io", href: "/ru/reference/settings/merge-tree-settings/min#min_merge_bytes_to_use_direct_io" },
        { name: "min_parts_to_merge_at_once", href: "/ru/reference/settings/merge-tree-settings/min#min_parts_to_merge_at_once" },
        { name: "min_replicated_logs_to_keep", href: "/ru/reference/settings/merge-tree-settings/min#min_replicated_logs_to_keep" }
      ],
      children: []
    },
    {
      label: "min_age_*",
      count: 2,
      settings: [
        { name: "min_age_to_force_merge_on_partition_only", href: "/ru/reference/settings/merge-tree-settings/min-age#min_age_to_force_merge_on_partition_only" },
        { name: "min_age_to_force_merge_seconds", href: "/ru/reference/settings/merge-tree-settings/min-age#min_age_to_force_merge_seconds" }
      ],
      children: []
    },
    {
      label: "min_bytes_*",
      count: 5,
      settings: [
        { name: "min_bytes_for_compact_part", href: "/ru/reference/settings/merge-tree-settings/min-bytes#min_bytes_for_compact_part" },
        { name: "min_bytes_for_full_part_storage", href: "/ru/reference/settings/merge-tree-settings/min-bytes#min_bytes_for_full_part_storage" },
        { name: "min_bytes_for_wide_part", href: "/ru/reference/settings/merge-tree-settings/min-bytes#min_bytes_for_wide_part" },
        { name: "min_bytes_to_prewarm_caches", href: "/ru/reference/settings/merge-tree-settings/min-bytes#min_bytes_to_prewarm_caches" },
        { name: "min_bytes_to_rebalance_partition_over_jbod", href: "/ru/reference/settings/merge-tree-settings/min-bytes#min_bytes_to_rebalance_partition_over_jbod" }
      ],
      children: []
    },
    {
      label: "min_compressed_*",
      count: 2,
      settings: [
        { name: "min_compressed_bytes_to_fsync_after_fetch", href: "/ru/reference/settings/merge-tree-settings/min-compressed#min_compressed_bytes_to_fsync_after_fetch" },
        { name: "min_compressed_bytes_to_fsync_after_merge", href: "/ru/reference/settings/merge-tree-settings/min-compressed#min_compressed_bytes_to_fsync_after_merge" }
      ],
      children: []
    },
    {
      label: "min_delay_*",
      count: 2,
      settings: [
        { name: "min_delay_to_insert_ms", href: "/ru/reference/settings/merge-tree-settings/min-delay#min_delay_to_insert_ms" },
        { name: "min_delay_to_mutate_ms", href: "/ru/reference/settings/merge-tree-settings/min-delay#min_delay_to_mutate_ms" }
      ],
      children: []
    },
    {
      label: "min_free_*",
      count: 2,
      settings: [
        { name: "min_free_disk_bytes_to_perform_insert", href: "/ru/reference/settings/merge-tree-settings/min-free#min_free_disk_bytes_to_perform_insert" },
        { name: "min_free_disk_ratio_to_perform_insert", href: "/ru/reference/settings/merge-tree-settings/min-free#min_free_disk_ratio_to_perform_insert" }
      ],
      children: []
    },
    {
      label: "min_level_*",
      count: 2,
      settings: [
        { name: "min_level_for_full_part_storage", href: "/ru/reference/settings/merge-tree-settings/min-level#min_level_for_full_part_storage" },
        { name: "min_level_for_wide_part", href: "/ru/reference/settings/merge-tree-settings/min-level#min_level_for_wide_part" }
      ],
      children: []
    },
    {
      label: "min_relative_*",
      count: 3,
      settings: [
        { name: "min_relative_delay_to_close", href: "/ru/reference/settings/merge-tree-settings/min-relative#min_relative_delay_to_close" },
        { name: "min_relative_delay_to_measure", href: "/ru/reference/settings/merge-tree-settings/min-relative#min_relative_delay_to_measure" },
        { name: "min_relative_delay_to_yield_leadership", href: "/ru/reference/settings/merge-tree-settings/min-relative#min_relative_delay_to_yield_leadership" }
      ],
      children: []
    },
    {
      label: "min_rows_*",
      count: 4,
      settings: [
        { name: "min_rows_for_compact_part", href: "/ru/reference/settings/merge-tree-settings/min-rows#min_rows_for_compact_part" },
        { name: "min_rows_for_full_part_storage", href: "/ru/reference/settings/merge-tree-settings/min-rows#min_rows_for_full_part_storage" },
        { name: "min_rows_for_wide_part", href: "/ru/reference/settings/merge-tree-settings/min-rows#min_rows_for_wide_part" },
        { name: "min_rows_to_fsync_after_merge", href: "/ru/reference/settings/merge-tree-settings/min-rows#min_rows_to_fsync_after_merge" }
      ],
      children: []
    },
    {
      label: "number_of_*",
      count: 6,
      settings: [
        { name: "number_of_free_entries_in_pool_to_execute_mutation", href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_free_entries_in_pool_to_execute_mutation" },
        {
          name: "number_of_free_entries_in_pool_to_execute_optimize_entire_partition",
          href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_free_entries_in_pool_to_execute_optimize_entire_partition"
        },
        { name: "number_of_free_entries_in_pool_to_lower_max_size_of_merge", href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_free_entries_in_pool_to_lower_max_size_of_merge" },
        { name: "number_of_mutations_to_delay", href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_mutations_to_delay" },
        { name: "number_of_mutations_to_throw", href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_mutations_to_throw" },
        { name: "number_of_partitions_to_consider_for_merge", href: "/ru/reference/settings/merge-tree-settings/number-of#number_of_partitions_to_consider_for_merge" }
      ],
      children: []
    },
    {
      label: "object_shared_*",
      count: 4,
      settings: [
        { name: "object_shared_data_buckets_for_compact_part", href: "/ru/reference/settings/merge-tree-settings/object-shared#object_shared_data_buckets_for_compact_part" },
        { name: "object_shared_data_buckets_for_wide_part", href: "/ru/reference/settings/merge-tree-settings/object-shared#object_shared_data_buckets_for_wide_part" },
        { name: "object_shared_data_serialization_version", href: "/ru/reference/settings/merge-tree-settings/object-shared#object_shared_data_serialization_version" },
        {
          name: "object_shared_data_serialization_version_for_zero_level_parts",
          href: "/ru/reference/settings/merge-tree-settings/object-shared#object_shared_data_serialization_version_for_zero_level_parts"
        }
      ],
      children: []
    },
    {
      label: "part_moves_*",
      count: 2,
      settings: [
        { name: "part_moves_between_shards_delay_seconds", href: "/ru/reference/settings/merge-tree-settings/part-moves#part_moves_between_shards_delay_seconds" },
        { name: "part_moves_between_shards_enable", href: "/ru/reference/settings/merge-tree-settings/part-moves#part_moves_between_shards_enable" }
      ],
      children: []
    },
    {
      label: "parts_to_*",
      count: 2,
      settings: [
        { name: "parts_to_delay_insert", href: "/ru/reference/settings/merge-tree-settings/parts-to#parts_to_delay_insert" },
        { name: "parts_to_throw_insert", href: "/ru/reference/settings/merge-tree-settings/parts-to#parts_to_throw_insert" }
      ],
      children: []
    },
    {
      label: "prefer_fetch_*",
      count: 2,
      settings: [
        { name: "prefer_fetch_merged_part_size_threshold", href: "/ru/reference/settings/merge-tree-settings/prefer-fetch#prefer_fetch_merged_part_size_threshold" },
        { name: "prefer_fetch_merged_part_time_threshold", href: "/ru/reference/settings/merge-tree-settings/prefer-fetch#prefer_fetch_merged_part_time_threshold" }
      ],
      children: []
    },
    {
      label: "prewarm_*",
      count: 2,
      settings: [
        { name: "prewarm_mark_cache", href: "/ru/reference/settings/merge-tree-settings/prewarm#prewarm_mark_cache" },
        { name: "prewarm_primary_key_cache", href: "/ru/reference/settings/merge-tree-settings/prewarm#prewarm_primary_key_cache" }
      ],
      children: []
    },
    {
      label: "primary_key_*",
      count: 4,
      settings: [
        { name: "primary_key_compress_block_size", href: "/ru/reference/settings/merge-tree-settings/primary-key#primary_key_compress_block_size" },
        { name: "primary_key_compression_codec", href: "/ru/reference/settings/merge-tree-settings/primary-key#primary_key_compression_codec" },
        { name: "primary_key_lazy_load", href: "/ru/reference/settings/merge-tree-settings/primary-key#primary_key_lazy_load" },
        {
          name: "primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns",
          href: "/ru/reference/settings/merge-tree-settings/primary-key#primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns"
        }
      ],
      children: []
    },
    {
      label: "refresh_*",
      count: 2,
      settings: [
        { name: "refresh_parts_interval", href: "/ru/reference/settings/merge-tree-settings/refresh#refresh_parts_interval" },
        { name: "refresh_statistics_interval", href: "/ru/reference/settings/merge-tree-settings/refresh#refresh_statistics_interval" }
      ],
      children: []
    },
    {
      label: "remote_fs_*",
      count: 3,
      settings: [
        { name: "remote_fs_execute_merges_on_single_replica_time_threshold", href: "/ru/reference/settings/merge-tree-settings/remote-fs#remote_fs_execute_merges_on_single_replica_time_threshold" },
        { name: "remote_fs_zero_copy_path_compatible_mode", href: "/ru/reference/settings/merge-tree-settings/remote-fs#remote_fs_zero_copy_path_compatible_mode" },
        { name: "remote_fs_zero_copy_zookeeper_path", href: "/ru/reference/settings/merge-tree-settings/remote-fs#remote_fs_zero_copy_zookeeper_path" }
      ],
      children: []
    },
    {
      label: "remove_*",
      count: 3,
      settings: [
        { name: "remove_empty_parts", href: "/ru/reference/settings/merge-tree-settings/remove#remove_empty_parts" },
        { name: "remove_rolled_back_parts_immediately", href: "/ru/reference/settings/merge-tree-settings/remove#remove_rolled_back_parts_immediately" },
        { name: "remove_unused_patch_parts", href: "/ru/reference/settings/merge-tree-settings/remove#remove_unused_patch_parts" }
      ],
      children: []
    },
    {
      label: "replicated_deduplication_window_*",
      count: 4,
      settings: [
        { name: "replicated_deduplication_window", href: "/ru/reference/settings/merge-tree-settings/replicated-deduplication-window#replicated_deduplication_window" },
        {
          name: "replicated_deduplication_window_for_async_inserts",
          href: "/ru/reference/settings/merge-tree-settings/replicated-deduplication-window#replicated_deduplication_window_for_async_inserts"
        },
        { name: "replicated_deduplication_window_seconds", href: "/ru/reference/settings/merge-tree-settings/replicated-deduplication-window#replicated_deduplication_window_seconds" },
        {
          name: "replicated_deduplication_window_seconds_for_async_inserts",
          href: "/ru/reference/settings/merge-tree-settings/replicated-deduplication-window#replicated_deduplication_window_seconds_for_async_inserts"
        }
      ],
      children: []
    },
    {
      label: "replicated_fetches_*",
      count: 5,
      settings: [
        { name: "replicated_fetches_http_connection_timeout", href: "/ru/reference/settings/merge-tree-settings/replicated-fetches#replicated_fetches_http_connection_timeout" },
        { name: "replicated_fetches_http_receive_timeout", href: "/ru/reference/settings/merge-tree-settings/replicated-fetches#replicated_fetches_http_receive_timeout" },
        { name: "replicated_fetches_http_send_timeout", href: "/ru/reference/settings/merge-tree-settings/replicated-fetches#replicated_fetches_http_send_timeout" },
        { name: "replicated_fetches_min_part_level", href: "/ru/reference/settings/merge-tree-settings/replicated-fetches#replicated_fetches_min_part_level" },
        { name: "replicated_fetches_min_part_level_timeout_seconds", href: "/ru/reference/settings/merge-tree-settings/replicated-fetches#replicated_fetches_min_part_level_timeout_seconds" }
      ],
      children: []
    },
    {
      label: "replicated_max_*",
      count: 7,
      settings: [
        { name: "replicated_max_mutations_in_one_entry", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_mutations_in_one_entry" },
        { name: "replicated_max_parallel_fetches", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_parallel_fetches" },
        { name: "replicated_max_parallel_fetches_for_host", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_parallel_fetches_for_host" },
        { name: "replicated_max_parallel_fetches_for_table", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_parallel_fetches_for_table" },
        { name: "replicated_max_parallel_sends", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_parallel_sends" },
        { name: "replicated_max_parallel_sends_for_table", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_parallel_sends_for_table" },
        { name: "replicated_max_ratio_of_wrong_parts", href: "/ru/reference/settings/merge-tree-settings/replicated-max#replicated_max_ratio_of_wrong_parts" }
      ],
      children: []
    },
    {
      label: "shared_merge_*",
      count: 51,
      settings: [
        { name: "shared_merge_tree_activate_coordinated_merges_tasks", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_activate_coordinated_merges_tasks" },
        { name: "shared_merge_tree_create_per_replica_metadata_nodes", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_create_per_replica_metadata_nodes" },
        { name: "shared_merge_tree_disable_merges_and_mutations_assignment", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_disable_merges_and_mutations_assignment" },
        { name: "shared_merge_tree_empty_partition_lifetime", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_empty_partition_lifetime" },
        {
          name: "shared_merge_tree_enable_automatic_empty_partitions_cleanup",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_enable_automatic_empty_partitions_cleanup"
        },
        { name: "shared_merge_tree_enable_coordinated_merges", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_enable_coordinated_merges" },
        { name: "shared_merge_tree_enable_keeper_parts_extra_data", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_enable_keeper_parts_extra_data" },
        { name: "shared_merge_tree_enable_outdated_parts_check", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_enable_outdated_parts_check" },
        { name: "shared_merge_tree_idle_parts_update_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_idle_parts_update_seconds" },
        { name: "shared_merge_tree_inactive_replica_cutoff_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_inactive_replica_cutoff_seconds" },
        { name: "shared_merge_tree_initial_parts_update_backoff_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_initial_parts_update_backoff_ms" },
        { name: "shared_merge_tree_interserver_http_connection_timeout_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_interserver_http_connection_timeout_ms" },
        { name: "shared_merge_tree_interserver_http_timeout_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_interserver_http_timeout_ms" },
        { name: "shared_merge_tree_leader_update_period_random_add_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_leader_update_period_random_add_seconds" },
        { name: "shared_merge_tree_leader_update_period_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_leader_update_period_seconds" },
        { name: "shared_merge_tree_max_outdated_parts_to_process_at_once", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_outdated_parts_to_process_at_once" },
        { name: "shared_merge_tree_max_parts_update_backoff_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_parts_update_backoff_ms" },
        { name: "shared_merge_tree_max_parts_update_leaders_in_total", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_parts_update_leaders_in_total" },
        { name: "shared_merge_tree_max_parts_update_leaders_per_az", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_parts_update_leaders_per_az" },
        { name: "shared_merge_tree_max_replicas_for_parts_deletion", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_replicas_for_parts_deletion" },
        {
          name: "shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range"
        },
        { name: "shared_merge_tree_max_suspicious_broken_parts", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_suspicious_broken_parts" },
        { name: "shared_merge_tree_max_suspicious_broken_parts_bytes", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_max_suspicious_broken_parts_bytes" },
        { name: "shared_merge_tree_memo_ids_remove_timeout_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_memo_ids_remove_timeout_seconds" },
        { name: "shared_merge_tree_merge_coordinator_distribution_algorithm", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_distribution_algorithm" },
        {
          name: "shared_merge_tree_merge_coordinator_election_check_period_ms",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_election_check_period_ms"
        },
        { name: "shared_merge_tree_merge_coordinator_factor", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_factor" },
        {
          name: "shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms"
        },
        { name: "shared_merge_tree_merge_coordinator_max_merge_request_size", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_max_merge_request_size" },
        { name: "shared_merge_tree_merge_coordinator_max_period_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_max_period_ms" },
        { name: "shared_merge_tree_merge_coordinator_merges_prepare_count", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_merges_prepare_count" },
        { name: "shared_merge_tree_merge_coordinator_min_period_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_coordinator_min_period_ms" },
        { name: "shared_merge_tree_merge_worker_fast_timeout_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_worker_fast_timeout_ms" },
        { name: "shared_merge_tree_merge_worker_regular_timeout_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_merge_worker_regular_timeout_ms" },
        { name: "shared_merge_tree_outdated_parts_group_size", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_outdated_parts_group_size" },
        {
          name: "shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations"
        },
        { name: "shared_merge_tree_parts_load_batch_size", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_parts_load_batch_size" },
        {
          name: "shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms"
        },
        {
          name: "shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold"
        },
        { name: "shared_merge_tree_range_for_merge_window_size", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_range_for_merge_window_size" },
        { name: "shared_merge_tree_read_virtual_parts_from_leader", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_read_virtual_parts_from_leader" },
        { name: "shared_merge_tree_replica_set_max_lifetime_seconds", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_replica_set_max_lifetime_seconds" },
        {
          name: "shared_merge_tree_try_fetch_part_in_memory_data_from_replicas",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_try_fetch_part_in_memory_data_from_replicas"
        },
        {
          name: "shared_merge_tree_try_fetch_part_in_memory_data_from_replicas_on_startup",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_try_fetch_part_in_memory_data_from_replicas_on_startup"
        },
        { name: "shared_merge_tree_update_replica_flags_delay_ms", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_update_replica_flags_delay_ms" },
        { name: "shared_merge_tree_use_metadata_hints_cache", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_use_metadata_hints_cache" },
        { name: "shared_merge_tree_use_outdated_parts_compact_format", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_use_outdated_parts_compact_format" },
        {
          name: "shared_merge_tree_use_too_many_parts_count_from_virtual_parts",
          href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_use_too_many_parts_count_from_virtual_parts"
        },
        { name: "shared_merge_tree_use_zookeeper_connection_pool", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_use_zookeeper_connection_pool" },
        { name: "shared_merge_tree_virtual_parts_discovery_batch", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_virtual_parts_discovery_batch" },
        { name: "shared_merge_tree_virtual_parts_partition_atomic_discovery", href: "/ru/reference/settings/merge-tree-settings/shared-merge#shared_merge_tree_virtual_parts_partition_atomic_discovery" }
      ],
      children: []
    },
    {
      label: "sleep_before_*",
      count: 2,
      settings: [
        { name: "sleep_before_commit_local_part_in_replicated_table_ms", href: "/ru/reference/settings/merge-tree-settings/sleep-before#sleep_before_commit_local_part_in_replicated_table_ms" },
        { name: "sleep_before_loading_outdated_parts_ms", href: "/ru/reference/settings/merge-tree-settings/sleep-before#sleep_before_loading_outdated_parts_ms" }
      ],
      children: []
    },
    {
      label: "table_*",
      count: 2,
      settings: [
        { name: "table_disk", href: "/ru/reference/settings/merge-tree-settings/table#table_disk" },
        { name: "table_readonly", href: "/ru/reference/settings/merge-tree-settings/table#table_readonly" }
      ],
      children: []
    },
    {
      label: "text_index_*",
      count: 4,
      settings: [
        { name: "text_index_dictionary_block_frontcoding_compression", href: "/ru/reference/settings/merge-tree-settings/text-index#text_index_dictionary_block_frontcoding_compression" },
        { name: "text_index_dictionary_block_size", href: "/ru/reference/settings/merge-tree-settings/text-index#text_index_dictionary_block_size" },
        { name: "text_index_posting_list_block_size", href: "/ru/reference/settings/merge-tree-settings/text-index#text_index_posting_list_block_size" },
        { name: "text_index_posting_list_codec", href: "/ru/reference/settings/merge-tree-settings/text-index#text_index_posting_list_codec" }
      ],
      children: []
    },
    {
      label: "use_*",
      count: 6,
      settings: [
        { name: "use_adaptive_write_buffer_for_dynamic_subcolumns", href: "/ru/reference/settings/merge-tree-settings/use#use_adaptive_write_buffer_for_dynamic_subcolumns" },
        { name: "use_async_block_ids_cache", href: "/ru/reference/settings/merge-tree-settings/use#use_async_block_ids_cache" },
        { name: "use_compact_variant_discriminators_serialization", href: "/ru/reference/settings/merge-tree-settings/use#use_compact_variant_discriminators_serialization" },
        { name: "use_const_adaptive_granularity", href: "/ru/reference/settings/merge-tree-settings/use#use_const_adaptive_granularity" },
        { name: "use_metadata_cache", href: "/ru/reference/settings/merge-tree-settings/use#use_metadata_cache" },
        { name: "use_primary_key_cache", href: "/ru/reference/settings/merge-tree-settings/use#use_primary_key_cache" }
      ],
      children: []
    },
    {
      label: "use_minimalistic_*",
      count: 2,
      settings: [
        { name: "use_minimalistic_checksums_in_zookeeper", href: "/ru/reference/settings/merge-tree-settings/use-minimalistic#use_minimalistic_checksums_in_zookeeper" },
        { name: "use_minimalistic_part_header_in_zookeeper", href: "/ru/reference/settings/merge-tree-settings/use-minimalistic#use_minimalistic_part_header_in_zookeeper" }
      ],
      children: []
    },
    {
      label: "vertical_merge_*",
      count: 6,
      settings: [
        { name: "vertical_merge_algorithm_min_bytes_to_activate", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_algorithm_min_bytes_to_activate" },
        { name: "vertical_merge_algorithm_min_columns_to_activate", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_algorithm_min_columns_to_activate" },
        { name: "vertical_merge_algorithm_min_rows_to_activate", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_algorithm_min_rows_to_activate" },
        { name: "vertical_merge_optimize_lightweight_delete", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_optimize_lightweight_delete" },
        { name: "vertical_merge_optimize_ttl_delete", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_optimize_ttl_delete" },
        { name: "vertical_merge_remote_filesystem_prefetch", href: "/ru/reference/settings/merge-tree-settings/vertical-merge#vertical_merge_remote_filesystem_prefetch" }
      ],
      children: []
    },
    {
      label: "write_*",
      count: 2,
      settings: [
        { name: "write_final_mark", href: "/ru/reference/settings/merge-tree-settings/write#write_final_mark" },
        { name: "write_marks_for_substreams_in_compact_parts", href: "/ru/reference/settings/merge-tree-settings/write#write_marks_for_substreams_in_compact_parts" }
      ],
      children: []
    },
    {
      label: "write_ahead_*",
      count: 3,
      settings: [
        { name: "write_ahead_log_bytes_to_fsync", href: "/ru/reference/settings/merge-tree-settings/write-ahead#write_ahead_log_bytes_to_fsync" },
        { name: "write_ahead_log_interval_ms_to_fsync", href: "/ru/reference/settings/merge-tree-settings/write-ahead#write_ahead_log_interval_ms_to_fsync" },
        { name: "write_ahead_log_max_bytes", href: "/ru/reference/settings/merge-tree-settings/write-ahead#write_ahead_log_max_bytes" }
      ],
      children: []
    },
    {
      label: "zero_copy_*",
      count: 4,
      settings: [
        { name: "zero_copy_concurrent_part_removal_max_postpone_ratio", href: "/ru/reference/settings/merge-tree-settings/zero-copy#zero_copy_concurrent_part_removal_max_postpone_ratio" },
        { name: "zero_copy_concurrent_part_removal_max_split_times", href: "/ru/reference/settings/merge-tree-settings/zero-copy#zero_copy_concurrent_part_removal_max_split_times" },
        { name: "zero_copy_merge_mutation_min_parts_size_sleep_before_lock", href: "/ru/reference/settings/merge-tree-settings/zero-copy#zero_copy_merge_mutation_min_parts_size_sleep_before_lock" },
        {
          name: "zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock",
          href: "/ru/reference/settings/merge-tree-settings/zero-copy#zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock"
        }
      ],
      children: []
    },
    {
      label: "Other",
      count: 50,
      settings: [
        { name: "adaptive_write_buffer_initial_size", href: "/ru/reference/settings/merge-tree-settings/other#adaptive_write_buffer_initial_size" },
        { name: "add_implicit_sign_column_constraint_for_collapsing_engine", href: "/ru/reference/settings/merge-tree-settings/other#add_implicit_sign_column_constraint_for_collapsing_engine" },
        { name: "alter_column_secondary_index_mode", href: "/ru/reference/settings/merge-tree-settings/other#alter_column_secondary_index_mode" },
        { name: "apply_patches_on_merge", href: "/ru/reference/settings/merge-tree-settings/other#apply_patches_on_merge" },
        { name: "assign_part_uuids", href: "/ru/reference/settings/merge-tree-settings/other#assign_part_uuids" },
        { name: "auto_statistics_types", href: "/ru/reference/settings/merge-tree-settings/other#auto_statistics_types" },
        { name: "background_task_preferred_step_execution_time_ms", href: "/ru/reference/settings/merge-tree-settings/other#background_task_preferred_step_execution_time_ms" },
        { name: "clean_deleted_rows", href: "/ru/reference/settings/merge-tree-settings/other#clean_deleted_rows" },
        { name: "clone_replica_zookeeper_create_get_part_batch_size", href: "/ru/reference/settings/merge-tree-settings/other#clone_replica_zookeeper_create_get_part_batch_size" },
        { name: "compatibility_allow_sampling_expression_not_in_primary_key", href: "/ru/reference/settings/merge-tree-settings/other#compatibility_allow_sampling_expression_not_in_primary_key" },
        { name: "compute_exact_num_defaults_for_sparse_columns", href: "/ru/reference/settings/merge-tree-settings/other#compute_exact_num_defaults_for_sparse_columns" },
        { name: "deduplicate_merge_projection_mode", href: "/ru/reference/settings/merge-tree-settings/other#deduplicate_merge_projection_mode" },
        { name: "deduplication_hashes_cache_update_wait_ms", href: "/ru/reference/settings/merge-tree-settings/other#deduplication_hashes_cache_update_wait_ms" },
        { name: "default_compression_codec", href: "/ru/reference/settings/merge-tree-settings/other#default_compression_codec" },
        { name: "disk", href: "/ru/reference/settings/merge-tree-settings/other#disk" },
        { name: "dynamic_serialization_version", href: "/ru/reference/settings/merge-tree-settings/other#dynamic_serialization_version" },
        { name: "enforce_index_structure_match_on_partition_manipulation", href: "/ru/reference/settings/merge-tree-settings/other#enforce_index_structure_match_on_partition_manipulation" },
        { name: "execute_merges_on_single_replica_time_threshold", href: "/ru/reference/settings/merge-tree-settings/other#execute_merges_on_single_replica_time_threshold" },
        { name: "finished_mutations_to_keep", href: "/ru/reference/settings/merge-tree-settings/other#finished_mutations_to_keep" },
        { name: "force_read_through_cache_for_merges", href: "/ru/reference/settings/merge-tree-settings/other#force_read_through_cache_for_merges" },
        { name: "initialization_retry_period", href: "/ru/reference/settings/merge-tree-settings/other#initialization_retry_period" },
        { name: "kill_threads", href: "/ru/reference/settings/merge-tree-settings/other#kill_threads" },
        { name: "lightweight_mutation_projection_mode", href: "/ru/reference/settings/merge-tree-settings/other#lightweight_mutation_projection_mode" },
        { name: "load_existing_rows_count_for_old_parts", href: "/ru/reference/settings/merge-tree-settings/other#load_existing_rows_count_for_old_parts" },
        { name: "lock_acquire_timeout_for_background_operations", href: "/ru/reference/settings/merge-tree-settings/other#lock_acquire_timeout_for_background_operations" },
        { name: "mutation_workload", href: "/ru/reference/settings/merge-tree-settings/other#mutation_workload" },
        { name: "non_replicated_deduplication_window", href: "/ru/reference/settings/merge-tree-settings/other#non_replicated_deduplication_window" },
        { name: "notify_newest_block_number", href: "/ru/reference/settings/merge-tree-settings/other#notify_newest_block_number" },
        { name: "nullable_serialization_version", href: "/ru/reference/settings/merge-tree-settings/other#nullable_serialization_version" },
        { name: "object_serialization_version", href: "/ru/reference/settings/merge-tree-settings/other#object_serialization_version" },
        { name: "old_parts_lifetime", href: "/ru/reference/settings/merge-tree-settings/other#old_parts_lifetime" },
        { name: "optimize_row_order", href: "/ru/reference/settings/merge-tree-settings/other#optimize_row_order" },
        { name: "packed_skip_index_max_bytes", href: "/ru/reference/settings/merge-tree-settings/other#packed_skip_index_max_bytes" },
        { name: "part_minmax_index_columns", href: "/ru/reference/settings/merge-tree-settings/other#part_minmax_index_columns" },
        { name: "propagate_types_serialization_versions_to_nested_types", href: "/ru/reference/settings/merge-tree-settings/other#propagate_types_serialization_versions_to_nested_types" },
        { name: "ratio_of_defaults_for_sparse_serialization", href: "/ru/reference/settings/merge-tree-settings/other#ratio_of_defaults_for_sparse_serialization" },
        { name: "reduce_blocking_parts_sleep_ms", href: "/ru/reference/settings/merge-tree-settings/other#reduce_blocking_parts_sleep_ms" },
        { name: "replace_long_file_name_to_hash", href: "/ru/reference/settings/merge-tree-settings/other#replace_long_file_name_to_hash" },
        { name: "replicated_can_become_leader", href: "/ru/reference/settings/merge-tree-settings/other#replicated_can_become_leader" },
        { name: "search_orphaned_parts_disks", href: "/ru/reference/settings/merge-tree-settings/other#search_orphaned_parts_disks" },
        { name: "serialization_info_version", href: "/ru/reference/settings/merge-tree-settings/other#serialization_info_version" },
        { name: "share_nested_offsets", href: "/ru/reference/settings/merge-tree-settings/other#share_nested_offsets" },
        { name: "simultaneous_parts_removal_limit", href: "/ru/reference/settings/merge-tree-settings/other#simultaneous_parts_removal_limit" },
        { name: "storage_policy", href: "/ru/reference/settings/merge-tree-settings/other#storage_policy" },
        { name: "string_serialization_version", href: "/ru/reference/settings/merge-tree-settings/other#string_serialization_version" },
        { name: "temporary_directories_lifetime", href: "/ru/reference/settings/merge-tree-settings/other#temporary_directories_lifetime" },
        { name: "try_fetch_recompressed_part_timeout", href: "/ru/reference/settings/merge-tree-settings/other#try_fetch_recompressed_part_timeout" },
        { name: "ttl_only_drop_parts", href: "/ru/reference/settings/merge-tree-settings/other#ttl_only_drop_parts" },
        { name: "wait_for_unique_parts_send_before_shutdown_ms", href: "/ru/reference/settings/merge-tree-settings/other#wait_for_unique_parts_send_before_shutdown_ms" },
        { name: "zookeeper_session_expiration_check_period", href: "/ru/reference/settings/merge-tree-settings/other#zookeeper_session_expiration_check_period" }
      ],
      children: []
    }
  ])
  const [anchorRoutes] = useState(() => ({
    adaptive_write_buffer_initial_size: "/reference/settings/merge-tree-settings/other",
    add_implicit_sign_column_constraint_for_collapsing_engine: "/reference/settings/merge-tree-settings/other",
    add_minmax_index_for_block_number_column: "/reference/settings/merge-tree-settings/add-minmax",
    add_minmax_index_for_block_offset_column: "/reference/settings/merge-tree-settings/add-minmax",
    add_minmax_index_for_numeric_columns: "/reference/settings/merge-tree-settings/add-minmax",
    add_minmax_index_for_string_columns: "/reference/settings/merge-tree-settings/add-minmax",
    add_minmax_index_for_temporal_columns: "/reference/settings/merge-tree-settings/add-minmax",
    allow_coalescing_columns_in_partition_or_order_key: "/reference/settings/merge-tree-settings/allow",
    allow_commit_order_projection: "/reference/settings/merge-tree-settings/allow",
    allow_dimensions_outside_sorting_key: "/reference/settings/merge-tree-settings/allow",
    allow_experimental_replacing_merge_with_cleanup: "/reference/settings/merge-tree-settings/allow-experimental",
    allow_experimental_reverse_key: "/reference/settings/merge-tree-settings/allow-experimental",
    allow_experimental_text_index_phrase_search: "/reference/settings/merge-tree-settings/allow-experimental",
    allow_floating_point_partition_key: "/reference/settings/merge-tree-settings/allow",
    allow_minmax_index_for_json: "/reference/settings/merge-tree-settings/allow",
    allow_nullable_key: "/reference/settings/merge-tree-settings/allow",
    allow_part_offset_column_in_projections: "/reference/settings/merge-tree-settings/allow",
    allow_reduce_blocking_parts_task: "/reference/settings/merge-tree-settings/allow",
    allow_remote_fs_zero_copy_replication: "/reference/settings/merge-tree-settings/allow",
    allow_summing_columns_in_partition_or_order_key: "/reference/settings/merge-tree-settings/allow",
    allow_suspicious_indices: "/reference/settings/merge-tree-settings/allow",
    allow_tuple_element_aggregation: "/reference/settings/merge-tree-settings/allow",
    allow_vertical_merges_from_compact_to_wide_parts: "/reference/settings/merge-tree-settings/allow",
    alter_column_secondary_index_mode: "/reference/settings/merge-tree-settings/other",
    always_fetch_merged_part: "/reference/settings/merge-tree-settings/always",
    always_use_copy_instead_of_hardlinks: "/reference/settings/merge-tree-settings/always",
    apply_patches_on_merge: "/reference/settings/merge-tree-settings/other",
    assign_part_uuids: "/reference/settings/merge-tree-settings/other",
    async_block_ids_cache_update_wait_ms: "/reference/settings/merge-tree-settings/async",
    async_insert: "/reference/settings/merge-tree-settings/async",
    auto_statistics_types: "/reference/settings/merge-tree-settings/other",
    background_task_preferred_step_execution_time_ms: "/reference/settings/merge-tree-settings/other",
    cache_populated_by_fetch: "/reference/settings/merge-tree-settings/cache-populated-by-fetch",
    cache_populated_by_fetch_filename_regexp: "/reference/settings/merge-tree-settings/cache-populated-by-fetch",
    check_delay_period: "/reference/settings/merge-tree-settings/check",
    check_sample_column_is_correct: "/reference/settings/merge-tree-settings/check",
    clean_deleted_rows: "/reference/settings/merge-tree-settings/other",
    cleanup_delay_period: "/reference/settings/merge-tree-settings/cleanup-delay-period",
    cleanup_delay_period_random_add: "/reference/settings/merge-tree-settings/cleanup-delay-period",
    cleanup_thread_preferred_points_per_iteration: "/reference/settings/merge-tree-settings/cleanup",
    cleanup_threads: "/reference/settings/merge-tree-settings/cleanup",
    clone_replica_zookeeper_create_get_part_batch_size: "/reference/settings/merge-tree-settings/other",
    columns_and_secondary_indices_sizes_lazy_calculation: "/reference/settings/merge-tree-settings/columns",
    columns_to_prewarm_mark_cache: "/reference/settings/merge-tree-settings/columns",
    compact_parts_max_bytes_to_buffer: "/reference/settings/merge-tree-settings/compact-parts",
    compact_parts_max_granules_to_buffer: "/reference/settings/merge-tree-settings/compact-parts",
    compact_parts_merge_max_bytes_to_prefetch_part: "/reference/settings/merge-tree-settings/compact-parts",
    compatibility_allow_sampling_expression_not_in_primary_key: "/reference/settings/merge-tree-settings/other",
    compress_marks: "/reference/settings/merge-tree-settings/compress",
    compress_per_column_in_compact_parts: "/reference/settings/merge-tree-settings/compress",
    compress_primary_key: "/reference/settings/merge-tree-settings/compress",
    compute_exact_num_defaults_for_sparse_columns: "/reference/settings/merge-tree-settings/other",
    concurrent_part_removal_threshold: "/reference/settings/merge-tree-settings/concurrent-part-removal-threshold",
    concurrent_part_removal_threshold_for_remote_disk: "/reference/settings/merge-tree-settings/concurrent-part-removal-threshold",
    dead_blobs_to_delay_insert: "/reference/settings/merge-tree-settings/dead-blobs",
    dead_blobs_to_throw_insert: "/reference/settings/merge-tree-settings/dead-blobs",
    deduplicate_merge_projection_mode: "/reference/settings/merge-tree-settings/other",
    deduplication_hashes_cache_update_wait_ms: "/reference/settings/merge-tree-settings/other",
    default_compression_codec: "/reference/settings/merge-tree-settings/other",
    detach_not_byte_identical_parts: "/reference/settings/merge-tree-settings/detach",
    detach_old_local_parts_when_cloning_replica: "/reference/settings/merge-tree-settings/detach",
    disable_detach_partition_for_zero_copy_replication: "/reference/settings/merge-tree-settings/disable",
    disable_fetch_partition_for_zero_copy_replication: "/reference/settings/merge-tree-settings/disable",
    disable_freeze_partition_for_zero_copy_replication: "/reference/settings/merge-tree-settings/disable",
    disk: "/reference/settings/merge-tree-settings/other",
    distributed_index_analysis_min_indexes_bytes_to_activate: "/reference/settings/merge-tree-settings/distributed-index",
    distributed_index_analysis_min_parts_to_activate: "/reference/settings/merge-tree-settings/distributed-index",
    dynamic_serialization_version: "/reference/settings/merge-tree-settings/other",
    enable_block_number_column: "/reference/settings/merge-tree-settings/enable-block",
    enable_block_offset_column: "/reference/settings/merge-tree-settings/enable-block",
    enable_index_granularity_compression: "/reference/settings/merge-tree-settings/enable",
    enable_max_bytes_limit_for_min_age_to_force_merge: "/reference/settings/merge-tree-settings/enable",
    enable_mixed_granularity_parts: "/reference/settings/merge-tree-settings/enable",
    enable_replacing_merge_with_cleanup_for_min_age_to_force_merge: "/reference/settings/merge-tree-settings/enable",
    enable_the_endpoint_id_with_zookeeper_name_prefix: "/reference/settings/merge-tree-settings/enable",
    enable_vertical_merge_algorithm: "/reference/settings/merge-tree-settings/enable",
    enforce_index_structure_match_on_partition_manipulation: "/reference/settings/merge-tree-settings/other",
    escape_index_filenames: "/reference/settings/merge-tree-settings/escape",
    escape_variant_subcolumn_filenames: "/reference/settings/merge-tree-settings/escape",
    exclude_deleted_rows_for_part_size_in_merge: "/reference/settings/merge-tree-settings/exclude",
    exclude_materialize_skip_indexes_on_merge: "/reference/settings/merge-tree-settings/exclude",
    execute_merges_on_single_replica_time_threshold: "/reference/settings/merge-tree-settings/other",
    fault_probability_after_part_commit: "/reference/settings/merge-tree-settings/fault-probability",
    fault_probability_before_part_commit: "/reference/settings/merge-tree-settings/fault-probability",
    finished_mutations_to_keep: "/reference/settings/merge-tree-settings/other",
    force_read_through_cache_for_merges: "/reference/settings/merge-tree-settings/other",
    fsync_after_insert: "/reference/settings/merge-tree-settings/fsync",
    fsync_part_directory: "/reference/settings/merge-tree-settings/fsync",
    in_memory_parts_enable_wal: "/reference/settings/merge-tree-settings/in-memory",
    in_memory_parts_insert_sync: "/reference/settings/merge-tree-settings/in-memory",
    inactive_parts_to_delay_insert: "/reference/settings/merge-tree-settings/inactive-parts",
    inactive_parts_to_throw_insert: "/reference/settings/merge-tree-settings/inactive-parts",
    index_granularity: "/reference/settings/merge-tree-settings/index-granularity",
    index_granularity_bytes: "/reference/settings/merge-tree-settings/index-granularity",
    initialization_retry_period: "/reference/settings/merge-tree-settings/other",
    kill_delay_period: "/reference/settings/merge-tree-settings/kill-delay-period",
    kill_delay_period_random_add: "/reference/settings/merge-tree-settings/kill-delay-period",
    kill_threads: "/reference/settings/merge-tree-settings/other",
    lightweight_mutation_projection_mode: "/reference/settings/merge-tree-settings/other",
    load_existing_rows_count_for_old_parts: "/reference/settings/merge-tree-settings/other",
    lock_acquire_timeout_for_background_operations: "/reference/settings/merge-tree-settings/other",
    map_buckets_coefficient: "/reference/settings/merge-tree-settings/map-buckets",
    map_buckets_min_avg_size: "/reference/settings/merge-tree-settings/map-buckets",
    map_buckets_strategy: "/reference/settings/merge-tree-settings/map-buckets",
    map_serialization_version: "/reference/settings/merge-tree-settings/map-serialization-version",
    map_serialization_version_for_zero_level_parts: "/reference/settings/merge-tree-settings/map-serialization-version",
    marks_compress_block_size: "/reference/settings/merge-tree-settings/marks",
    marks_compression_codec: "/reference/settings/merge-tree-settings/marks",
    materialize_projections_on_insert: "/reference/settings/merge-tree-settings/materialize-projections",
    materialize_projections_on_merge: "/reference/settings/merge-tree-settings/materialize-projections",
    materialize_skip_indexes_on_merge: "/reference/settings/merge-tree-settings/materialize",
    materialize_statistics_on_merge: "/reference/settings/merge-tree-settings/materialize",
    materialize_ttl_recalculate_only: "/reference/settings/merge-tree-settings/materialize",
    max_avg_part_size_for_too_many_parts: "/reference/settings/merge-tree-settings/max",
    max_buckets_in_map: "/reference/settings/merge-tree-settings/max",
    max_bytes_to_merge_at_max_space_in_pool: "/reference/settings/merge-tree-settings/max-bytes",
    max_bytes_to_merge_at_min_space_in_pool: "/reference/settings/merge-tree-settings/max-bytes",
    max_cleanup_delay_period: "/reference/settings/merge-tree-settings/max",
    max_compress_block_size: "/reference/settings/merge-tree-settings/max",
    max_concurrent_queries: "/reference/settings/merge-tree-settings/max",
    max_delay_to_insert: "/reference/settings/merge-tree-settings/max-delay",
    max_delay_to_mutate_ms: "/reference/settings/merge-tree-settings/max-delay",
    max_digestion_size_per_segment: "/reference/settings/merge-tree-settings/max",
    max_file_name_length: "/reference/settings/merge-tree-settings/max",
    max_files_to_modify_in_alter_columns: "/reference/settings/merge-tree-settings/max-files",
    max_files_to_remove_in_alter_columns: "/reference/settings/merge-tree-settings/max-files",
    max_merge_delayed_streams_for_parallel_write: "/reference/settings/merge-tree-settings/max-merge",
    max_merge_selecting_sleep_ms: "/reference/settings/merge-tree-settings/max-merge",
    max_number_of_merges_with_ttl_in_pool: "/reference/settings/merge-tree-settings/max-number",
    max_number_of_mutations_for_replica: "/reference/settings/merge-tree-settings/max-number",
    max_part_loading_threads: "/reference/settings/merge-tree-settings/max-part",
    max_part_removal_threads: "/reference/settings/merge-tree-settings/max-part",
    max_partitions_to_read: "/reference/settings/merge-tree-settings/max",
    max_parts_in_total: "/reference/settings/merge-tree-settings/max-parts",
    max_parts_to_merge_at_once: "/reference/settings/merge-tree-settings/max-parts",
    max_postpone_time_for_failed_mutations_ms: "/reference/settings/merge-tree-settings/max-postpone",
    max_postpone_time_for_failed_replicated_fetches_ms: "/reference/settings/merge-tree-settings/max-postpone",
    max_postpone_time_for_failed_replicated_merges_ms: "/reference/settings/merge-tree-settings/max-postpone",
    max_postpone_time_for_failed_replicated_tasks_ms: "/reference/settings/merge-tree-settings/max-postpone",
    max_projections: "/reference/settings/merge-tree-settings/max",
    max_replicated_fetches_network_bandwidth: "/reference/settings/merge-tree-settings/max-replicated",
    max_replicated_logs_to_keep: "/reference/settings/merge-tree-settings/max-replicated",
    max_replicated_merges_in_queue: "/reference/settings/merge-tree-settings/max-replicated",
    max_replicated_merges_with_ttl_in_queue: "/reference/settings/merge-tree-settings/max-replicated",
    max_replicated_mutations_in_queue: "/reference/settings/merge-tree-settings/max-replicated",
    max_replicated_sends_network_bandwidth: "/reference/settings/merge-tree-settings/max-replicated",
    max_suspicious_broken_parts: "/reference/settings/merge-tree-settings/max-suspicious-broken-parts",
    max_suspicious_broken_parts_bytes: "/reference/settings/merge-tree-settings/max-suspicious-broken-parts",
    max_uncompressed_bytes_in_patches: "/reference/settings/merge-tree-settings/max",
    merge_max_block_size: "/reference/settings/merge-tree-settings/merge-max",
    merge_max_block_size_bytes: "/reference/settings/merge-tree-settings/merge-max",
    merge_max_bytes_to_prewarm_cache: "/reference/settings/merge-tree-settings/merge-max",
    merge_max_dynamic_subcolumns_in_compact_part: "/reference/settings/merge-tree-settings/merge-max",
    merge_max_dynamic_subcolumns_in_wide_part: "/reference/settings/merge-tree-settings/merge-max",
    merge_selecting_sleep_ms: "/reference/settings/merge-tree-settings/merge-selecting",
    merge_selecting_sleep_slowdown_factor: "/reference/settings/merge-tree-settings/merge-selecting",
    merge_selector_algorithm: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_base: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_blurry_base_scale_factor: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_enable_heuristic_to_remove_small_parts_at_right: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent: "/reference/settings/merge-tree-settings/merge-selector",
    merge_selector_window_size: "/reference/settings/merge-tree-settings/merge-selector",
    merge_total_max_bytes_to_prewarm_cache: "/reference/settings/merge-tree-settings/merge",
    merge_tree_clear_old_broken_detached_parts_ttl_timeout_seconds: "/reference/settings/merge-tree-settings/merge-tree",
    merge_tree_clear_old_parts_interval_seconds: "/reference/settings/merge-tree-settings/merge-tree",
    merge_tree_clear_old_temporary_directories_interval_seconds: "/reference/settings/merge-tree-settings/merge-tree",
    merge_tree_enable_clear_old_broken_detached: "/reference/settings/merge-tree-settings/merge-tree",
    merge_with_recompression_ttl_timeout: "/reference/settings/merge-tree-settings/merge-with",
    merge_with_ttl_timeout: "/reference/settings/merge-tree-settings/merge-with",
    merge_workload: "/reference/settings/merge-tree-settings/merge",
    min_absolute_delay_to_close: "/reference/settings/merge-tree-settings/min",
    min_age_to_force_merge_on_partition_only: "/reference/settings/merge-tree-settings/min-age",
    min_age_to_force_merge_seconds: "/reference/settings/merge-tree-settings/min-age",
    min_bytes_for_compact_part: "/reference/settings/merge-tree-settings/min-bytes",
    min_bytes_for_full_part_storage: "/reference/settings/merge-tree-settings/min-bytes",
    min_bytes_for_wide_part: "/reference/settings/merge-tree-settings/min-bytes",
    min_bytes_to_prewarm_caches: "/reference/settings/merge-tree-settings/min-bytes",
    min_bytes_to_rebalance_partition_over_jbod: "/reference/settings/merge-tree-settings/min-bytes",
    min_columns_to_activate_adaptive_write_buffer: "/reference/settings/merge-tree-settings/min",
    min_compress_block_size: "/reference/settings/merge-tree-settings/min",
    min_compressed_bytes_to_fsync_after_fetch: "/reference/settings/merge-tree-settings/min-compressed",
    min_compressed_bytes_to_fsync_after_merge: "/reference/settings/merge-tree-settings/min-compressed",
    min_delay_to_insert_ms: "/reference/settings/merge-tree-settings/min-delay",
    min_delay_to_mutate_ms: "/reference/settings/merge-tree-settings/min-delay",
    min_free_disk_bytes_to_perform_insert: "/reference/settings/merge-tree-settings/min-free",
    min_free_disk_ratio_to_perform_insert: "/reference/settings/merge-tree-settings/min-free",
    min_index_granularity_bytes: "/reference/settings/merge-tree-settings/min",
    min_level_for_full_part_storage: "/reference/settings/merge-tree-settings/min-level",
    min_level_for_wide_part: "/reference/settings/merge-tree-settings/min-level",
    min_marks_to_honor_max_concurrent_queries: "/reference/settings/merge-tree-settings/min",
    min_merge_bytes_to_use_direct_io: "/reference/settings/merge-tree-settings/min",
    min_parts_to_merge_at_once: "/reference/settings/merge-tree-settings/min",
    min_relative_delay_to_close: "/reference/settings/merge-tree-settings/min-relative",
    min_relative_delay_to_measure: "/reference/settings/merge-tree-settings/min-relative",
    min_relative_delay_to_yield_leadership: "/reference/settings/merge-tree-settings/min-relative",
    min_replicated_logs_to_keep: "/reference/settings/merge-tree-settings/min",
    min_rows_for_compact_part: "/reference/settings/merge-tree-settings/min-rows",
    min_rows_for_full_part_storage: "/reference/settings/merge-tree-settings/min-rows",
    min_rows_for_wide_part: "/reference/settings/merge-tree-settings/min-rows",
    min_rows_to_fsync_after_merge: "/reference/settings/merge-tree-settings/min-rows",
    mutation_workload: "/reference/settings/merge-tree-settings/other",
    non_replicated_deduplication_window: "/reference/settings/merge-tree-settings/other",
    notify_newest_block_number: "/reference/settings/merge-tree-settings/other",
    nullable_serialization_version: "/reference/settings/merge-tree-settings/other",
    number_of_free_entries_in_pool_to_execute_mutation: "/reference/settings/merge-tree-settings/number-of",
    number_of_free_entries_in_pool_to_execute_optimize_entire_partition: "/reference/settings/merge-tree-settings/number-of",
    number_of_free_entries_in_pool_to_lower_max_size_of_merge: "/reference/settings/merge-tree-settings/number-of",
    number_of_mutations_to_delay: "/reference/settings/merge-tree-settings/number-of",
    number_of_mutations_to_throw: "/reference/settings/merge-tree-settings/number-of",
    number_of_partitions_to_consider_for_merge: "/reference/settings/merge-tree-settings/number-of",
    object_serialization_version: "/reference/settings/merge-tree-settings/other",
    object_shared_data_buckets_for_compact_part: "/reference/settings/merge-tree-settings/object-shared",
    object_shared_data_buckets_for_wide_part: "/reference/settings/merge-tree-settings/object-shared",
    object_shared_data_serialization_version: "/reference/settings/merge-tree-settings/object-shared",
    object_shared_data_serialization_version_for_zero_level_parts: "/reference/settings/merge-tree-settings/object-shared",
    old_parts_lifetime: "/reference/settings/merge-tree-settings/other",
    optimize_row_order: "/reference/settings/merge-tree-settings/other",
    packed_skip_index_max_bytes: "/reference/settings/merge-tree-settings/other",
    part_minmax_index_columns: "/reference/settings/merge-tree-settings/other",
    part_moves_between_shards_delay_seconds: "/reference/settings/merge-tree-settings/part-moves",
    part_moves_between_shards_enable: "/reference/settings/merge-tree-settings/part-moves",
    parts_to_delay_insert: "/reference/settings/merge-tree-settings/parts-to",
    parts_to_throw_insert: "/reference/settings/merge-tree-settings/parts-to",
    prefer_fetch_merged_part_size_threshold: "/reference/settings/merge-tree-settings/prefer-fetch",
    prefer_fetch_merged_part_time_threshold: "/reference/settings/merge-tree-settings/prefer-fetch",
    prewarm_mark_cache: "/reference/settings/merge-tree-settings/prewarm",
    prewarm_primary_key_cache: "/reference/settings/merge-tree-settings/prewarm",
    primary_key_compress_block_size: "/reference/settings/merge-tree-settings/primary-key",
    primary_key_compression_codec: "/reference/settings/merge-tree-settings/primary-key",
    primary_key_lazy_load: "/reference/settings/merge-tree-settings/primary-key",
    primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns: "/reference/settings/merge-tree-settings/primary-key",
    propagate_types_serialization_versions_to_nested_types: "/reference/settings/merge-tree-settings/other",
    ratio_of_defaults_for_sparse_serialization: "/reference/settings/merge-tree-settings/other",
    reduce_blocking_parts_sleep_ms: "/reference/settings/merge-tree-settings/other",
    refresh_parts_interval: "/reference/settings/merge-tree-settings/refresh",
    refresh_statistics_interval: "/reference/settings/merge-tree-settings/refresh",
    remote_fs_execute_merges_on_single_replica_time_threshold: "/reference/settings/merge-tree-settings/remote-fs",
    remote_fs_zero_copy_path_compatible_mode: "/reference/settings/merge-tree-settings/remote-fs",
    remote_fs_zero_copy_zookeeper_path: "/reference/settings/merge-tree-settings/remote-fs",
    remove_empty_parts: "/reference/settings/merge-tree-settings/remove",
    remove_rolled_back_parts_immediately: "/reference/settings/merge-tree-settings/remove",
    remove_unused_patch_parts: "/reference/settings/merge-tree-settings/remove",
    replace_long_file_name_to_hash: "/reference/settings/merge-tree-settings/other",
    replicated_can_become_leader: "/reference/settings/merge-tree-settings/other",
    replicated_deduplication_window: "/reference/settings/merge-tree-settings/replicated-deduplication-window",
    replicated_deduplication_window_for_async_inserts: "/reference/settings/merge-tree-settings/replicated-deduplication-window",
    replicated_deduplication_window_seconds: "/reference/settings/merge-tree-settings/replicated-deduplication-window",
    replicated_deduplication_window_seconds_for_async_inserts: "/reference/settings/merge-tree-settings/replicated-deduplication-window",
    replicated_fetches_http_connection_timeout: "/reference/settings/merge-tree-settings/replicated-fetches",
    replicated_fetches_http_receive_timeout: "/reference/settings/merge-tree-settings/replicated-fetches",
    replicated_fetches_http_send_timeout: "/reference/settings/merge-tree-settings/replicated-fetches",
    replicated_fetches_min_part_level: "/reference/settings/merge-tree-settings/replicated-fetches",
    replicated_fetches_min_part_level_timeout_seconds: "/reference/settings/merge-tree-settings/replicated-fetches",
    replicated_max_mutations_in_one_entry: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_parallel_fetches: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_parallel_fetches_for_host: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_parallel_fetches_for_table: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_parallel_sends: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_parallel_sends_for_table: "/reference/settings/merge-tree-settings/replicated-max",
    replicated_max_ratio_of_wrong_parts: "/reference/settings/merge-tree-settings/replicated-max",
    search_orphaned_parts_disks: "/reference/settings/merge-tree-settings/other",
    serialization_info_version: "/reference/settings/merge-tree-settings/other",
    share_nested_offsets: "/reference/settings/merge-tree-settings/other",
    shared_merge_tree_activate_coordinated_merges_tasks: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_create_per_replica_metadata_nodes: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_disable_merges_and_mutations_assignment: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_empty_partition_lifetime: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_enable_automatic_empty_partitions_cleanup: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_enable_coordinated_merges: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_enable_keeper_parts_extra_data: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_enable_outdated_parts_check: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_idle_parts_update_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_inactive_replica_cutoff_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_initial_parts_update_backoff_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_interserver_http_connection_timeout_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_interserver_http_timeout_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_leader_update_period_random_add_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_leader_update_period_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_outdated_parts_to_process_at_once: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_parts_update_backoff_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_parts_update_leaders_in_total: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_parts_update_leaders_per_az: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_replicas_for_parts_deletion: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_suspicious_broken_parts: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_max_suspicious_broken_parts_bytes: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_memo_ids_remove_timeout_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_distribution_algorithm: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_election_check_period_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_factor: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_max_merge_request_size: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_max_period_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_merges_prepare_count: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_coordinator_min_period_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_worker_fast_timeout_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_merge_worker_regular_timeout_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_outdated_parts_group_size: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_partitions_hint_ratio_to_reload_merge_pred_for_mutations: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_parts_load_batch_size: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_range_for_merge_window_size: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_read_virtual_parts_from_leader: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_replica_set_max_lifetime_seconds: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_try_fetch_part_in_memory_data_from_replicas: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_try_fetch_part_in_memory_data_from_replicas_on_startup: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_update_replica_flags_delay_ms: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_use_metadata_hints_cache: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_use_outdated_parts_compact_format: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_use_too_many_parts_count_from_virtual_parts: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_use_zookeeper_connection_pool: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_virtual_parts_discovery_batch: "/reference/settings/merge-tree-settings/shared-merge",
    shared_merge_tree_virtual_parts_partition_atomic_discovery: "/reference/settings/merge-tree-settings/shared-merge",
    simultaneous_parts_removal_limit: "/reference/settings/merge-tree-settings/other",
    sleep_before_commit_local_part_in_replicated_table_ms: "/reference/settings/merge-tree-settings/sleep-before",
    sleep_before_loading_outdated_parts_ms: "/reference/settings/merge-tree-settings/sleep-before",
    storage_policy: "/reference/settings/merge-tree-settings/other",
    string_serialization_version: "/reference/settings/merge-tree-settings/other",
    table_disk: "/reference/settings/merge-tree-settings/table",
    table_readonly: "/reference/settings/merge-tree-settings/table",
    temporary_directories_lifetime: "/reference/settings/merge-tree-settings/other",
    text_index_dictionary_block_frontcoding_compression: "/reference/settings/merge-tree-settings/text-index",
    text_index_dictionary_block_size: "/reference/settings/merge-tree-settings/text-index",
    text_index_posting_list_block_size: "/reference/settings/merge-tree-settings/text-index",
    text_index_posting_list_codec: "/reference/settings/merge-tree-settings/text-index",
    try_fetch_recompressed_part_timeout: "/reference/settings/merge-tree-settings/other",
    ttl_only_drop_parts: "/reference/settings/merge-tree-settings/other",
    use_adaptive_write_buffer_for_dynamic_subcolumns: "/reference/settings/merge-tree-settings/use",
    use_async_block_ids_cache: "/reference/settings/merge-tree-settings/use",
    use_compact_variant_discriminators_serialization: "/reference/settings/merge-tree-settings/use",
    use_const_adaptive_granularity: "/reference/settings/merge-tree-settings/use",
    use_metadata_cache: "/reference/settings/merge-tree-settings/use",
    use_minimalistic_checksums_in_zookeeper: "/reference/settings/merge-tree-settings/use-minimalistic",
    use_minimalistic_part_header_in_zookeeper: "/reference/settings/merge-tree-settings/use-minimalistic",
    use_primary_key_cache: "/reference/settings/merge-tree-settings/use",
    vertical_merge_algorithm_min_bytes_to_activate: "/reference/settings/merge-tree-settings/vertical-merge",
    vertical_merge_algorithm_min_columns_to_activate: "/reference/settings/merge-tree-settings/vertical-merge",
    vertical_merge_algorithm_min_rows_to_activate: "/reference/settings/merge-tree-settings/vertical-merge",
    vertical_merge_optimize_lightweight_delete: "/reference/settings/merge-tree-settings/vertical-merge",
    vertical_merge_optimize_ttl_delete: "/reference/settings/merge-tree-settings/vertical-merge",
    vertical_merge_remote_filesystem_prefetch: "/reference/settings/merge-tree-settings/vertical-merge",
    wait_for_unique_parts_send_before_shutdown_ms: "/reference/settings/merge-tree-settings/other",
    write_ahead_log_bytes_to_fsync: "/reference/settings/merge-tree-settings/write-ahead",
    write_ahead_log_interval_ms_to_fsync: "/reference/settings/merge-tree-settings/write-ahead",
    write_ahead_log_max_bytes: "/reference/settings/merge-tree-settings/write-ahead",
    write_final_mark: "/reference/settings/merge-tree-settings/write",
    write_marks_for_substreams_in_compact_parts: "/reference/settings/merge-tree-settings/write",
    zero_copy_concurrent_part_removal_max_postpone_ratio: "/reference/settings/merge-tree-settings/zero-copy",
    zero_copy_concurrent_part_removal_max_split_times: "/reference/settings/merge-tree-settings/zero-copy",
    zero_copy_merge_mutation_min_parts_size_sleep_before_lock: "/reference/settings/merge-tree-settings/zero-copy",
    zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock: "/reference/settings/merge-tree-settings/zero-copy",
    zookeeper_session_expiration_check_period: "/reference/settings/merge-tree-settings/other"
  }))

  useEffect(() => {
    const rawHash = window.location.hash.slice(1)
    if (!rawHash) return

    let decodedHash
    try {
      decodedHash = decodeURIComponent(rawHash)
    } catch {
      return
    }

    const canonicalAnchor = (value) => {
      const candidates = [value, value.replace(/[?,;:!'"()[\]{}]/g, "")]
      for (const candidate of candidates) {
        if (anchorRoutes[candidate]) return candidate
        const lowerValue = candidate.toLowerCase()
        if (anchorRoutes[lowerValue]) return lowerValue
      }
      return null
    }

    const directAnchor = canonicalAnchor(decodedHash)
    const baseAnchor = directAnchor || canonicalAnchor(decodedHash.split("-", 1)[0])
    if (!baseAnchor) return
    const target = anchorRoutes[baseAnchor]
    const targetHash = directAnchor || rawHash

    const marker = "/reference/settings/merge-tree-settings"
    const markerIndex = window.location.pathname.indexOf(marker)
    let basePath = ""
    if (markerIndex >= 0) {
      basePath = window.location.pathname.slice(0, markerIndex)
    } else if (window.location.pathname.startsWith("/docs/")) {
      basePath = "/docs"
    }

    window.location.replace(`${basePath}${target}${window.location.search}#${targetHash}`)
  }, [])

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

  const toggleGroup = (key) => {
    setExpandedGroups((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const branchPrefix = (continuations, isLast) => {
    const prefix = continuations.map((continued) => (continued ? "│  " : "   ")).join("")
    return `${prefix}${isLast ? "└─ " : "├─ "}`
  }

  const branch = (value) => (
    <span aria-hidden="true" className="select-none text-gray-400 dark:text-gray-600" style={{ whiteSpace: "pre" }}>
      {value}
    </span>
  )

  const renderGroup = (entry, continuations = [], isLast = false, path = []) => {
    const key = [...path, entry.label].join("/")
    const isOpen = isSearching || expandedGroups.has(key)
    const items = [...entry.settings.map((setting) => ({ type: "setting", value: setting })), ...entry.children.map((child) => ({ type: "group", value: child }))]
    const countLabel = `${entry.count} ${entry.count === 1 ? "настройка" : entry.count >= 2 && entry.count <= 4 ? "настройки" : "настроек"}`

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
              <div key={item.value.name} className="flex min-w-max items-baseline whitespace-nowrap">
                <span aria-hidden="true" style={{ display: "inline-block", width: "1rem" }} />
                {branch(branchPrefix(childContinuations, itemIsLast))}
                <a href={item.value.href} className="no-underline hover:underline">
                  {item.value.name}
                </a>
              </div>
            )
          })}
      </div>
    )
  }

  return (
    <div className="not-prose my-6 w-full">
      <input
        aria-label="Поиск настроек"
        type="search"
        value={searchTerm}
        onChange={(event) => setSearchTerm(event.target.value)}
        placeholder="Поиск настроек, например: parallel replicas или %materialized%"
        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 outline-none placeholder:text-gray-500 focus:border-yellow-400 focus:ring-2 focus:ring-yellow-400/20 dark:border-white/10 dark:bg-transparent dark:text-white dark:placeholder:text-gray-500"
      />
      {isSearching && (
        <div className="mt-2 text-right text-xs text-gray-500 dark:text-gray-400">
          <span>
            {matchingCount === 1 ? "Найдена" : "Найдено"} {matchingCount} {matchingCount === 1 ? "настройка" : matchingCount >= 2 && matchingCount <= 4 ? "настройки" : "настроек"}
          </span>
        </div>
      )}
      <div className="mt-3 w-full overflow-x-auto rounded-xl border border-gray-200 bg-gray-50/50 px-4 py-3 font-mono text-sm leading-6 dark:border-white/10 dark:bg-transparent">
        <div className="min-w-max font-semibold">/merge-tree-settings</div>
        {filteredEntries.length > 0 ? (
          filteredEntries.map((entry, index) => renderGroup(entry, [], index === filteredEntries.length - 1))
        ) : (
          <div className="py-2 text-gray-500 dark:text-gray-400">Настройки не найдены</div>
        )}
      </div>
    </div>
  )
}

export default MergeTreeSettingsExplorer;