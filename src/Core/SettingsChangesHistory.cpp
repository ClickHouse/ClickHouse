#include <Core/SettingsChangesHistory.h>

#include <Core/SettingsEnums.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

static void addSettingsChanges(
    VersionToSettingsChangesMap & settings_changes_history,
    std::string_view version,
    SettingsChangesHistory::SettingsChanges && changes)
{
    /// Forbid duplicate versions
    auto [_, inserted] = settings_changes_history.emplace(ClickHouseVersion(version), std::move(changes));
    if (!inserted)
        throw Exception{ErrorCodes::LOGICAL_ERROR, "Detected duplicate version '{}'", ClickHouseVersion(version).toString()};
}

const VersionToSettingsChangesMap & getSettingsChangesHistory()
{
    static VersionToSettingsChangesMap settings_changes_history;
    static std::once_flag initialized_flag;
    std::call_once(initialized_flag, [&]
    {
        // clang-format off
        /// History of settings changes that controls some backward incompatible changes
        /// across all ClickHouse versions. It maps ClickHouse version to settings changes that were done
        /// in this version. This history contains both changes to existing settings and newly added settings.
        /// Settings changes is a vector of structs
        ///     {setting_name, previous_value, new_value, reason}.
        /// For newly added setting choose the most appropriate previous_value (for example, if new setting
        /// controls new feature and it's 'true' by default, use 'false' as previous_value).
        /// It's used to implement `compatibility` setting (see https://github.com/ClickHouse/ClickHouse/issues/35972)
        /// Note: please check if the key already exists to prevent duplicate entries.
        addSettingsChanges(settings_changes_history, "26.3",
        {
            {"allow_calculating_subcolumns_sizes_for_merge_tree_reading", false, true, "Allow calculating subcolumns sizes for merge tree reading to improve read tasks splitting"},
            {"delta_lake_reload_schema_for_consistency", false, false, "New setting to control whether DeltaLake reloads schema before each query for consistency."},
            {"use_partition_pruning", true, true, "New setting controlling whether MergeTree uses partition key for pruning. 'use_partition_key' is an alias for this setting."},
            {"use_partition_key", true, true, "Alias for setting 'use_partition_pruning'."},
        });
        addSettingsChanges(settings_changes_history, "26.2",
        {
            {"allow_fuzz_query_functions", false, false, "New setting to enable the fuzzQuery function."},
            {"ast_fuzzer_runs", 0, 0, "New setting to enable server-side AST fuzzer."},
            {"ast_fuzzer_any_query", false, false, "New setting to allow fuzzing all query types, not just read-only."},
            {"check_named_collection_dependencies", true, true, "New setting to check if dropping a named collection would break dependent tables."},
            {"deduplicate_blocks_in_dependent_materialized_views", false, true, "Enable deduplication for dependent materialized views by default."},
            {"deduplicate_insert", "backward_compatible_choice", "enable", "Enable deduplication for all sync and async inserts by default."},
            {"deduplicate_insert", "backward_compatible_choice", "backward_compatible_choice", "New setting to control deduplication for INSERT queries."},
            {"enable_join_runtime_filters", false, true, "Enabled this optimization"},
            {"parallel_replicas_filter_pushdown", false, false, "New setting"},
            {"optimize_dry_run_check_part", true, true, "New setting"},
            {"parallel_non_joined_rows_processing", true, true, "New setting to enable parallel processing of non-joined rows in RIGHT/FULL parallel_hash joins."},
            {"enable_automatic_decision_for_merging_across_partitions_for_final", true, true, "New setting"},
            {"enable_full_text_index", false, true, "The text index is now GA"},
            {"allow_experimental_full_text_index", false, true, "The text index is now GA"},
            {"use_page_cache_for_local_disks", false, false, "New setting to use userspace page cache for local disks"},
            {"use_page_cache_for_object_storage", false, false, "New setting to use userspace page cache for object storage table functions"},
            {"use_statistics_cache", false, true, "Enable statistics cache"},
            {"apply_row_policy_after_final", false, true, "Enabling apply_row_policy_after_final by default, as if was in 25.8 before #87303"},
            {"ignore_format_null_for_explain", false, true, "FORMAT Null is now ignored for EXPLAIN queries by default"},
            {"input_format_connection_handling", false, false, "New setting to allow parsing and processing remaining data in the buffer if the connection closes unexpectedly"},
            {"input_format_max_block_wait_ms", 0, 0, "New setting to limit maximum wait time in milliseconds before a block is emitted by input format"},
            {"allow_insert_into_iceberg", false, false, "Insert into iceberg was moved to Beta"},
            {"allow_experimental_insert_into_iceberg", false, false, "Insert into iceberg was moved to Beta"},
            {"output_format_arrow_date_as_uint16", true, false, "Write Date as Arrow DATE32 instead of plain UInt16 by default."},
            {"jemalloc_profile_text_output_format", "collapsed", "collapsed", "New setting to control output format for system.jemalloc_profile_text table. Possible values: 'raw', 'symbolized', 'collapsed'"},
            {"jemalloc_profile_text_symbolize_with_inline", true, true, "New setting to control whether to include inline frames when symbolizing jemalloc heap profile. When enabled, inline frames are included at the cost of slower symbolization; when disabled, they are skipped for faster output"},
            {"jemalloc_profile_text_collapsed_use_count", false, false, "New setting to aggregate by allocation count instead of bytes in the collapsed jemalloc heap profile format"},
            {"opentelemetry_start_keeper_trace_probability", "auto", "auto", "New setting"},
        });
        addSettingsChanges(settings_changes_history, "26.1",
        {
            {"use_statistics", true, true, "Enable this optimization by default."},
            {"ignore_on_cluster_for_replicated_database", false, false, "Add a new setting to ignore ON CLUSTER clause for DDL queries with a replicated database."},
            {"input_format_binary_max_type_complexity", 1000, 1000, "Add a new setting to control max number of type nodes when decoding binary types. Protects against malicious inputs."},
            {"distributed_index_analysis", false, false, "New experimental setting"},
            {"distributed_index_analysis_for_non_shared_merge_tree", false, false, "New setting"},
            {"distributed_cache_file_cache_name", "", "", "New setting."},
            {"trace_profile_events_list", "", "", "New setting"},
            {"query_plan_reuse_storage_ordering_for_window_functions", true, false, "Disable this logic by default."},
            {"optimize_read_in_window_order", true, false, "Disable this logic by default."},
            {"correlated_subqueries_use_in_memory_buffer", false, true, "Use in-memory buffer for input of correlated subqueries by default."},
            {"allow_experimental_database_paimon_rest_catalog", false, false, "New setting"},
            {"allow_experimental_object_storage_queue_hive_partitioning", false, false, "New setting."},
            {"type_json_use_partial_match_to_skip_paths_by_regexp", false, true, "Add new setting that allows to use partial match in regexp paths skip in JSON type parsing"},
            {"max_insert_block_size_bytes", 0, 0, "New setting that allows to control the size of blocks in bytes during parsing of data in Row Input Format."},
            {"max_insert_block_size_rows", DEFAULT_INSERT_BLOCK_SIZE, DEFAULT_INSERT_BLOCK_SIZE, "An alias for max_insert_block_size."},
            {"join_runtime_filter_pass_ratio_threshold_for_disabling", 0.7, 0.7, "New setting"},
            {"join_runtime_filter_blocks_to_skip_before_reenabling", 30, 30, "New setting"},
            {"use_join_disjunctions_push_down", false, true, "Enabled this optimization."},
            {"join_runtime_bloom_filter_max_ratio_of_set_bits", 0.7, 0.7, "New setting"},
            {"check_conversion_from_numbers_to_enum", false, true, "New setting"},
            {"allow_experimental_nullable_tuple_type", false, false, "New experimental setting"},
            {"use_skip_indexes_on_data_read", false, true, "Default enable"},
            {"check_conversion_from_numbers_to_enum", false, false, "New setting"},
            {"archive_adaptive_buffer_max_size_bytes", 8 * 1024 * 1024, 8 * 1024 * 1024, "New setting"},
            {"type_json_allow_duplicated_key_with_literal_and_nested_object", false, false, "Add a new setting to allow duplicated paths in JSON type with literal and nested object"},
            {"use_primary_key", true, true, "New setting controlling whether MergeTree uses the primary key for granule-level pruning."},
            {"deduplicate_insert_select", "enable_even_for_bad_queries", "enable_when_possible", "change the default behavior of deduplicate_insert_select to ENABLE_WHEN_POSSIBLE"},
            {"allow_experimental_qbit_type", false, true, "QBit was moved to Beta"},
            {"enable_qbit_type", false, true, "QBit was moved to Beta. Added an alias for setting 'allow_experimental_qbit_type'."},
            {"use_variant_default_implementation_for_comparisons", false, true, "Enable default implementation for Variant type in comparison functions"},
            {"use_variant_as_common_type", false, true, "Improves usability."},
            {"max_dynamic_subcolumns_in_json_type_parsing", "auto", "auto", "Add a new setting to limit number of dynamic subcolumns during JSON type parsing regardless the parameters specified in the data type"},
            {"use_hash_table_stats_for_join_reordering", true, true, "New setting. Previously mirrored 'collect_hash_table_stats_during_joins' setting."},
            {"throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert", true, false, "It becomes obsolete."},
            {"database_datalake_require_metadata_access", true, true, "New setting."},
            {"automatic_parallel_replicas_min_bytes_per_replica", 0, 1_MiB, "Better default value derived from testing results"},
        });
        addSettingsChanges(settings_changes_history, "25.12",
        {
            {"format_binary_max_object_size", 100000, 100000, "New setting that limits the maximum size of object during JSON type binary deserialization"},
            {"max_streams_for_files_processing_in_cluster_functions", 0, 0, "Add a new setting that allows to limit number of streams for files processing in *Cluster table functions"},
            {"max_reverse_dictionary_lookup_cache_size_bytes", 100 * 1024 * 1024, 100 * 1024 * 1024, "New setting. Maximum size in bytes of the per-query reverse dictionary lookup cache used by the function `dictGetKeys`. The cache stores serialized key tuples per attribute value to avoid re-scanning the dictionary within the same query."},
            {"query_plan_remove_unused_columns", false, true, "New setting. Add optimization to remove unused columns in query plan."},
            {"query_plan_optimize_join_order_limit", 1, 10, "Allow JOIN reordering with more tables by default"},
            {"iceberg_insert_max_partitions", 100, 100, "New setting."},
            {"check_query_single_value_result", true, false, "Changed setting to make CHECK TABLE more useful"},
            {"use_paimon_partition_pruning", false, false, "New setting."},
            {"use_skip_indexes_for_disjunctions", false, true, "New setting"},
            {"allow_statistics_optimize", false, true, "Enable this optimization by default."},
            {"allow_statistic_optimize", false, true, "Enable this optimization by default."},
            {"query_plan_text_index_add_hint", true, true, "New setting"},
            {"query_plan_read_in_order_through_join", false, true, "New setting"},
            {"query_plan_max_limit_for_lazy_materialization", 10, 10000, "Increase the limit after performance improvement"},
            {"text_index_hint_max_selectivity", 0.2, 0.2, "New setting"},
            {"allow_experimental_time_time64_type", false, true, "Enable Time and Time64 type by default"},
            {"enable_time_time64_type", false, true, "Enable Time and Time64 type by default"},
            {"use_skip_indexes_for_top_k", false, false, "New setting."},
            {"use_top_k_dynamic_filtering", false, false, "New setting."},
            {"query_plan_max_limit_for_top_k_optimization", 0, 1000, "New setting."},
            {"aggregate_function_input_format", "state", "state", "New setting to control AggregateFunction input format during INSERT operations. Setting Value set to state by default"},
            {"delta_lake_snapshot_start_version", -1, -1, "New setting."},
            {"delta_lake_snapshot_end_version", -1, -1, "New setting."},
            {"apply_row_policy_after_final", false, false, "New setting to control if row policies and PREWHERE are applied after FINAL processing for *MergeTree tables"},
            {"apply_prewhere_after_final", false, false, "New setting. When enabled, PREWHERE conditions are applied after FINAL processing."},
            {"compatibility_s3_presigned_url_query_in_path", false, false, "New setting."},
            {"serialize_string_in_memory_with_zero_byte", true, true, "New setting"},
            {"optimize_inverse_dictionary_lookup", false, true, "New setting"},
            {"automatic_parallel_replicas_mode", 0, 0, "New setting"},
            {"automatic_parallel_replicas_min_bytes_per_replica", 0, 0, "New setting"},
            {"type_json_skip_invalid_typed_paths", false, false, "Allow skipping typed paths that fail type coercion in JSON columns"},
            {"query_plan_optimize_join_order_algorithm", "greedy", "greedy", "New experimental setting."},
            {"s3_path_filter_limit", 0, 1000, "New setting"},
            {"format_capn_proto_max_message_size", 0, 1_GiB, "Prevent allocating large amount of memory"},
            {"parallel_replicas_allow_materialized_views", false, true, "Allow usage of materialized views with parallel replicas"},
            {"distributed_cache_use_clients_cache_for_read", true, true, "New setting"},
            {"distributed_cache_use_clients_cache_for_write", false, false, "New setting"},
            {"enable_positional_arguments_for_projections", true, false, "New setting to control positional arguments in projections."},
            {"enable_full_text_index", false, false, "Text index was moved to Beta."},
            {"enable_shared_storage_snapshot_in_query", false, true, "Enable share storage snapshot in query by default"},
            {"insert_select_deduplicate", Field{"auto"}, Field{"auto"}, "New setting"},
            {"output_format_pretty_named_tuples_as_json", false, true, "New setting to control whether named tuples in Pretty format are output as JSON objects"},
            {"deduplicate_insert_select", "enable_even_for_bad_queries", "enable_even_for_bad_queries", "New setting, replace insert_select_deduplicate"},

        });
        addSettingsChanges(settings_changes_history, "25.11",
        {
            {"query_plan_max_limit_for_lazy_materialization", 10, 100, "More optimal"},
            {"create_table_empty_primary_key_by_default", false, true, "Better usability"},
            {"cluster_table_function_split_granularity", "file", "file", "New setting."},
            {"cluster_table_function_buckets_batch_size", 0, 0, "New setting."},
            {"arrow_flight_request_descriptor_type", "path", "path", "New setting. Type of descriptor to use for Arrow Flight requests: 'path' or 'command'. Dremio requires 'command'."},
            {"send_profile_events", true, true, "New setting. Whether to send profile events to the clients."},
            {"into_outfile_create_parent_directories", false, false, "New setting"},
            {"correlated_subqueries_default_join_kind", "left", "right", "New setting. Default join kind for decorrelated query plan."},
            {"use_statistics_cache", 0, 0, "New setting"},
            {"input_format_parquet_use_native_reader_v3", false, true, "Seems stable"},
            {"max_projection_rows_to_use_projection_index", 1'000'000, 1'000'000, "New setting"},
            {"min_table_rows_to_use_projection_index", 1'000'000, 1'000'000, "New setting"},
            {"use_text_index_dictionary_cache", false, false, "New setting"},
            {"use_text_index_header_cache", false, false, "New setting"},
            {"use_text_index_postings_cache", false, false, "New setting"},
            {"s3_retry_attempts", 500, 500, "Changed the value of the obsolete setting"},
            {"http_write_exception_in_output_format", true, false, "Changed for consistency across formats"},
            {"optimize_const_name_size", -1, 256, "Replace with scalar and use hash as a name for large constants (size is estimated by name length)"},
            {"enable_lazy_columns_replication", false, true, "Enable lazy columns replication in JOIN and ARRAY JOIN by default"},
            {"allow_special_serialization_kinds_in_output_formats", false, true, "Enable direct output of special columns representations like Sparse/Replicated in some output formats"},
            {"allow_experimental_alias_table_engine", false, false, "New setting"},
            {"input_format_parquet_local_time_as_utc", false, true, "Use more appropriate type DateTime64(..., 'UTC') for parquet 'local time without timezone' type."},
            {"input_format_parquet_verify_checksums", true, true, "New setting."},
            {"output_format_parquet_write_checksums", false, true, "New setting."},
            {"database_shared_drop_table_delay_seconds", 8 * 60 * 60, 8 * 60 * 60, "New setting."},
            {"filesystem_cache_allow_background_download", true, true, "New setting to control background downloads in filesystem cache per query."},
            {"show_processlist_include_internal", false, true, "New setting."},
            {"enable_positional_arguments_for_projections", true, false, "New setting to control positional arguments in projections."},
        });
        addSettingsChanges(settings_changes_history, "25.10",
        {
            {"allow_special_serialization_kinds_in_output_formats", false, false, "Add a setting to allow output of special columns representations like Sparse/Replicated without converting them to full columns"},
            {"enable_lazy_columns_replication", false, false, "Add a setting to enable lazy columns replication in JOIN and ARRAY JOIN"},
            {"correlated_subqueries_default_join_kind", "left", "right", "New setting. Default join kind for decorrelated query plan."},
            {"show_data_lake_catalogs_in_system_tables", true, false, "Disable catalogs in system tables by default"},
            {"optimize_rewrite_like_perfect_affix", false, true, "New setting"},
            {"allow_dynamic_type_in_join_keys", true, false, "Disallow using Dynamic type in JOIN keys by default"},
            {"s3queue_keeper_fault_injection_probability", 0, 0, "New setting."},
            {"enable_join_runtime_filters", false, false, "New setting"},
            {"join_runtime_filter_exact_values_limit", 10000, 10000, "New setting"},
            {"join_runtime_bloom_filter_bytes", 512_KiB, 512_KiB, "New setting"},
            {"join_runtime_bloom_filter_hash_functions", 3, 3, "New setting"},
            {"use_join_disjunctions_push_down", false, false, "New setting."},
            {"joined_block_split_single_row", false, false, "New setting"},
            {"temporary_files_buffer_size", DBMS_DEFAULT_BUFFER_SIZE, DBMS_DEFAULT_BUFFER_SIZE, "New setting"},
            {"rewrite_in_to_join", false, false, "New experimental setting"},
            {"delta_lake_log_metadata", false, false, "New setting."},
            {"distributed_cache_prefer_bigger_buffer_size", false, false, "New setting."},
            {"allow_experimental_qbit_type", false, false, "New experimental setting"},
            {"optimize_qbit_distance_function_reads", true, true, "New setting"},
            {"read_from_distributed_cache_if_exists_otherwise_bypass_cache", false, false, "New setting"},
            {"s3_slow_all_threads_after_retryable_error", false, false, "Disable the setting by default"},
            {"backup_slow_all_threads_after_retryable_s3_error", false, false, "Disable the setting by default"},
            {"enable_http_compression", false, true, "It should be beneficial in general"},
            {"inject_random_order_for_select_without_order_by", false, false, "New setting"},
            {"exclude_materialize_skip_indexes_on_insert", "", "", "New setting."},
            {"optimize_empty_string_comparisons", false, true, "A new setting."},
            {"query_plan_use_logical_join_step", true, true, "Added alias"},
            {"schema_inference_make_columns_nullable", 1, 3, "Take nullability information from Parquet/ORC/Arrow metadata by default, instead of making everything nullable."},
            {"materialized_views_squash_parallel_inserts", false, true, "Added setting to preserve old behavior if needed."},
            {"distributed_cache_connect_timeout_ms", 50, 50, "New setting"},
            {"distributed_cache_receive_timeout_ms", 3000, 3000, "New setting"},
            {"distributed_cache_send_timeout_ms", 3000, 3000, "New setting"},
            {"distributed_cache_tcp_keep_alive_timeout_ms", 2900, 2900, "New setting"},
            {"enable_positional_arguments_for_projections", true, false, "New setting to control positional arguments in projections."},
        });
        addSettingsChanges(settings_changes_history, "25.9",
        {
            {"input_format_protobuf_oneof_presence", false, false, "New setting"},
            {"iceberg_delete_data_on_drop", false, false, "New setting"},
            {"use_skip_indexes_on_data_read", false, false, "New setting"},
            {"s3_slow_all_threads_after_retryable_error", false, false, "Added an alias for setting `backup_slow_all_threads_after_retryable_s3_error`"},
            {"iceberg_metadata_log_level", "none", "none", "New setting."},
            {"iceberg_insert_max_rows_in_data_file", 1000000, 1000000, "New setting."},
            {"iceberg_insert_max_bytes_in_data_file", 1_GiB, 1_GiB, "New setting."},
            {"query_plan_optimize_join_order_limit", 1, 1, "New setting"},
            {"query_plan_display_internal_aliases", false, false, "New setting"},
            {"query_plan_max_step_description_length", 1000000000, 500, "New setting"},
            {"allow_experimental_delta_lake_writes", false, false, "New setting."},
            {"query_plan_convert_any_join_to_semi_or_anti_join", true, true, "New setting."},
            {"text_index_use_bloom_filter", true, true, "New setting."},
            {"query_plan_direct_read_from_text_index", true, true, "New setting."},
            {"enable_producing_buckets_out_of_order_in_aggregation", false, true, "New setting"},
            {"jemalloc_enable_profiler", false, false, "New setting"},
            {"jemalloc_collect_profile_samples_in_trace_log", false, false, "New setting"},
            {"delta_lake_insert_max_bytes_in_data_file", 1_GiB, 1_GiB, "New setting."},
            {"delta_lake_insert_max_rows_in_data_file", 1000000, 1000000, "New setting."},
            {"promql_evaluation_time", Field{"auto"}, Field{"auto"}, "The setting was renamed. The previous name is `evaluation_time`."},
            {"evaluation_time", 0, 0, "Old setting which popped up here being renamed."},
            {"os_threads_nice_value_query", 0, 0, "New setting."},
            {"os_threads_nice_value_materialized_view", 0, 0, "New setting."},
            {"os_thread_priority", 0, 0, "Alias for os_threads_nice_value_query."},
        });
        addSettingsChanges(settings_changes_history, "25.8",
        {
            {"output_format_json_quote_64bit_integers", true, false, "Disable quoting of the 64 bit integers in JSON by default"},
            {"show_data_lake_catalogs_in_system_tables", true, true, "New setting"},
            {"optimize_rewrite_regexp_functions", false, true, "A new setting"},
            {"max_joined_block_size_bytes", 0, 4 * 1024 * 1024, "New setting"},
            {"azure_max_single_part_upload_size", 100 * 1024 * 1024, 32 * 1024 * 1024, "Align with S3"},
            {"azure_max_redirects", 10, 10, "New setting"},
            {"azure_max_get_rps", 0, 0, "New setting"},
            {"azure_max_get_burst", 0, 0, "New setting"},
            {"azure_max_put_rps", 0, 0, "New setting"},
            {"azure_max_put_burst", 0, 0, "New setting"},
            {"azure_use_adaptive_timeouts", true, true, "New setting"},
            {"azure_request_timeout_ms", 30000, 30000, "New setting"},
            {"azure_connect_timeout_ms", 1000, 1000, "New setting"},
            {"azure_sdk_use_native_client", false, true, "New setting"},
            {"analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested", false, true, "New setting."},
            {"distributed_cache_connect_backoff_min_ms", 0, 0, "New setting"},
            {"distributed_cache_connect_backoff_max_ms", 50, 50, "New setting"},
            {"distributed_cache_read_request_max_tries", 20, 10, "Changed setting value"},
            {"distributed_cache_connect_max_tries", 20, 5, "Changed setting value"},
            {"opentelemetry_trace_cpu_scheduling", false, false, "New setting to trace `cpu_slot_preemption` feature."},
            {"output_format_parquet_max_dictionary_size", 1024 * 1024, 1024 * 1024, "New setting"},
            {"input_format_parquet_use_native_reader_v3", false, false, "New setting"},
            {"input_format_parquet_memory_low_watermark", 2ul << 20, 2ul << 20, "New setting"},
            {"input_format_parquet_memory_high_watermark", 4ul << 30, 4ul << 30, "New setting"},
            {"input_format_parquet_page_filter_push_down", true, true, "New setting (no effect when input_format_parquet_use_native_reader_v3 is disabled)"},
            {"input_format_parquet_use_offset_index", true, true, "New setting (no effect when input_format_parquet_use_native_reader_v3 is disabled)"},
            {"output_format_parquet_enum_as_byte_array", false, true, "Enable writing Enum as byte array in Parquet by default"},
            {"json_type_escape_dots_in_keys", false, false, "Add new setting that allows to escape dots in JSON keys during JSON type parsing"},
            {"parallel_replicas_support_projection", false, true, "New setting. Optimization of projections can be applied in parallel replicas. Effective only with enabled parallel_replicas_local_plan and aggregation_in_order is inactive."},
            {"input_format_json_infer_array_of_dynamic_from_array_of_different_types", false, true, "Infer Array(Dynamic) for JSON arrays with different values types by default"},
            {"enable_add_distinct_to_in_subqueries", false, false, "New setting to reduce the size of temporary tables transferred for distributed IN subqueries."},
            {"enable_vector_similarity_index", false, true, "Vector similarity indexes are GA."},
            {"execute_exists_as_scalar_subquery", false, true, "New setting"},
            {"allow_experimental_vector_similarity_index", false, true, "Vector similarity indexes are GA."},
            {"vector_search_with_rescoring", true, false, "New setting."},
            {"delta_lake_enable_expression_visitor_logging", false, false, "New setting"},
            {"write_full_path_in_iceberg_metadata", false, false, "New setting."},
            {"output_format_orc_compression_block_size", 65536, 262144, "New setting"},
            {"allow_database_iceberg", false, false, "Added an alias for setting `allow_experimental_database_iceberg`"},
            {"allow_database_unity_catalog", false, false, "Added an alias for setting `allow_experimental_database_unity_catalog`"},
            {"allow_database_glue_catalog", false, false, "Added an alias for setting `allow_experimental_database_glue_catalog`"},
            {"apply_patch_parts_join_cache_buckets", 8, 8, "New setting"},
            {"delta_lake_throw_on_engine_predicate_error", false, false, "New setting"},
            {"delta_lake_enable_engine_predicate", true, true, "New setting"},
            {"backup_restore_s3_retry_initial_backoff_ms", 25, 25, "New setting"},
            {"backup_restore_s3_retry_max_backoff_ms", 5000, 5000, "New setting"},
            {"backup_restore_s3_retry_jitter_factor", 0.0, 0.1, "New setting"},
            {"vector_search_index_fetch_multiplier", 1.0, 1.0, "Alias for setting 'vector_search_postfilter_multiplier'"},
            {"backup_slow_all_threads_after_retryable_s3_error", false, false, "New setting"},
            {"allow_experimental_ytsaurus_table_engine", false, false, "New setting."},
            {"allow_experimental_ytsaurus_table_function", false, false, "New setting."},
            {"allow_experimental_ytsaurus_dictionary_source", false, false, "New setting."},
            {"per_part_index_stats", false, false, "New setting."},
            {"allow_experimental_iceberg_compaction", 0, 0, "New setting"},
            {"delta_lake_snapshot_version", -1, -1, "New setting"},
            {"use_roaring_bitmap_iceberg_positional_deletes", false, false, "New setting"},
            {"iceberg_metadata_compression_method", "", "", "New setting"},
            {"allow_experimental_correlated_subqueries", false, true, "Mark correlated subqueries support as Beta."},
            {"promql_database", "", "", "New experimental setting"},
            {"promql_table", "", "", "New experimental setting"},
            {"evaluation_time", 0, 0, "New experimental setting"},
            {"output_format_parquet_date_as_uint16", false, false, "Added a compatibility setting for a minor compatibility-breaking change introduced back in 24.12."},
            {"enable_lightweight_update", false, true, "Lightweight updates were moved to Beta. Added an alias for setting 'allow_experimental_lightweight_update'."},
            {"allow_experimental_lightweight_update", false, true, "Lightweight updates were moved to Beta."},
            {"s3_slow_all_threads_after_retryable_error", false, false, "Added an alias for setting `backup_slow_all_threads_after_retryable_s3_error`"},
            {"serialize_string_in_memory_with_zero_byte", true, true, "New setting"},
        });
        addSettingsChanges(settings_changes_history, "25.7",
        {
            /// RELEASE CLOSED
            {"correlated_subqueries_substitute_equivalent_expressions", false, true, "New setting to correlated subquery planning optimization."},
            {"function_date_trunc_return_type_behavior", 0, 0, "Add new setting to preserve old behaviour of dateTrunc function"},
            {"output_format_parquet_geometadata", false, true, "A new setting to allow to write information about geo columns in parquet metadata and encode columns in WKB format."},
            {"cluster_function_process_archive_on_multiple_nodes", false, true, "New setting"},
            {"enable_vector_similarity_index", false, false, "Added an alias for setting `allow_experimental_vector_similarity_index`"},
            {"distributed_plan_max_rows_to_broadcast", 20000, 20000, "New experimental setting."},
            {"output_format_json_map_as_array_of_tuples", false, false, "New setting"},
            {"input_format_json_map_as_array_of_tuples", false, false, "New setting"},
            {"parallel_distributed_insert_select", 0, 2, "Enable parallel distributed insert select by default"},
            {"write_through_distributed_cache_buffer_size", 0, 0, "New cloud setting"},
            {"min_joined_block_size_rows", 0, DEFAULT_BLOCK_SIZE, "New setting."},
            {"table_engine_read_through_distributed_cache", false, false, "New setting"},
            {"distributed_cache_alignment", 0, 0, "Rename of distributed_cache_read_alignment"},
            {"enable_scopes_for_with_statement", true, true, "New setting for backward compatibility with the old analyzer."},
            {"output_format_parquet_enum_as_byte_array", false, false, "Write enum using parquet physical type: BYTE_ARRAY and logical type: ENUM"},
            {"distributed_plan_force_shuffle_aggregation", 0, 0, "New experimental setting"},
            {"allow_insert_into_iceberg", false, false, "New setting."},
            /// RELEASE CLOSED
        });
        addSettingsChanges(settings_changes_history, "25.6",
        {
            /// RELEASE CLOSED
            {"output_format_native_use_flattened_dynamic_and_json_serialization", false, false, "Add flattened Dynamic/JSON serializations to Native format"},
            {"cast_string_to_date_time_mode", "basic", "basic", "Allow to use different DateTime parsing mode in String to DateTime cast"},
            {"parallel_replicas_connect_timeout_ms", 1000, 300, "Separate connection timeout for parallel replicas queries"},
            {"use_iceberg_partition_pruning", false, true, "Enable Iceberg partition pruning by default."},
            {"distributed_cache_credentials_refresh_period_seconds", 5, 5, "New private setting"},
            {"enable_shared_storage_snapshot_in_query", false, false, "A new setting to share storage snapshot in query"},
            {"merge_tree_storage_snapshot_sleep_ms", 0, 0, "A new setting to debug storage snapshot consistency in query"},
            {"enable_job_stack_trace", false, false, "The setting was disabled by default to avoid performance overhead."},
            {"use_legacy_to_time", true, true, "New setting. Allows for user to use the old function logic for toTime, which works as toTimeWithFixedDate."},
            {"allow_experimental_time_time64_type", false, false, "New settings. Allows to use a new experimental Time and Time64 data types."},
            {"enable_time_time64_type", false, false, "New settings. Allows to use a new experimental Time and Time64 data types."},
            {"optimize_use_projection_filtering", false, true, "New setting"},
            {"input_format_parquet_enable_json_parsing", false, true, "When reading Parquet files, parse JSON columns as ClickHouse JSON Column."},
            {"use_skip_indexes_if_final", 0, 1, "Change in default value of setting"},
            {"use_skip_indexes_if_final_exact_mode", 0, 1, "Change in default value of setting"},
            {"allow_experimental_time_series_aggregate_functions", false, false, "New setting to enable experimental timeSeries* aggregate functions."},
            {"min_outstreams_per_resize_after_split", 0, 24, "New setting."},
            {"count_matches_stop_at_empty_match", true, false, "New setting."},
            {"enable_parallel_blocks_marshalling", "false", "true", "A new setting"},
            {"format_schema_source", "file", "file", "New setting"},
            {"format_schema_message_name", "", "", "New setting"},
            {"enable_scopes_for_with_statement", true, true, "New setting for backward compatibility with the old analyzer."},
            {"backup_slow_all_threads_after_retryable_s3_error", false, false, "New setting"},
            {"s3_slow_all_threads_after_retryable_error", false, false, "Added an alias for setting `backup_slow_all_threads_after_retryable_s3_error`"},
            {"s3_retry_attempts", 500, 500, "Changed the value of the obsolete setting"},
            /// RELEASE CLOSED
        });
        addSettingsChanges(settings_changes_history, "25.5",
        {
            /// Release closed. Please use 25.6
            {"geotoh3_argument_order", "lon_lat", "lat_lon", "A new setting for legacy behaviour to set lon and lat argument order"},
            {"secondary_indices_enable_bulk_filtering", false, true, "A new algorithm for filtering by data skipping indices"},
            {"implicit_table_at_top_level", "", "", "A new setting, used in clickhouse-local"},
            {"use_skip_indexes_if_final_exact_mode", 0, 0, "This setting was introduced to help FINAL query return correct results with skip indexes"},
            {"parsedatetime_e_requires_space_padding", true, false, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"},
            {"formatdatetime_e_with_space_padding", true, false, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"},
            {"input_format_max_block_size_bytes", 0, 0, "New setting to limit bytes size if blocks created by input format"},
            {"parallel_replicas_insert_select_local_pipeline", false, true, "Use local pipeline during distributed INSERT SELECT with parallel replicas. Currently disabled due to performance issues"},
            {"page_cache_block_size", 1048576, 1048576, "Made this setting adjustable on a per-query level."},
            {"page_cache_lookahead_blocks", 16, 16, "Made this setting adjustable on a per-query level."},
            {"output_format_pretty_glue_chunks", "0", "auto", "A new setting to make Pretty formats prettier."},
            {"distributed_cache_read_only_from_current_az", true, true, "New setting"},
            {"parallel_hash_join_threshold", 0, 100'000, "New setting"},
            {"max_limit_for_ann_queries", 1'000, 0, "Obsolete setting"},
            {"max_limit_for_vector_search_queries", 1'000, 1'000, "New setting"},
            {"min_os_cpu_wait_time_ratio_to_throw", 0, 0, "Setting values were changed and backported to 25.4"},
            {"max_os_cpu_wait_time_ratio_to_throw", 0, 0, "Setting values were changed and backported to 25.4"},
            {"make_distributed_plan", 0, 0, "New experimental setting."},
            {"distributed_plan_execute_locally", 0, 0, "New experimental setting."},
            {"distributed_plan_default_shuffle_join_bucket_count", 8, 8, "New experimental setting."},
            {"distributed_plan_default_reader_bucket_count", 8, 8, "New experimental setting."},
            {"distributed_plan_optimize_exchanges", true, true, "New experimental setting."},
            {"distributed_plan_force_exchange_kind", "", "", "New experimental setting."},
            {"update_sequential_consistency", true, true, "A new setting"},
            {"update_parallel_mode", "auto", "auto", "A new setting"},
            {"lightweight_delete_mode", "alter_update", "alter_update", "A new setting"},
            {"alter_update_mode", "heavy", "heavy", "A new setting"},
            {"apply_patch_parts", true, true, "A new setting"},
            {"allow_experimental_lightweight_update", false, false, "A new setting"},
            {"allow_experimental_delta_kernel_rs", false, true, "New setting"},
            {"allow_experimental_database_hms_catalog", false, false, "Allow experimental database engine DataLakeCatalog with catalog_type = 'hive'"},
            {"vector_search_filter_strategy", "auto", "auto", "New setting"},
            {"vector_search_postfilter_multiplier", 1.0, 1.0, "New setting"},
            {"compile_expressions", false, true, "We believe that the LLVM infrastructure behind the JIT compiler is stable enough to enable this setting by default."},
            {"input_format_parquet_bloom_filter_push_down", false, true, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and bloom filter in the Parquet metadata."},
            {"input_format_parquet_allow_geoparquet_parser", false, true, "A new setting to use geo columns in parquet file"},
            {"enable_url_encoding", true, false, "Changed existing setting's default value"},
            {"s3_slow_all_threads_after_network_error", false, true, "New setting"},
            {"enable_scopes_for_with_statement", true, true, "New setting for backward compatibility with the old analyzer."},
            /// Release closed. Please use 25.6
        });
        addSettingsChanges(settings_changes_history, "25.4",
        {
            /// Release closed. Please use 25.5
            {"use_query_condition_cache", false, true, "A new optimization"},
            {"allow_materialized_view_with_bad_select", true, false, "Don't allow creating MVs referencing nonexistent columns or tables"},
            {"query_plan_optimize_lazy_materialization", false, true, "Added new setting to use query plan for lazy materialization optimisation"},
            {"query_plan_max_limit_for_lazy_materialization", 10, 10, "Added new setting to control maximum limit value that allows to use query plan for lazy materialization optimisation. If zero, there is no limit"},
            {"query_plan_convert_join_to_in", false, false, "New setting"},
            {"enable_hdfs_pread", true, true, "New setting."},
            {"low_priority_query_wait_time_ms", 1000, 1000, "New setting."},
            {"allow_experimental_correlated_subqueries", false, false, "Added new setting to allow correlated subqueries execution."},
            {"serialize_query_plan", false, false, "NewSetting"},
            {"allow_experimental_shared_set_join", 0, 1, "A setting for ClickHouse Cloud to enable SharedSet and SharedJoin"},
            {"allow_special_bool_values_inside_variant", true, false, "Don't allow special bool values during Variant type parsing"},
            {"cast_string_to_variant_use_inference", true, true, "New setting to enable/disable types inference during CAST from String to Variant"},
            {"distributed_cache_read_request_max_tries", 20, 20, "New setting"},
            {"query_condition_cache_store_conditions_as_plaintext", false, false, "New setting"},
            {"min_os_cpu_wait_time_ratio_to_throw", 0, 0, "New setting"},
            {"max_os_cpu_wait_time_ratio_to_throw", 0, 0, "New setting"},
            {"query_plan_merge_filter_into_join_condition", false, true, "Added new setting to merge filter into join condition"},
            {"use_local_cache_for_remote_storage", true, false, "Obsolete setting."},
            {"iceberg_timestamp_ms", 0, 0, "New setting."},
            {"iceberg_snapshot_id", 0, 0, "New setting."},
            {"use_iceberg_metadata_files_cache", true, true, "New setting"},
            {"query_plan_join_shard_by_pk_ranges", false, false, "New setting"},
            {"parallel_replicas_insert_select_local_pipeline", false, false, "Use local pipeline during distributed INSERT SELECT with parallel replicas. Currently disabled due to performance issues"},
            {"parallel_hash_join_threshold", 0, 0, "New setting"},
            {"function_date_trunc_return_type_behavior", 1, 0, "Change the result type for dateTrunc function for DateTime64/Date32 arguments to DateTime64/Date32 regardless of time unit to get correct result for negative values"},
            {"enable_scopes_for_with_statement", true, true, "New setting for backward compatibility with the old analyzer."},
            /// Release closed. Please use 25.5
        });
        addSettingsChanges(settings_changes_history, "25.3",
        {
            /// Release closed. Please use 25.4
            {"enable_json_type", false, true, "JSON data type is production-ready"},
            {"enable_dynamic_type", false, true, "Dynamic data type is production-ready"},
            {"enable_variant_type", false, true, "Variant data type is production-ready"},
            {"allow_experimental_json_type", false, true, "JSON data type is production-ready"},
            {"allow_experimental_dynamic_type", false, true, "Dynamic data type is production-ready"},
            {"allow_experimental_variant_type", false, true, "Variant data type is production-ready"},
            {"allow_experimental_database_unity_catalog", false, false, "Allow experimental database engine DataLakeCatalog with catalog_type = 'unity'"},
            {"allow_experimental_database_glue_catalog", false, false, "Allow experimental database engine DataLakeCatalog with catalog_type = 'glue'"},
            {"use_page_cache_with_distributed_cache", false, false, "New setting"},
            {"use_query_condition_cache", false, false, "New setting."},
            {"parallel_replicas_for_cluster_engines", false, true, "New setting."},
            {"parallel_hash_join_threshold", 0, 0, "New setting"},
            /// Release closed. Please use 25.4
        });
        addSettingsChanges(settings_changes_history, "25.2",
        {
            /// Release closed. Please use 25.3
            {"schema_inference_make_json_columns_nullable", false, false, "Allow to infer Nullable(JSON) during schema inference"},
            {"query_plan_use_new_logical_join_step", false, true, "Enable new step"},
            {"postgresql_fault_injection_probability", 0., 0., "New setting"},
            {"apply_settings_from_server", false, true, "Client-side code (e.g. INSERT input parsing and query output formatting) will use the same settings as the server, including settings from server config."},
            {"merge_tree_use_deserialization_prefixes_cache", true, true, "A new setting to control the usage of deserialization prefixes cache in MergeTree"},
            {"merge_tree_use_prefixes_deserialization_thread_pool", true, true, "A new setting controlling the usage of the thread pool for parallel prefixes deserialization in MergeTree"},
            {"optimize_and_compare_chain", false, true, "A new setting"},
            {"enable_adaptive_memory_spill_scheduler", false, false, "New setting. Enable spill memory data into external storage adaptively."},
            {"output_format_parquet_write_bloom_filter", false, true, "Added support for writing Parquet bloom filters."},
            {"output_format_parquet_bloom_filter_bits_per_value", 10.5, 10.5, "New setting."},
            {"output_format_parquet_bloom_filter_flush_threshold_bytes", 128 * 1024 * 1024, 128 * 1024 * 1024, "New setting."},
            {"output_format_pretty_max_rows", 10000, 1000, "It is better for usability - less amount to scroll."},
            {"restore_replicated_merge_tree_to_shared_merge_tree", false, false, "New setting."},
            {"parallel_replicas_only_with_analyzer", true, true, "Parallel replicas is supported only with analyzer enabled"},
            {"s3_allow_multipart_copy", true, true, "New setting."},
        });
        addSettingsChanges(settings_changes_history, "25.1",
        {
            /// Release closed. Please use 25.2
            {"allow_not_comparable_types_in_order_by", true, false, "Don't allow not comparable types in order by by default"},
            {"allow_not_comparable_types_in_comparison_functions", true, false, "Don't allow not comparable types in comparison functions by default"},
            {"output_format_json_pretty_print", false, true, "Print values in a pretty format in JSON output format by default"},
            {"allow_experimental_ts_to_grid_aggregate_function", false, false, "Cloud only"},
            {"formatdatetime_f_prints_scale_number_of_digits", true, false, "New setting."},
            {"distributed_cache_connect_max_tries", 20, 20, "Cloud only"},
            {"query_plan_use_new_logical_join_step", false, false, "New join step, internal change"},
            {"distributed_cache_min_bytes_for_seek", 0, 0, "New private setting."},
            {"use_iceberg_partition_pruning", false, false, "New setting for Iceberg partition pruning."},
            {"max_bytes_ratio_before_external_group_by", 0.0, 0.5, "Enable automatic spilling to disk by default."},
            {"max_bytes_ratio_before_external_sort", 0.0, 0.5, "Enable automatic spilling to disk by default."},
            {"min_external_sort_block_bytes", 0., 100_MiB, "New setting."},
            {"s3queue_migrate_old_metadata_to_buckets", false, false, "New setting."},
            {"distributed_cache_pool_behaviour_on_limit", "allocate_bypassing_pool", "wait", "Cloud only"},
            {"use_hive_partitioning", false, true, "Enabled the setting by default."},
            {"query_plan_try_use_vector_search", false, true, "New setting."},
            {"short_circuit_function_evaluation_for_nulls", false, true, "Allow to execute functions with Nullable arguments only on rows with non-NULL values in all arguments"},
            {"short_circuit_function_evaluation_for_nulls_threshold", 1.0, 1.0, "Ratio threshold of NULL values to execute functions with Nullable arguments only on rows with non-NULL values in all arguments. Applies when setting short_circuit_function_evaluation_for_nulls is enabled."},
            {"output_format_orc_writer_time_zone_name", "GMT", "GMT", "The time zone name for ORC writer, the default ORC writer's time zone is GMT."},
            {"output_format_pretty_highlight_trailing_spaces", false, true, "A new setting."},
            {"allow_experimental_bfloat16_type", false, true, "Add new BFloat16 type"},
            {"allow_push_predicate_ast_for_distributed_subqueries", false, true, "A new setting"},
            {"output_format_pretty_squash_consecutive_ms", 0, 50, "Add new setting"},
            {"output_format_pretty_squash_max_wait_ms", 0, 1000, "Add new setting"},
            {"output_format_pretty_max_column_name_width_cut_to", 0, 24, "A new setting"},
            {"output_format_pretty_max_column_name_width_min_chars_to_cut", 0, 4, "A new setting"},
            {"output_format_pretty_multiline_fields", false, true, "A new setting"},
            {"output_format_pretty_fallback_to_vertical", false, true, "A new setting"},
            {"output_format_pretty_fallback_to_vertical_max_rows_per_chunk", 0, 100, "A new setting"},
            {"output_format_pretty_fallback_to_vertical_min_columns", 0, 5, "A new setting"},
            {"output_format_pretty_fallback_to_vertical_min_table_width", 0, 250, "A new setting"},
            {"merge_table_max_tables_to_look_for_schema_inference", 1, 1000, "A new setting"},
            {"max_autoincrement_series", 1000, 1000, "A new setting"},
            {"validate_enum_literals_in_operators", false, false, "A new setting"},
            {"allow_experimental_kusto_dialect", true, false, "A new setting"},
            {"allow_experimental_prql_dialect", true, false, "A new setting"},
            {"h3togeo_lon_lat_result_order", true, false, "A new setting"},
            {"max_parallel_replicas", 1, 1000, "Use up to 1000 parallel replicas by default."},
            {"allow_general_join_planning", false, true, "Allow more general join planning algorithm when hash join algorithm is enabled."},
            {"optimize_extract_common_expressions", false, true, "Optimize WHERE, PREWHERE, ON, HAVING and QUALIFY expressions by extracting common expressions out from disjunction of conjunctions."},
            /// Release closed. Please use 25.2
        });
        addSettingsChanges(settings_changes_history, "24.12",
        {
            /// Release closed. Please use 25.1
            {"allow_experimental_database_iceberg", false, false, "New setting."},
            {"shared_merge_tree_sync_parts_on_partition_operations", 1, 1, "New setting. By default parts are always synchronized"},
            {"query_plan_join_swap_table", "false", "auto", "New setting. Right table was always chosen before."},
            {"max_size_to_preallocate_for_aggregation", 100'000'000, 1'000'000'000'000, "Enable optimisation for bigger tables."},
            {"max_size_to_preallocate_for_joins", 100'000'000, 1'000'000'000'000, "Enable optimisation for bigger tables."},
            {"max_bytes_ratio_before_external_group_by", 0., 0., "New setting."},
            {"optimize_extract_common_expressions", false, false, "Introduce setting to optimize WHERE, PREWHERE, ON, HAVING and QUALIFY expressions by extracting common expressions out from disjunction of conjunctions."},
            {"max_bytes_ratio_before_external_sort", 0., 0., "New setting."},
            {"use_async_executor_for_materialized_views", false, false, "New setting."},
            {"http_response_headers", "", "", "New setting."},
            {"output_format_parquet_datetime_as_uint32", true, false, "Write DateTime as DateTime64(3) instead of UInt32 (these are the two Parquet types closest to DateTime)."},
            {"output_format_parquet_date_as_uint16", true, false, "Write Date as Date32 instead of plain UInt16 (these are the two Parquet types closest to Date)."},
            {"skip_redundant_aliases_in_udf", false, false, "When enabled, this allows you to use the same user defined function several times for several materialized columns in the same table."},
            {"parallel_replicas_index_analysis_only_on_coordinator", true, true, "Index analysis done only on replica-coordinator and skipped on other replicas. Effective only with enabled parallel_replicas_local_plan"}, // enabling it was moved to 24.10
            {"least_greatest_legacy_null_behavior", true, false, "New setting"},
            {"use_concurrency_control", false, true, "Enable concurrency control by default"},
            {"join_algorithm", "default", "direct,parallel_hash,hash", "'default' was deprecated in favor of explicitly specified join algorithms, also parallel_hash is now preferred over hash"},
            /// Release closed. Please use 25.1
        });
        addSettingsChanges(settings_changes_history, "24.11",
        {
            {"validate_mutation_query", false, true, "New setting to validate mutation queries by default."},
            {"enable_job_stack_trace", false, false, "Enables collecting stack traces from job's scheduling. Disabled by default to avoid performance overhead."},
            {"allow_suspicious_types_in_group_by", true, false, "Don't allow Variant/Dynamic types in GROUP BY by default"},
            {"allow_suspicious_types_in_order_by", true, false, "Don't allow Variant/Dynamic types in ORDER BY by default"},
            {"distributed_cache_discard_connection_if_unread_data", true, true, "New setting"},
            {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", true, true, "New setting"},
            {"filesystem_cache_enable_background_download_during_fetch", true, true, "New setting"},
            {"azure_check_objects_after_upload", false, false, "Check each uploaded object in azure blob storage to be sure that upload was successful"},
            {"backup_restore_keeper_max_retries", 20, 1000, "Should be big enough so the whole operation BACKUP or RESTORE operation won't fail because of a temporary [Zoo]Keeper failure in the middle of it."},
            {"backup_restore_failure_after_host_disconnected_for_seconds", 0, 3600, "New setting."},
            {"backup_restore_keeper_max_retries_while_initializing", 0, 20, "New setting."},
            {"backup_restore_keeper_max_retries_while_handling_error", 0, 20, "New setting."},
            {"backup_restore_finish_timeout_after_error_sec", 0, 180, "New setting."},
            {"query_plan_merge_filters", false, true, "Allow to merge filters in the query plan. This is required to properly support filter-push-down with the analyzer."},
            {"parallel_replicas_local_plan", false, true, "Use local plan for local replica in a query with parallel replicas"},
            {"merge_tree_use_v1_object_and_dynamic_serialization", true, false, "Add new serialization V2 version for JSON and Dynamic types"},
            {"min_joined_block_size_bytes", 524288, 524288, "New setting."},
            {"allow_experimental_bfloat16_type", false, false, "Add new experimental BFloat16 type"},
            {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", 1, 1, "Rename of setting skip_download_if_exceeds_query_cache_limit"},
            {"filesystem_cache_prefer_bigger_buffer_size", true, true, "New setting"},
            {"read_in_order_use_virtual_row", false, false, "Use virtual row while reading in order of primary key or its monotonic function fashion. It is useful when searching over multiple parts as only relevant ones are touched."},
            {"s3_skip_empty_files", false, true, "We hope it will provide better UX"},
            {"filesystem_cache_boundary_alignment", 0, 0, "New setting"},
            {"push_external_roles_in_interserver_queries", false, true, "New setting."},
            {"enable_variant_type", false, false, "Add alias to allow_experimental_variant_type"},
            {"enable_dynamic_type", false, false, "Add alias to allow_experimental_dynamic_type"},
            {"enable_json_type", false, false, "Add alias to allow_experimental_json_type"},
        });
        addSettingsChanges(settings_changes_history, "24.10",
        {
            {"query_metric_log_interval", 0, -1, "New setting."},
            {"enforce_strict_identifier_format", false, false, "New setting."},
            {"enable_parsing_to_custom_serialization", false, true, "New setting"},
            {"mongodb_throw_on_unsupported_query", false, true, "New setting."},
            {"enable_parallel_replicas", false, false, "Parallel replicas with read tasks became the Beta tier feature."},
            {"parallel_replicas_mode", "read_tasks", "read_tasks", "This setting was introduced as a part of making parallel replicas feature Beta"},
            {"filesystem_cache_name", "", "", "Filesystem cache name to use for stateless table engines or data lakes"},
            {"restore_replace_external_dictionary_source_to_null", false, false, "New setting."},
            {"show_create_query_identifier_quoting_rule", "when_necessary", "when_necessary", "New setting."},
            {"show_create_query_identifier_quoting_style", "Backticks", "Backticks", "New setting."},
            {"merge_tree_min_read_task_size", 8, 8, "New setting"},
            {"merge_tree_min_rows_for_concurrent_read_for_remote_filesystem", (20 * 8192), 0, "Setting is deprecated"},
            {"merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem", (24 * 10 * 1024 * 1024), 0, "Setting is deprecated"},
            {"implicit_select", false, false, "A new setting."},
            {"output_format_native_write_json_as_string", false, false, "Add new setting to allow write JSON column as single String column in Native format"},
            {"output_format_binary_write_json_as_string", false, false, "Add new setting to write values of JSON type as JSON string in RowBinary output format"},
            {"input_format_binary_read_json_as_string", false, false, "Add new setting to read values of JSON type as JSON string in RowBinary input format"},
            {"min_free_disk_bytes_to_perform_insert", 0, 0, "New setting."},
            {"min_free_disk_ratio_to_perform_insert", 0.0, 0.0, "New setting."},
            {"parallel_replicas_local_plan", false, true, "Use local plan for local replica in a query with parallel replicas"},
            {"enable_named_columns_in_function_tuple", false, false, "Disabled pending usability improvements"},
            {"cloud_mode_database_engine", 1, 1, "A setting for ClickHouse Cloud"},
            {"allow_experimental_shared_set_join", 0, 0, "A setting for ClickHouse Cloud"},
            {"read_through_distributed_cache", 0, 0, "A setting for ClickHouse Cloud"},
            {"write_through_distributed_cache", 0, 0, "A setting for ClickHouse Cloud"},
            {"distributed_cache_throw_on_error", 0, 0, "A setting for ClickHouse Cloud"},
            {"distributed_cache_log_mode", "on_error", "on_error", "A setting for ClickHouse Cloud"},
            {"distributed_cache_fetch_metrics_only_from_current_az", 1, 1, "A setting for ClickHouse Cloud"},
            {"distributed_cache_connect_max_tries", 20, 20, "A setting for ClickHouse Cloud"},
            {"distributed_cache_receive_response_wait_milliseconds", 60000, 60000, "A setting for ClickHouse Cloud"},
            {"distributed_cache_receive_timeout_milliseconds", 10000, 10000, "A setting for ClickHouse Cloud"},
            {"distributed_cache_wait_connection_from_pool_milliseconds", 100, 100, "A setting for ClickHouse Cloud"},
            {"distributed_cache_bypass_connection_pool", 0, 0, "A setting for ClickHouse Cloud"},
            {"distributed_cache_pool_behaviour_on_limit", "allocate_bypassing_pool", "allocate_bypassing_pool", "A setting for ClickHouse Cloud"},
            {"distributed_cache_read_alignment", 0, 0, "A setting for ClickHouse Cloud"},
            {"distributed_cache_max_unacked_inflight_packets", 10, 10, "A setting for ClickHouse Cloud"},
            {"distributed_cache_data_packet_ack_window", 5, 5, "A setting for ClickHouse Cloud"},
            {"input_format_parquet_enable_row_group_prefetch", false, true, "Enable row group prefetching during parquet parsing. Currently, only single-threaded parsing can prefetch."},
            {"input_format_orc_dictionary_as_low_cardinality", false, true, "Treat ORC dictionary encoded columns as LowCardinality columns while reading ORC files"},
            {"allow_experimental_refreshable_materialized_view", false, true, "Not experimental anymore"},
            {"max_parts_to_move", 0, 1000, "New setting"},
            {"hnsw_candidate_list_size_for_search", 64, 256, "New setting. Previously, the value was optionally specified in CREATE INDEX and 64 by default."},
            {"allow_reorder_prewhere_conditions", true, true, "New setting"},
            {"input_format_parquet_bloom_filter_push_down", false, false, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and bloom filter in the Parquet metadata."},
            {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", false, false, "Dynamically trim the trailing zeros of datetime64 values to adjust the output scale to (0, 3, 6), corresponding to 'seconds', 'milliseconds', and 'microseconds'."},
            {"parallel_replicas_index_analysis_only_on_coordinator", false, true, "Index analysis done only on replica-coordinator and skipped on other replicas. Effective only with enabled parallel_replicas_local_plan"},
            {"distributed_cache_discard_connection_if_unread_data", true, true, "New setting"},
            {"azure_check_objects_after_upload", false, false, "Check each uploaded object in azure blob storage to be sure that upload was successful"},
            {"backup_restore_keeper_max_retries", 20, 1000, "Should be big enough so the whole operation BACKUP or RESTORE operation won't fail because of a temporary [Zoo]Keeper failure in the middle of it."},
            {"backup_restore_failure_after_host_disconnected_for_seconds", 0, 3600, "New setting."},
            {"backup_restore_keeper_max_retries_while_initializing", 0, 20, "New setting."},
            {"backup_restore_keeper_max_retries_while_handling_error", 0, 20, "New setting."},
            {"backup_restore_finish_timeout_after_error_sec", 0, 180, "New setting."},
        });
        addSettingsChanges(settings_changes_history, "24.9",
        {
            {"output_format_orc_dictionary_key_size_threshold", 0.0, 0.0, "For a string column in ORC output format, if the number of distinct values is greater than this fraction of the total number of non-null rows, turn off dictionary encoding. Otherwise dictionary encoding is enabled"},
            {"input_format_json_empty_as_default", false, false, "Added new setting to allow to treat empty fields in JSON input as default values."},
            {"input_format_try_infer_variants", false, false, "Try to infer Variant type in text formats when there is more than one possible type for column/array elements"},
            {"join_output_by_rowlist_perkey_rows_threshold", 0, 5, "The lower limit of per-key average rows in the right table to determine whether to output by row list in hash join."},
            {"create_if_not_exists", false, false, "New setting."},
            {"allow_materialized_view_with_bad_select", true, true, "Support (but not enable yet) stricter validation in CREATE MATERIALIZED VIEW"},
            {"parallel_replicas_mark_segment_size", 128, 0, "Value for this setting now determined automatically"},
            {"database_replicated_allow_replicated_engine_arguments", 1, 0, "Don't allow explicit arguments by default"},
            {"database_replicated_allow_explicit_uuid", 1, 0, "Added a new setting to disallow explicitly specifying table UUID"},
            {"parallel_replicas_local_plan", false, false, "Use local plan for local replica in a query with parallel replicas"},
            {"join_to_sort_minimum_perkey_rows", 0, 40, "The lower limit of per-key average rows in the right table to determine whether to rerange the right table by key in left or inner join. This setting ensures that the optimization is not applied for sparse table keys"},
            {"join_to_sort_maximum_table_rows", 0, 10000, "The maximum number of rows in the right table to determine whether to rerange the right table by key in left or inner join"},
            {"allow_experimental_join_right_table_sorting", false, false, "If it is set to true, and the conditions of `join_to_sort_minimum_perkey_rows` and `join_to_sort_maximum_table_rows` are met, rerange the right table by key to improve the performance in left or inner hash join"},
            {"mongodb_throw_on_unsupported_query", false, true, "New setting."},
            {"min_free_disk_bytes_to_perform_insert", 0, 0, "Maintain some free disk space bytes from inserts while still allowing for temporary writing."},
            {"min_free_disk_ratio_to_perform_insert", 0.0, 0.0, "Maintain some free disk space bytes expressed as ratio to total disk space from inserts while still allowing for temporary writing."},
        });
        addSettingsChanges(settings_changes_history, "24.8",
        {
            {"rows_before_aggregation", false, false, "Provide exact value for rows_before_aggregation statistic, represents the number of rows read before aggregation"},
            {"restore_replace_external_table_functions_to_null", false, false, "New setting."},
            {"restore_replace_external_engines_to_null", false, false, "New setting."},
            {"input_format_json_max_depth", 1000000, 1000, "It was unlimited in previous versions, but that was unsafe."},
            {"merge_tree_min_bytes_per_task_for_remote_reading", 4194304, 2097152, "Value is unified with `filesystem_prefetch_min_bytes_for_single_read_task`"},
            {"use_hive_partitioning", false, false, "Allows to use hive partitioning for File, URL, S3, AzureBlobStorage and HDFS engines."},
            {"allow_experimental_kafka_offsets_storage_in_keeper", false, false, "Allow the usage of experimental Kafka storage engine that stores the committed offsets in ClickHouse Keeper"},
            {"allow_archive_path_syntax", true, true, "Added new setting to allow disabling archive path syntax."},
            {"query_cache_tag", "", "", "New setting for labeling query cache settings."},
            {"allow_experimental_time_series_table", false, false, "Added new setting to allow the TimeSeries table engine"},
            {"enable_analyzer", 1, 1, "Added an alias to a setting `allow_experimental_analyzer`."},
            {"optimize_functions_to_subcolumns", false, true, "Enabled settings by default"},
            {"allow_experimental_json_type", false, false, "Add new experimental JSON type"},
            {"use_json_alias_for_old_object_type", true, false, "Use JSON type alias to create new JSON type"},
            {"type_json_skip_duplicated_paths", false, false, "Allow to skip duplicated paths during JSON parsing"},
            {"allow_experimental_vector_similarity_index", false, false, "Added new setting to allow experimental vector similarity indexes"},
            {"input_format_try_infer_datetimes_only_datetime64", true, false, "Allow to infer DateTime instead of DateTime64 in data formats"},
        });
        addSettingsChanges(settings_changes_history, "24.7",
        {
            {"output_format_parquet_write_page_index", false, true, "Add a possibility to write page index into parquet files."},
            {"output_format_binary_encode_types_in_binary_format", false, false, "Added new setting to allow to write type names in binary format in RowBinaryWithNamesAndTypes output format"},
            {"input_format_binary_decode_types_in_binary_format", false, false, "Added new setting to allow to read type names in binary format in RowBinaryWithNamesAndTypes input format"},
            {"output_format_native_encode_types_in_binary_format", false, false, "Added new setting to allow to write type names in binary format in Native output format"},
            {"input_format_native_decode_types_in_binary_format", false, false, "Added new setting to allow to read type names in binary format in Native output format"},
            {"read_in_order_use_buffering", false, true, "Use buffering before merging while reading in order of primary key"},
            {"enable_named_columns_in_function_tuple", false, false, "Generate named tuples in function tuple() when all names are unique and can be treated as unquoted identifiers."},
            {"optimize_trivial_insert_select", true, false, "The optimization does not make sense in many cases."},
            {"dictionary_validate_primary_key_type", false, false, "Validate primary key type for dictionaries. By default id type for simple layouts will be implicitly converted to UInt64."},
            {"collect_hash_table_stats_during_joins", false, true, "New setting."},
            {"max_size_to_preallocate_for_joins", 0, 100'000'000, "New setting."},
            {"input_format_orc_reader_time_zone_name", "GMT", "GMT", "The time zone name for ORC row reader, the default ORC row reader's time zone is GMT."},
            {"database_replicated_allow_heavy_create", true, false, "Long-running DDL queries (CREATE AS SELECT and POPULATE) for Replicated database engine was forbidden"},
            {"query_plan_merge_filters", false, false, "Allow to merge filters in the query plan"},
            {"azure_sdk_max_retries", 10, 10, "Maximum number of retries in azure sdk"},
            {"azure_sdk_retry_initial_backoff_ms", 10, 10, "Minimal backoff between retries in azure sdk"},
            {"azure_sdk_retry_max_backoff_ms", 1000, 1000, "Maximal backoff between retries in azure sdk"},
            {"ignore_on_cluster_for_replicated_named_collections_queries", false, false, "Ignore ON CLUSTER clause for replicated named collections management queries."},
            {"backup_restore_s3_retry_attempts", 1000,1000, "Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries. It takes place only for backup/restore."},
            {"postgresql_connection_attempt_timeout", 2, 2, "Allow to control 'connect_timeout' parameter of PostgreSQL connection."},
            {"postgresql_connection_pool_retries", 2, 2, "Allow to control the number of retries in PostgreSQL connection pool."}
        });
        addSettingsChanges(settings_changes_history, "24.6",
        {
            {"materialize_skip_indexes_on_insert", true, true, "Added new setting to allow to disable materialization of skip indexes on insert"},
            {"materialize_statistics_on_insert", true, true, "Added new setting to allow to disable materialization of statistics on insert"},
            {"input_format_parquet_use_native_reader", false, false, "When reading Parquet files, to use native reader instead of arrow reader."},
            {"hdfs_throw_on_zero_files_match", false, false, "Allow to throw an error when ListObjects request cannot match any files in HDFS engine instead of empty query result"},
            {"azure_throw_on_zero_files_match", false, false, "Allow to throw an error when ListObjects request cannot match any files in AzureBlobStorage engine instead of empty query result"},
            {"s3_validate_request_settings", true, true, "Allow to disable S3 request settings validation"},
            {"allow_experimental_full_text_index", false, false, "Enable experimental text index"},
            {"azure_skip_empty_files", false, false, "Allow to skip empty files in azure table engine"},
            {"hdfs_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in HDFS table engine"},
            {"azure_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in AzureBlobStorage table engine"},
            {"s3_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in S3 table engine"},
            {"s3_max_part_number", 10000, 10000, "Maximum part number number for s3 upload part"},
            {"s3_max_single_operation_copy_size", 32 * 1024 * 1024, 32 * 1024 * 1024, "Maximum size for a single copy operation in s3"},
            {"input_format_parquet_max_block_size", 8192, DEFAULT_BLOCK_SIZE, "Increase block size for parquet reader."},
            {"input_format_parquet_prefer_block_bytes", 0, DEFAULT_BLOCK_SIZE * 256, "Average block bytes output by parquet reader."},
            {"enable_blob_storage_log", true, true, "Write information about blob storage operations to system.blob_storage_log table"},
            {"allow_deprecated_snowflake_conversion_functions", true, false, "Disabled deprecated functions snowflakeToDateTime[64] and dateTime[64]ToSnowflake."},
            {"allow_statistic_optimize", false, false, "Old setting which popped up here being renamed."},
            {"allow_experimental_statistic", false, false, "Old setting which popped up here being renamed."},
            {"allow_statistics_optimize", false, false, "The setting was renamed. The previous name is `allow_statistic_optimize`."},
            {"allow_experimental_statistics", false, false, "The setting was renamed. The previous name is `allow_experimental_statistic`."},
            {"enable_vertical_final", false, true, "Enable vertical final by default again after fixing bug"},
            {"parallel_replicas_custom_key_range_lower", 0, 0, "Add settings to control the range filter when using parallel replicas with dynamic shards"},
            {"parallel_replicas_custom_key_range_upper", 0, 0, "Add settings to control the range filter when using parallel replicas with dynamic shards. A value of 0 disables the upper limit"},
            {"output_format_pretty_display_footer_column_names", 0, 1, "Add a setting to display column names in the footer if there are many rows. Threshold value is controlled by output_format_pretty_display_footer_column_names_min_rows."},
            {"output_format_pretty_display_footer_column_names_min_rows", 0, 50, "Add a setting to control the threshold value for setting output_format_pretty_display_footer_column_names_min_rows. Default 50."},
            {"output_format_csv_serialize_tuple_into_separate_columns", true, true, "A new way of how interpret tuples in CSV format was added."},
            {"input_format_csv_deserialize_separate_columns_into_tuple", true, true, "A new way of how interpret tuples in CSV format was added."},
            {"input_format_csv_try_infer_strings_from_quoted_tuples", true, true, "A new way of how interpret tuples in CSV format was added."},
        });
        addSettingsChanges(settings_changes_history, "24.5",
        {
            {"allow_deprecated_error_prone_window_functions", true, false, "Allow usage of deprecated error prone window functions (neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference)"},
            {"allow_experimental_join_condition", false, false, "Support join with inequal conditions which involve columns from both left and right table. e.g. t1.y < t2.y."},
            {"input_format_tsv_crlf_end_of_line", false, false, "Enables reading of CRLF line endings with TSV formats"},
            {"output_format_parquet_use_custom_encoder", false, true, "Enable custom Parquet encoder."},
            {"cross_join_min_rows_to_compress", 0, 10000000, "Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached."},
            {"cross_join_min_bytes_to_compress", 0, 1_GiB, "Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached."},
            {"http_max_chunk_size", 0, 0, "Internal limitation"},
            {"prefer_external_sort_block_bytes", 0, DEFAULT_BLOCK_SIZE * 256, "Prefer maximum block bytes for external sort, reduce the memory usage during merging."},
            {"input_format_force_null_for_omitted_fields", false, false, "Disable type-defaults for omitted fields when needed"},
            {"cast_string_to_dynamic_use_inference", false, false, "Add setting to allow converting String to Dynamic through parsing"},
            {"allow_experimental_dynamic_type", false, false, "Add new experimental Dynamic type"},
            {"azure_max_blocks_in_multipart_upload", 50000, 50000, "Maximum number of blocks in multipart upload for Azure."},
            {"allow_archive_path_syntax", false, true, "Added new setting to allow disabling archive path syntax."},
        });
        addSettingsChanges(settings_changes_history, "24.4",
        {
            {"input_format_json_throw_on_bad_escape_sequence", true, true, "Allow to save JSON strings with bad escape sequences"},
            {"max_parsing_threads", 0, 0, "Add a separate setting to control number of threads in parallel parsing from files"},
            {"ignore_drop_queries_probability", 0, 0, "Allow to ignore drop queries in server with specified probability for testing purposes"},
            {"lightweight_deletes_sync", 2, 2, "The same as 'mutation_sync', but controls only execution of lightweight deletes"},
            {"query_cache_system_table_handling", "save", "throw", "The query cache no longer caches results of queries against system tables"},
            {"input_format_json_ignore_unnecessary_fields", false, true, "Ignore unnecessary fields and not parse them. Enabling this may not throw exceptions on json strings of invalid format or with duplicated fields"},
            {"input_format_hive_text_allow_variable_number_of_columns", false, true, "Ignore extra columns in Hive Text input (if file has more columns than expected) and treat missing fields in Hive Text input as default values."},
            {"allow_experimental_database_replicated", false, true, "Database engine Replicated is now in Beta stage"},
            {"temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds", (10 * 60 * 1000), (10 * 60 * 1000), "Wait time to lock cache for space reservation in temporary data in filesystem cache"},
            {"optimize_rewrite_sum_if_to_count_if", false, true, "Only available for the analyzer, where it works correctly"},
            {"azure_allow_parallel_part_upload", "true", "true", "Use multiple threads for azure multipart upload."},
            {"max_recursive_cte_evaluation_depth", DBMS_RECURSIVE_CTE_MAX_EVALUATION_DEPTH, DBMS_RECURSIVE_CTE_MAX_EVALUATION_DEPTH, "Maximum limit on recursive CTE evaluation depth"},
            {"query_plan_convert_outer_join_to_inner_join", false, true, "Allow to convert OUTER JOIN to INNER JOIN if filter after JOIN always filters default values"},
        });
        addSettingsChanges(settings_changes_history, "24.3",
        {
            {"s3_connect_timeout_ms", 1000, 1000, "Introduce new dedicated setting for s3 connection timeout"},
            {"allow_experimental_shared_merge_tree", false, true, "The setting is obsolete"},
            {"use_page_cache_for_disks_without_file_cache", false, false, "Added userspace page cache"},
            {"read_from_page_cache_if_exists_otherwise_bypass_cache", false, false, "Added userspace page cache"},
            {"page_cache_inject_eviction", false, false, "Added userspace page cache"},
            {"default_table_engine", "None", "MergeTree", "Set default table engine to MergeTree for better usability"},
            {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", false, false, "Allow to use String type for ambiguous paths during named tuple inference from JSON objects"},
            {"traverse_shadow_remote_data_paths", false, false, "Traverse shadow directory when query system.remote_data_paths."},
            {"throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert", false, true, "Deduplication in dependent materialized view cannot work together with async inserts."},
            {"parallel_replicas_allow_in_with_subquery", false, true, "If true, subquery for IN will be executed on every follower replica"},
            {"log_processors_profiles", false, true, "Enable by default"},
            {"function_locate_has_mysql_compatible_argument_order", false, true, "Increase compatibility with MySQL's locate function."},
            {"allow_suspicious_primary_key", true, false, "Forbid suspicious PRIMARY KEY/ORDER BY for MergeTree (i.e. SimpleAggregateFunction)"},
            {"filesystem_cache_reserve_space_wait_lock_timeout_milliseconds", 1000, 1000, "Wait time to lock cache for space reservation in filesystem cache"},
            {"max_parser_backtracks", 0, 1000000, "Limiting the complexity of parsing"},
            {"analyzer_compatibility_join_using_top_level_identifier", false, false, "Force to resolve identifier in JOIN USING from projection"},
            {"distributed_insert_skip_read_only_replicas", false, false, "If true, INSERT into Distributed will skip read-only replicas"},
            {"keeper_max_retries", 10, 10, "Max retries for general keeper operations"},
            {"keeper_retry_initial_backoff_ms", 100, 100, "Initial backoff timeout for general keeper operations"},
            {"keeper_retry_max_backoff_ms", 5000, 5000, "Max backoff timeout for general keeper operations"},
            {"s3queue_allow_experimental_sharded_mode", false, false, "Enable experimental sharded mode of S3Queue table engine. It is experimental because it will be rewritten"},
            {"allow_experimental_analyzer", false, true, "Enable analyzer and planner by default."},
            {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability", 0.0, 0.0, "For testing of `PartsSplitter` - split read ranges into intersecting and non intersecting every time you read from MergeTree with the specified probability."},
            {"allow_get_client_http_header", false, false, "Introduced a new function."},
            {"output_format_pretty_row_numbers", false, true, "It is better for usability."},
            {"output_format_pretty_max_value_width_apply_for_single_value", true, false, "Single values in Pretty formats won't be cut."},
            {"output_format_parquet_string_as_string", false, true, "ClickHouse allows arbitrary binary data in the String data type, which is typically UTF-8. Parquet/ORC/Arrow Strings only support UTF-8. That's why you can choose which Arrow's data type to use for the ClickHouse String data type - String or Binary. While Binary would be more correct and compatible, using String by default will correspond to user expectations in most cases."},
            {"output_format_orc_string_as_string", false, true, "ClickHouse allows arbitrary binary data in the String data type, which is typically UTF-8. Parquet/ORC/Arrow Strings only support UTF-8. That's why you can choose which Arrow's data type to use for the ClickHouse String data type - String or Binary. While Binary would be more correct and compatible, using String by default will correspond to user expectations in most cases."},
            {"output_format_arrow_string_as_string", false, true, "ClickHouse allows arbitrary binary data in the String data type, which is typically UTF-8. Parquet/ORC/Arrow Strings only support UTF-8. That's why you can choose which Arrow's data type to use for the ClickHouse String data type - String or Binary. While Binary would be more correct and compatible, using String by default will correspond to user expectations in most cases."},
            {"output_format_parquet_compression_method", "lz4", "zstd", "Parquet/ORC/Arrow support many compression methods, including lz4 and zstd. ClickHouse supports each and every compression method. Some inferior tools, such as 'duckdb', lack support for the faster `lz4` compression method, that's why we set zstd by default."},
            {"output_format_orc_compression_method", "lz4", "zstd", "Parquet/ORC/Arrow support many compression methods, including lz4 and zstd. ClickHouse supports each and every compression method. Some inferior tools, such as 'duckdb', lack support for the faster `lz4` compression method, that's why we set zstd by default."},
            {"output_format_pretty_highlight_digit_groups", false, true, "If enabled and if output is a terminal, highlight every digit corresponding to the number of thousands, millions, etc. with underline."},
            {"geo_distance_returns_float64_on_float64_arguments", false, true, "Increase the default precision."},
            {"azure_max_inflight_parts_for_one_file", 20, 20, "The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited."},
            {"azure_strict_upload_part_size", 0, 0, "The exact size of part to upload during multipart upload to Azure blob storage."},
            {"azure_min_upload_part_size", 16*1024*1024, 16*1024*1024, "The minimum size of part to upload during multipart upload to Azure blob storage."},
            {"azure_max_upload_part_size", 5ull*1024*1024*1024, 5ull*1024*1024*1024, "The maximum size of part to upload during multipart upload to Azure blob storage."},
            {"azure_upload_part_size_multiply_factor", 2, 2, "Multiply azure_min_upload_part_size by this factor each time azure_multiply_parts_count_threshold parts were uploaded from a single write to Azure blob storage."},
            {"azure_upload_part_size_multiply_parts_count_threshold", 500, 500, "Each time this number of parts was uploaded to Azure blob storage, azure_min_upload_part_size is multiplied by azure_upload_part_size_multiply_factor."},
            {"output_format_csv_serialize_tuple_into_separate_columns", true, true, "A new way of how interpret tuples in CSV format was added."},
            {"input_format_csv_deserialize_separate_columns_into_tuple", true, true, "A new way of how interpret tuples in CSV format was added."},
            {"input_format_csv_try_infer_strings_from_quoted_tuples", true, true, "A new way of how interpret tuples in CSV format was added."},
        });
        addSettingsChanges(settings_changes_history, "24.2",
        {
            {"allow_suspicious_variant_types", true, false, "Don't allow creating Variant type with suspicious variants by default"},
            {"validate_experimental_and_suspicious_types_inside_nested_types", false, true, "Validate usage of experimental and suspicious types inside nested types"},
            {"output_format_values_escape_quote_with_quote", false, false, "If true escape ' with '', otherwise quoted with \\'"},
            {"output_format_pretty_single_large_number_tip_threshold", 0, 1'000'000, "Print a readable number tip on the right side of the table if the block consists of a single number which exceeds this value (except 0)"},
            {"input_format_try_infer_exponent_floats", true, false, "Don't infer floats in exponential notation by default"},
            {"query_plan_optimize_prewhere", true, true, "Allow to push down filter to PREWHERE expression for supported storages"},
            {"async_insert_max_data_size", 1000000, 10485760, "The previous value appeared to be too small."},
            {"async_insert_poll_timeout_ms", 10, 10, "Timeout in milliseconds for polling data from asynchronous insert queue"},
            {"async_insert_use_adaptive_busy_timeout", false, true, "Use adaptive asynchronous insert timeout"},
            {"async_insert_busy_timeout_min_ms", 50, 50, "The minimum value of the asynchronous insert timeout in milliseconds; it also serves as the initial value, which may be increased later by the adaptive algorithm"},
            {"async_insert_busy_timeout_max_ms", 200, 200, "The minimum value of the asynchronous insert timeout in milliseconds; async_insert_busy_timeout_ms is aliased to async_insert_busy_timeout_max_ms"},
            {"async_insert_busy_timeout_increase_rate", 0.2, 0.2, "The exponential growth rate at which the adaptive asynchronous insert timeout increases"},
            {"async_insert_busy_timeout_decrease_rate", 0.2, 0.2, "The exponential growth rate at which the adaptive asynchronous insert timeout decreases"},
            {"format_template_row_format", "", "", "Template row format string can be set directly in query"},
            {"format_template_resultset_format", "", "", "Template result set format string can be set in query"},
            {"split_parts_ranges_into_intersecting_and_non_intersecting_final", true, true, "Allow to split parts ranges into intersecting and non intersecting during FINAL optimization"},
            {"split_intersecting_parts_ranges_into_layers_final", true, true, "Allow to split intersecting parts ranges into layers during FINAL optimization"},
            {"azure_max_single_part_copy_size", 256*1024*1024, 256*1024*1024, "The maximum size of object to copy using single part copy to Azure blob storage."},
            {"min_external_table_block_size_rows", DEFAULT_INSERT_BLOCK_SIZE, DEFAULT_INSERT_BLOCK_SIZE, "Squash blocks passed to external table to specified size in rows, if blocks are not big enough"},
            {"min_external_table_block_size_bytes", DEFAULT_INSERT_BLOCK_SIZE * 256, DEFAULT_INSERT_BLOCK_SIZE * 256, "Squash blocks passed to external table to specified size in bytes, if blocks are not big enough."},
            {"parallel_replicas_prefer_local_join", true, true, "If true, and JOIN can be executed with parallel replicas algorithm, and all storages of right JOIN part are *MergeTree, local JOIN will be used instead of GLOBAL JOIN."},
            {"optimize_time_filter_with_preimage", true, true, "Optimize Date and DateTime predicates by converting functions into equivalent comparisons without conversions (e.g. toYear(col) = 2023 -> col >= '2023-01-01' AND col <= '2023-12-31')"},
            {"extract_key_value_pairs_max_pairs_per_row", 0, 0, "Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory."},
            {"default_view_definer", "CURRENT_USER", "CURRENT_USER", "Allows to set default `DEFINER` option while creating a view"},
            {"default_materialized_view_sql_security", "DEFINER", "DEFINER", "Allows to set a default value for SQL SECURITY option when creating a materialized view"},
            {"default_normal_view_sql_security", "INVOKER", "INVOKER", "Allows to set default `SQL SECURITY` option while creating a normal view"},
            {"mysql_map_string_to_text_in_show_columns", false, true, "Reduce the configuration effort to connect ClickHouse with BI tools."},
            {"mysql_map_fixed_string_to_text_in_show_columns", false, true, "Reduce the configuration effort to connect ClickHouse with BI tools."},
        });
        addSettingsChanges(settings_changes_history, "24.1",
        {
            {"print_pretty_type_names", false, true, "Better user experience."},
            {"input_format_json_read_bools_as_strings", false, true, "Allow to read bools as strings in JSON formats by default"},
            {"output_format_arrow_use_signed_indexes_for_dictionary", false, true, "Use signed indexes type for Arrow dictionaries by default as it's recommended"},
            {"allow_experimental_variant_type", false, false, "Add new experimental Variant type"},
            {"use_variant_as_common_type", false, false, "Allow to use Variant in if/multiIf if there is no common type"},
            {"output_format_arrow_use_64_bit_indexes_for_dictionary", false, false, "Allow to use 64 bit indexes type in Arrow dictionaries"},
            {"parallel_replicas_mark_segment_size", 128, 128, "Add new setting to control segment size in new parallel replicas coordinator implementation"},
            {"ignore_materialized_views_with_dropped_target_table", false, false, "Add new setting to allow to ignore materialized views with dropped target table"},
            {"output_format_compression_level", 3, 3, "Allow to change compression level in the query output"},
            {"output_format_compression_zstd_window_log", 0, 0, "Allow to change zstd window log in the query output when zstd compression is used"},
            {"enable_zstd_qat_codec", false, false, "Add new ZSTD_QAT codec"},
            {"enable_vertical_final", false, true, "Use vertical final by default"},
            {"output_format_arrow_use_64_bit_indexes_for_dictionary", false, false, "Allow to use 64 bit indexes type in Arrow dictionaries"},
            {"max_rows_in_set_to_optimize_join", 100000, 0, "Disable join optimization as it prevents from read in order optimization"},
            {"output_format_pretty_color", true, "auto", "Setting is changed to allow also for auto value, disabling ANSI escapes if output is not a tty"},
            {"function_visible_width_behavior", 0, 1, "We changed the default behavior of `visibleWidth` to be more precise"},
            {"max_estimated_execution_time", 0, 0, "Separate max_execution_time and max_estimated_execution_time"},
            {"iceberg_engine_ignore_schema_evolution", false, false, "Allow to ignore schema evolution in Iceberg table engine"},
            {"optimize_injective_functions_in_group_by", false, true, "Replace injective functions by it's arguments in GROUP BY section in analyzer"},
            {"update_insert_deduplication_token_in_dependent_materialized_views", false, false, "Allow to update insert deduplication token with table identifier during insert in dependent materialized views"},
            {"azure_max_unexpected_write_error_retries", 4, 4, "The maximum number of retries in case of unexpected errors during Azure blob storage write"},
            {"split_parts_ranges_into_intersecting_and_non_intersecting_final", false, true, "Allow to split parts ranges into intersecting and non intersecting during FINAL optimization"},
            {"split_intersecting_parts_ranges_into_layers_final", true, true, "Allow to split intersecting parts ranges into layers during FINAL optimization"}
        });
        addSettingsChanges(settings_changes_history, "23.12",
        {
            {"allow_suspicious_ttl_expressions", true, false, "It is a new setting, and in previous versions the behavior was equivalent to allowing."},
            {"input_format_parquet_allow_missing_columns", false, true, "Allow missing columns in Parquet files by default"},
            {"input_format_orc_allow_missing_columns", false, true, "Allow missing columns in ORC files by default"},
            {"input_format_arrow_allow_missing_columns", false, true, "Allow missing columns in Arrow files by default"}
        });
        addSettingsChanges(settings_changes_history, "23.11",
        {
            {"parsedatetime_parse_without_leading_zeros", false, true, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"}
        });
        addSettingsChanges(settings_changes_history, "23.9",
        {
            {"optimize_group_by_constant_keys", false, true, "Optimize group by constant keys by default"},
            {"input_format_json_try_infer_named_tuples_from_objects", false, true, "Try to infer named Tuples from JSON objects by default"},
            {"input_format_json_read_numbers_as_strings", false, true, "Allow to read numbers as strings in JSON formats by default"},
            {"input_format_json_read_arrays_as_strings", false, true, "Allow to read arrays as strings in JSON formats by default"},
            {"input_format_json_infer_incomplete_types_as_strings", false, true, "Allow to infer incomplete types as Strings in JSON formats by default"},
            {"input_format_json_try_infer_numbers_from_strings", true, false, "Don't infer numbers from strings in JSON formats by default to prevent possible parsing errors"},
            {"http_write_exception_in_output_format", false, true, "Output valid JSON/XML on exception in HTTP streaming."}
        });
        addSettingsChanges(settings_changes_history, "23.8",
        {
            {"rewrite_count_distinct_if_with_count_distinct_implementation", false, true, "Rewrite countDistinctIf with count_distinct_implementation configuration"}
        });
        addSettingsChanges(settings_changes_history, "23.7",
        {
            {"function_sleep_max_microseconds_per_block", 0, 3000000, "In previous versions, the maximum sleep time of 3 seconds was applied only for `sleep`, but not for `sleepEachRow` function. In the new version, we introduce this setting. If you set compatibility with the previous versions, we will disable the limit altogether."}
        });
        addSettingsChanges(settings_changes_history, "23.6",
        {
            {"http_send_timeout", 180, 30, "3 minutes seems crazy long. Note that this is timeout for a single network write call, not for the whole upload operation."},
            {"http_receive_timeout", 180, 30, "See http_send_timeout."}
        });
        addSettingsChanges(settings_changes_history, "23.5",
        {
            {"input_format_parquet_preserve_order", true, false, "Allow Parquet reader to reorder rows for better parallelism."},
            {"parallelize_output_from_storages", false, true, "Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows."},
            {"use_with_fill_by_sorting_prefix", false, true, "Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently"},
            {"output_format_parquet_compliant_nested_types", false, true, "Change an internal field name in output Parquet file schema."}
        });
        addSettingsChanges(settings_changes_history, "23.4",
        {
            {"allow_suspicious_indices", true, false, "If true, index can defined with identical expressions"},
            {"allow_nonconst_timezone_arguments", true, false, "Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()."},
            {"connect_timeout_with_failover_ms", 50, 1000, "Increase default connect timeout because of async connect"},
            {"connect_timeout_with_failover_secure_ms", 100, 1000, "Increase default secure connect timeout because of async connect"},
            {"hedged_connection_timeout_ms", 100, 50, "Start new connection in hedged requests after 50 ms instead of 100 to correspond with previous connect timeout"},
            {"formatdatetime_f_prints_single_zero", true, false, "Improved compatibility with MySQL DATE_FORMAT()/STR_TO_DATE()"},
            {"formatdatetime_parsedatetime_m_is_month_name", false, true, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"}
        });
        addSettingsChanges(settings_changes_history, "23.3",
        {
            {"output_format_parquet_version", "1.0", "2.latest", "Use latest Parquet format version for output format"},
            {"input_format_json_ignore_unknown_keys_in_named_tuple", false, true, "Improve parsing JSON objects as named tuples"},
            {"input_format_native_allow_types_conversion", false, true, "Allow types conversion in Native input forma"},
            {"output_format_arrow_compression_method", "none", "lz4_frame", "Use lz4 compression in Arrow output format by default"},
            {"output_format_parquet_compression_method", "snappy", "lz4", "Use lz4 compression in Parquet output format by default"},
            {"output_format_orc_compression_method", "none", "lz4_frame", "Use lz4 compression in ORC output format by default"},
            {"async_query_sending_for_remote", false, true, "Create connections and send query async across shards"}
        });
        addSettingsChanges(settings_changes_history, "23.2",
        {
            {"output_format_parquet_fixed_string_as_fixed_byte_array", false, true, "Use Parquet FIXED_LENGTH_BYTE_ARRAY type for FixedString by default"},
            {"output_format_arrow_fixed_string_as_fixed_byte_array", false, true, "Use Arrow FIXED_SIZE_BINARY type for FixedString by default"},
            {"query_plan_remove_redundant_distinct", false, true, "Remove redundant Distinct step in query plan"},
            {"optimize_duplicate_order_by_and_distinct", true, false, "Remove duplicate ORDER BY and DISTINCT if it's possible"},
            {"insert_keeper_max_retries", 0, 20, "Enable reconnections to Keeper on INSERT, improve reliability"}
        });
        addSettingsChanges(settings_changes_history, "23.1",
        {
            {"input_format_json_read_objects_as_strings", 0, 1, "Enable reading nested json objects as strings while object type is experimental"},
            {"input_format_json_defaults_for_missing_elements_in_named_tuple", false, true, "Allow missing elements in JSON objects while reading named tuples by default"},
            {"input_format_csv_detect_header", false, true, "Detect header in CSV format by default"},
            {"input_format_tsv_detect_header", false, true, "Detect header in TSV format by default"},
            {"input_format_custom_detect_header", false, true, "Detect header in CustomSeparated format by default"},
            {"query_plan_remove_redundant_sorting", false, true, "Remove redundant sorting in query plan. For example, sorting steps related to ORDER BY clauses in subqueries"}
        });
        addSettingsChanges(settings_changes_history, "22.12",
        {
            {"max_size_to_preallocate_for_aggregation", 10'000'000, 100'000'000, "This optimizes performance"},
            {"query_plan_aggregation_in_order", 0, 1, "Enable some refactoring around query plan"},
            {"format_binary_max_string_size", 0, 1_GiB, "Prevent allocating large amount of memory"}
        });
        addSettingsChanges(settings_changes_history, "22.11",
        {
            {"use_structure_from_insertion_table_in_table_functions", 0, 2, "Improve using structure from insertion table in table functions"}
        });
        addSettingsChanges(settings_changes_history, "22.9",
        {
            {"force_grouping_standard_compatibility", false, true, "Make GROUPING function output the same as in SQL standard and other DBMS"}
        });
        addSettingsChanges(settings_changes_history, "22.7",
        {
            {"cross_to_inner_join_rewrite", 1, 2, "Force rewrite comma join to inner"},
            {"enable_positional_arguments", false, true, "Enable positional arguments feature by default"},
            {"format_csv_allow_single_quotes", true, false, "Most tools don't treat single quote in CSV specially, don't do it by default too"}
        });
        addSettingsChanges(settings_changes_history, "22.6",
        {
            {"output_format_json_named_tuples_as_objects", false, true, "Allow to serialize named tuples as JSON objects in JSON formats by default"},
            {"input_format_skip_unknown_fields", false, true, "Optimize reading subset of columns for some input formats"}
        });
        addSettingsChanges(settings_changes_history, "22.5",
        {
            {"memory_overcommit_ratio_denominator", 0, 1073741824, "Enable memory overcommit feature by default"},
            {"memory_overcommit_ratio_denominator_for_user", 0, 1073741824, "Enable memory overcommit feature by default"}
        });
        addSettingsChanges(settings_changes_history, "22.4",
        {
            {"allow_settings_after_format_in_insert", true, false, "Do not allow SETTINGS after FORMAT for INSERT queries because ClickHouse interpret SETTINGS as some values, which is misleading"}
        });
        addSettingsChanges(settings_changes_history, "22.3",
        {
            {"cast_ipv4_ipv6_default_on_conversion_error", true, false, "Make functions cast(value, 'IPv4') and cast(value, 'IPv6') behave same as toIPv4 and toIPv6 functions"}
        });
        addSettingsChanges(settings_changes_history, "21.12",
        {
            {"stream_like_engine_allow_direct_select", true, false, "Do not allow direct select for Kafka/RabbitMQ/FileLog by default"}
        });
        addSettingsChanges(settings_changes_history, "21.9",
        {
            {"output_format_decimal_trailing_zeros", true, false, "Do not output trailing zeros in text representation of Decimal types by default for better looking output"},
            {"use_hedged_requests", false, true, "Enable Hedged Requests feature by default"}
        });
        addSettingsChanges(settings_changes_history, "21.7",
        {
            {"legacy_column_name_of_tuple_literal", true, false, "Add this setting only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher"}
        });
        addSettingsChanges(settings_changes_history, "21.5",
        {
            {"async_socket_for_remote", false, true, "Fix all problems and turn on asynchronous reads from socket for remote queries by default again"}
        });
        addSettingsChanges(settings_changes_history, "21.3",
        {
            {"async_socket_for_remote", true, false, "Turn off asynchronous reads from socket for remote queries because of some problems"},
            {"optimize_normalize_count_variants", false, true, "Rewrite aggregate functions that semantically equals to count() as count() by default"},
            {"normalize_function_names", false, true, "Normalize function names to their canonical names, this was needed for projection query routing"}
        });
        addSettingsChanges(settings_changes_history, "21.2",
        {
            {"enable_global_with_statement", false, true, "Propagate WITH statements to UNION queries and all subqueries by default"}
        });
        addSettingsChanges(settings_changes_history, "21.1",
        {
            {"insert_quorum_parallel", false, true, "Use parallel quorum inserts by default. It is significantly more convenient to use than sequential quorum inserts"},
            {"input_format_null_as_default", false, true, "Allow to insert NULL as default for input formats by default"},
            {"optimize_on_insert", false, true, "Enable data optimization on INSERT by default for better user experience"},
            {"use_compact_format_in_distributed_parts_names", false, true, "Use compact format for async INSERT into Distributed tables by default"}
        });
        addSettingsChanges(settings_changes_history, "20.10",
        {
            {"format_regexp_escaping_rule", "Escaped", "Raw", "Use Raw as default escaping rule for Regexp format to male the behaviour more like to what users expect"}
        });
        addSettingsChanges(settings_changes_history, "20.7",
        {
            {"show_table_uuid_in_table_create_query_if_not_nil", true, false, "Stop showing  UID of the table in its CREATE query for Engine=Atomic"}
        });
        addSettingsChanges(settings_changes_history, "20.5",
        {
            {"input_format_with_names_use_header", false, true, "Enable using header with names for formats with WithNames/WithNamesAndTypes suffixes"},
            {"allow_suspicious_codecs", true, false, "Don't allow to specify meaningless compression codecs"}
        });
        addSettingsChanges(settings_changes_history, "20.4",
        {
            {"validate_polygons", false, true, "Throw exception if polygon is invalid in function pointInPolygon by default instead of returning possibly wrong results"}
        });
        addSettingsChanges(settings_changes_history, "19.18",
        {
            {"enable_scalar_subquery_optimization", false, true, "Prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once"}
        });
        addSettingsChanges(settings_changes_history, "19.14",
        {
            {"any_join_distinct_right_table_keys", true, false, "Disable ANY RIGHT and ANY FULL JOINs by default to avoid inconsistency"}
        });
        addSettingsChanges(settings_changes_history, "19.12",
        {
            {"input_format_defaults_for_omitted_fields", false, true, "Enable calculation of complex default expressions for omitted fields for some input formats, because it should be the expected behaviour"}
        });
        addSettingsChanges(settings_changes_history, "19.5",
        {
            {"max_partitions_per_insert_block", 0, 100, "Add a limit for the number of partitions in one block"}
        });
        addSettingsChanges(settings_changes_history, "18.12.17",
        {
            {"enable_optimize_predicate_expression", 0, 1, "Optimize predicates to subqueries by default"}
        });
    });
    return settings_changes_history;
}

const VersionToSettingsChangesMap & getMergeTreeSettingsChangesHistory()
{
    static VersionToSettingsChangesMap merge_tree_settings_changes_history;
    static std::once_flag initialized_flag;
    std::call_once(initialized_flag, [&]
    {
        addSettingsChanges(merge_tree_settings_changes_history, "26.3",
        {

        });
        addSettingsChanges(merge_tree_settings_changes_history, "26.2",
        {
            {"clone_replica_zookeeper_create_get_part_batch_size", 1, 100, "New setting"},
            {"add_minmax_index_for_temporal_columns", false, false, "New setting"},
            {"distributed_index_analysis_min_parts_to_activate", 10, 10, "New setting"},
            {"distributed_index_analysis_min_indexes_bytes_to_activate", 1_GiB, 1_GiB, "New setting"},
            {"refresh_statistics_interval", 0, 300, "Enable statistics cache"},
            {"enable_max_bytes_limit_for_min_age_to_force_merge", false, true, "Limit part sizes even with min_age_to_force_merge_seconds by default"},
            {"shared_merge_tree_enable_automatic_empty_partitions_cleanup", false, true, "Enable by default"}
        });
        addSettingsChanges(merge_tree_settings_changes_history, "26.1",
        {
            {"min_columns_to_activate_adaptive_write_buffer", 500, 500, "New setting"},
            {"merge_max_dynamic_subcolumns_in_compact_part", "auto", "auto", "Add a new setting to limit number of dynamic subcolumns in Compact part after merge regardless the parameters specified in the data type"},
            {"materialize_statistics_on_merge", true, true, "New setting"},
            {"escape_index_filenames", false, true, "Escape non-ascii characters in filenames created for indices"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.12",
        {
            {"alter_column_secondary_index_mode", "compatibility", "rebuild", "Change the behaviour to allow ALTER `column` when they have dependent secondary indices"},
            {"merge_selector_enable_heuristic_to_lower_max_parts_to_merge_at_once", false, false, "New setting"},
            {"merge_selector_heuristic_to_lower_max_parts_to_merge_at_once_exponent", 5, 5, "New setting"},
            {"nullable_serialization_version", "basic", "basic", "New setting"},
            {"object_serialization_version", "v2", "v3", "Enable v3 serialization version for JSON by default to use advanced shared data serialization"},
            {"dynamic_serialization_version", "v2", "v3", "Enable v3 serialization version for Dynamic by default for better serialization/deserialization"},
            {"object_shared_data_serialization_version", "map", "advanced", "Enable advanced shared data serialization version by default"},
            {"object_shared_data_serialization_version_for_zero_level_parts", "map", "map_with_buckets", "Enable map_with_buckets shared data serialization version for zero level parts by default"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.11",
        {
            {"merge_max_dynamic_subcolumns_in_wide_part", "auto", "auto", "Add a new setting to limit number of dynamic subcolumns in Wide part after merge regardless the parameters specified in the data type"},
            {"refresh_statistics_interval", 0, 0, "New setting"},
            {"shared_merge_tree_create_per_replica_metadata_nodes", true, false, "Reduce the amount of metadata in Keeper."},
            {"serialization_info_version", "basic", "with_types", "Change to the newer format allowing custom string serialization"},
            {"string_serialization_version", "single_stream", "with_size_stream", "Change to the newer format with separate sizes"},
            {"escape_variant_subcolumn_filenames", false, true, "Escape special symbols for filenames created for Variant type subcolumns in Wide parts"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.10",
        {
            {"auto_statistics_types", "", "", "New setting"},
            {"exclude_materialize_skip_indexes_on_merge", "", "", "New setting."},
            {"serialization_info_version", "basic", "basic", "New setting"},
            {"string_serialization_version", "single_stream", "single_stream", "New setting"},
            {"replicated_deduplication_window_seconds", 7 * 24 * 60 * 60, 60*60, "decrease default value"},
            {"shared_merge_tree_activate_coordinated_merges_tasks", false, false, "New settings"},
            {"shared_merge_tree_merge_coordinator_factor", 1.1f, 1.1f, "Lower coordinator sleep time after load"},
            {"min_level_for_wide_part", 0, 0, "New setting"},
            {"min_level_for_full_part_storage", 0, 0, "New setting"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.9",
        {
            {"vertical_merge_optimize_lightweight_delete", false, true, "New setting"},
            {"replicated_deduplication_window", 1000, 10000, "increase default value"},
            {"shared_merge_tree_enable_automatic_empty_partitions_cleanup", false, false, "New setting"},
            {"shared_merge_tree_empty_partition_lifetime", 86400, 86400, "New setting"},
            {"shared_merge_tree_outdated_parts_group_size", 2, 2, "New setting"},
            {"shared_merge_tree_use_outdated_parts_compact_format", false, true, "Enable outdated parts v3 by default"},
            {"shared_merge_tree_activate_coordinated_merges_tasks", false, false, "New settings"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.8",
        {
            {"object_serialization_version", "v2", "v2", "Add a setting to control JSON serialization versions"},
            {"object_shared_data_serialization_version", "map", "map", "Add a setting to control JSON serialization versions"},
            {"object_shared_data_serialization_version_for_zero_level_parts", "map", "map", "Add a setting to control JSON serialization versions  for zero level parts"},
            {"object_shared_data_buckets_for_compact_part", 8, 8, "Add a setting to control number of buckets for shared data in JSON serialization in compact parts"},
            {"object_shared_data_buckets_for_wide_part", 32, 32, "Add a setting to control number of buckets for shared data in JSON serialization in wide parts"},
            {"dynamic_serialization_version", "v2", "v2", "Add a setting to control Dynamic serialization versions"},
            {"search_orphaned_parts_disks", "any", "any", "New setting"},
            {"shared_merge_tree_virtual_parts_discovery_batch", 1, 1, "New setting"},
            {"max_digestion_size_per_segment", 256_MiB, 256_MiB, "Obsolete setting"},
            {"shared_merge_tree_update_replica_flags_delay_ms", 30000, 30000, "New setting"},
            {"write_marks_for_substreams_in_compact_parts", false, true, "Enable writing marks for substreams in compact parts by default"},
            {"allow_part_offset_column_in_projections", false, true, "Now projections can use _part_offset column."},
            {"max_uncompressed_bytes_in_patches", 0, 30ULL * 1024 * 1024 * 1024, "New setting"},
            {"shared_merge_tree_activate_coordinated_merges_tasks", false, false, "New settings"},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.7",
        {
            /// RELEASE CLOSED
            {"shared_merge_tree_activate_coordinated_merges_tasks", false, false, "New settings"},
            /// RELEASE CLOSED
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.6",
        {
            /// RELEASE CLOSED
            {"cache_populated_by_fetch_filename_regexp", "", "", "New setting"},
            {"allow_coalescing_columns_in_partition_or_order_key", false, false, "New setting to allow coalescing of partition or sorting key columns."},
            {"shared_merge_tree_activate_coordinated_merges_tasks", false, false, "New settings"},
            /// RELEASE CLOSED
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.5",
        {
            /// Release closed. Please use 25.6
            {"shared_merge_tree_enable_coordinated_merges", false, false, "New setting"},
            {"shared_merge_tree_merge_coordinator_merges_prepare_count", 100, 100, "New setting"},
            {"shared_merge_tree_merge_coordinator_fetch_fresh_metadata_period_ms", 10000, 10000, "New setting"},
            {"shared_merge_tree_merge_coordinator_max_merge_request_size", 20, 20, "New setting"},
            {"shared_merge_tree_merge_coordinator_election_check_period_ms", 30000, 30000, "New setting"},
            {"shared_merge_tree_merge_coordinator_min_period_ms", 1, 1, "New setting"},
            {"shared_merge_tree_merge_coordinator_max_period_ms", 10000, 10000, "New setting"},
            {"shared_merge_tree_merge_coordinator_factor", 1.1f, 1.1f, "New setting"},
            {"shared_merge_tree_merge_worker_fast_timeout_ms", 100, 100, "New setting"},
            {"shared_merge_tree_merge_worker_regular_timeout_ms", 10000, 10000, "New setting"},
            {"apply_patches_on_merge", true, true, "New setting"},
            {"remove_unused_patch_parts", true, true, "New setting"},
            {"write_marks_for_substreams_in_compact_parts", false, false, "New setting"},
            /// Release closed. Please use 25.6
            {"allow_part_offset_column_in_projections", false, false, "New setting, it protects from creating projections with parent part offset column until it is stabilized."},
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.4",
        {
            /// Release closed. Please use 25.5
            {"max_postpone_time_for_failed_replicated_fetches_ms", 0, 1ULL * 60 * 1000, "Added new setting to enable postponing fetch tasks in the replication queue."},
            {"max_postpone_time_for_failed_replicated_merges_ms", 0, 1ULL * 60 * 1000, "Added new setting to enable postponing merge tasks in the replication queue."},
            {"max_postpone_time_for_failed_replicated_tasks_ms", 0, 5ULL * 60 * 1000, "Added new setting to enable postponing tasks in the replication queue."},
            {"default_compression_codec", "", "", "New setting"},
            {"refresh_parts_interval", 0, 0, "A new setting"},
            {"max_merge_delayed_streams_for_parallel_write", 40, 40, "New setting"},
            {"allow_summing_columns_in_partition_or_order_key", true, false, "New setting to allow summing of partition or sorting key columns"},
            /// Release closed. Please use 25.5
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.3",
        {
            /// Release closed. Please use 25.4
            {"shared_merge_tree_enable_keeper_parts_extra_data", false, false, "New setting"},
            {"zero_copy_merge_mutation_min_parts_size_sleep_no_scale_before_lock", 0, 0, "New setting"},
            {"enable_replacing_merge_with_cleanup_for_min_age_to_force_merge", false, false, "New setting to allow automatic cleanup merges for ReplacingMergeTree"},
            /// Release closed. Please use 25.4
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.2",
        {
            /// Release closed. Please use 25.3
            {"shared_merge_tree_initial_parts_update_backoff_ms", 50, 50, "New setting"},
            {"shared_merge_tree_max_parts_update_backoff_ms", 5000, 5000, "New setting"},
            {"shared_merge_tree_interserver_http_connection_timeout_ms", 100, 100, "New setting"},
            {"columns_and_secondary_indices_sizes_lazy_calculation", true, true, "New setting to calculate columns and indices sizes lazily"},
            {"table_disk", false, false, "New setting"},
            {"allow_reduce_blocking_parts_task", false, true, "Now SMT will remove stale blocking parts from ZooKeeper by default"},
            {"shared_merge_tree_max_suspicious_broken_parts", 0, 0, "Max broken parts for SMT, if more - deny automatic detach"},
            {"shared_merge_tree_max_suspicious_broken_parts_bytes", 0, 0, "Max size of all broken parts for SMT, if more - deny automatic detach"},
            /// Release closed. Please use 25.3
        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.1",
        {
            /// Release closed. Please use 25.2
            {"shared_merge_tree_try_fetch_part_in_memory_data_from_replicas", false, false, "New setting to fetch parts data from other replicas"},
            {"enable_max_bytes_limit_for_min_age_to_force_merge", false, false, "Added new setting to limit max bytes for min_age_to_force_merge."},
            {"enable_max_bytes_limit_for_min_age_to_force_merge", false, false, "New setting"},
            {"add_minmax_index_for_numeric_columns", false, false, "New setting"},
            {"add_minmax_index_for_string_columns", false, false, "New setting"},
            {"materialize_skip_indexes_on_merge", true, true, "New setting"},
            {"merge_max_bytes_to_prewarm_cache", 1ULL * 1024 * 1024 * 1024, 1ULL * 1024 * 1024 * 1024, "Cloud sync"},
            {"merge_total_max_bytes_to_prewarm_cache", 15ULL * 1024 * 1024 * 1024, 15ULL * 1024 * 1024 * 1024, "Cloud sync"},
            {"reduce_blocking_parts_sleep_ms", 5000, 5000, "Cloud sync"},
            {"number_of_partitions_to_consider_for_merge", 10, 10, "Cloud sync"},
            {"shared_merge_tree_enable_outdated_parts_check", true, true, "Cloud sync"},
            {"shared_merge_tree_max_parts_update_leaders_in_total", 6, 6, "Cloud sync"},
            {"shared_merge_tree_max_parts_update_leaders_per_az", 2, 2, "Cloud sync"},
            {"shared_merge_tree_leader_update_period_seconds", 30, 30, "Cloud sync"},
            {"shared_merge_tree_leader_update_period_random_add_seconds", 10, 10, "Cloud sync"},
            {"shared_merge_tree_read_virtual_parts_from_leader", true, true, "Cloud sync"},
            {"shared_merge_tree_interserver_http_timeout_ms", 10000, 10000, "Cloud sync"},
            {"shared_merge_tree_max_replicas_for_parts_deletion", 10, 10, "Cloud sync"},
            {"shared_merge_tree_max_replicas_to_merge_parts_for_each_parts_range", 5, 5, "Cloud sync"},
            {"shared_merge_tree_use_outdated_parts_compact_format", false, false, "Cloud sync"},
            {"shared_merge_tree_memo_ids_remove_timeout_seconds", 1800, 1800, "Cloud sync"},
            {"shared_merge_tree_idle_parts_update_seconds", 3600, 3600, "Cloud sync"},
            {"shared_merge_tree_max_outdated_parts_to_process_at_once", 1000, 1000, "Cloud sync"},
            {"shared_merge_tree_postpone_next_merge_for_locally_merged_parts_rows_threshold", 1000000, 1000000, "Cloud sync"},
            {"shared_merge_tree_postpone_next_merge_for_locally_merged_parts_ms", 0, 0, "Cloud sync"},
            {"shared_merge_tree_range_for_merge_window_size", 10, 10, "Cloud sync"},
            {"shared_merge_tree_use_too_many_parts_count_from_virtual_parts", 0, 0, "Cloud sync"},
            {"shared_merge_tree_create_per_replica_metadata_nodes", true, true, "Cloud sync"},
            {"shared_merge_tree_use_metadata_hints_cache", true, true, "Cloud sync"},
            {"notify_newest_block_number", false, false, "Cloud sync"},
            {"allow_reduce_blocking_parts_task", false, false, "Cloud sync"},
            /// Release closed. Please use 25.2
        });
        addSettingsChanges(merge_tree_settings_changes_history, "24.12",
        {
            /// Release closed. Please use 25.1
            {"enforce_index_structure_match_on_partition_manipulation", true, false, "New setting"},
            {"use_primary_key_cache", false, false, "New setting"},
            {"prewarm_primary_key_cache", false, false, "New setting"},
            {"min_bytes_to_prewarm_caches", 0, 0, "New setting"},
            {"allow_experimental_reverse_key", false, false, "New setting"},
            /// Release closed. Please use 25.1
        });
        addSettingsChanges(merge_tree_settings_changes_history, "24.11",
        {
        });
        addSettingsChanges(merge_tree_settings_changes_history, "24.10",
        {
        });
        addSettingsChanges(merge_tree_settings_changes_history, "24.9",
        {
        });
        addSettingsChanges(merge_tree_settings_changes_history, "24.8",
        {
            {"deduplicate_merge_projection_mode", "ignore", "throw", "Do not allow to create inconsistent projection"}
        });
    });

    return merge_tree_settings_changes_history;
}

}
