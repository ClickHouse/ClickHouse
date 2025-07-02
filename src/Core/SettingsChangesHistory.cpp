#include <Core/Defines.h>
#include <Core/SettingsChangesHistory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <boost/algorithm/string.hpp>
#include <Core/SettingsEnums.h>

#include <fmt/ranges.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

ClickHouseVersion::ClickHouseVersion(std::string_view version)
{
    Strings split;
    boost::split(split, version, [](char c){ return c == '.'; });
    components.reserve(split.size());
    if (split.empty())
        throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};

    for (const auto & split_element : split)
    {
        size_t component;
        ReadBufferFromString buf(split_element);
        if (!tryReadIntText(component, buf) || !buf.eof())
            throw Exception{ErrorCodes::BAD_ARGUMENTS, "Cannot parse ClickHouse version here: {}", version};
        components.push_back(component);
    }
}

String ClickHouseVersion::toString() const
{
    return fmt::format("{}", fmt::join(components, "."));
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
        addSettingsChanges(settings_changes_history, "25.7",
        {
            {"correlated_subqueries_substitute_equivalent_expressions", false, true, "New setting to correlated subquery planning optimization."},
            {"function_date_trunc_return_type_behavior", 0, 0, "Add new setting to preserve old behaviour of dateTrunc function"},
            {"output_format_parquet_geometadata", false, true, "A new setting to allow to write information about geo columns in parquet metadata and encode columns in WKB format."},
            {"cluster_function_process_archive_on_multiple_nodes", true, true, "New setting"},
            {"distributed_plan_max_rows_to_broadcast", 20000, 20000, "New experimental setting."},
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
            {"apply_patch_parts", false, true, "A new setting"},
            {"allow_experimental_lightweight_update", false, false, "A new setting"},
            {"allow_experimental_delta_kernel_rs", true, true, "New setting"},
            {"allow_experimental_database_hms_catalog", false, false, "Allow experimental database engine DataLakeCatalog with catalog_type = 'hive'"},
            {"vector_search_filter_strategy", "auto", "auto", "New setting"},
            {"vector_search_postfilter_multiplier", 1, 1, "New setting"},
            {"compile_expressions", false, true, "We believe that the LLVM infrastructure behind the JIT compiler is stable enough to enable this setting by default."},
            {"input_format_parquet_bloom_filter_push_down", false, true, "When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and bloom filter in the Parquet metadata."},
            {"input_format_parquet_allow_geoparquet_parser", false, true, "A new setting to use geo columns in parquet file"},
            {"enable_url_encoding", true, false, "Changed existing setting's default value"},
            {"s3_slow_all_threads_after_network_error", false, true, "New setting"},
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
            {"function_date_trunc_return_type_behavior", 1, 0, "Change the result type for dateTrunc function for DateTime64/Date32 arguments to DateTime64/Date32 regardless of time unit to get correct result for negative values"}
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
            {"query_plan_merge_filters", false, true, "Allow to merge filters in the query plan. This is required to properly support filter-push-down with a new analyzer."},
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
            {"allow_experimental_full_text_index", false, false, "Enable experimental full-text index"},
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
            {"temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds", (10 * 60 * 1000), (10 * 60 * 1000), "Wait time to lock cache for sapce reservation in temporary data in filesystem cache"},
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
            {"filesystem_cache_reserve_space_wait_lock_timeout_milliseconds", 1000, 1000, "Wait time to lock cache for sapce reservation in filesystem cache"},
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
        addSettingsChanges(merge_tree_settings_changes_history, "25.7",
        {

        });
        addSettingsChanges(merge_tree_settings_changes_history, "25.6",
        {
            /// RELEASE CLOSED
            {"cache_populated_by_fetch_filename_regexp", "", "", "New setting"},
            {"allow_coalescing_columns_in_partition_or_order_key", false, false, "New setting to allow coalescing of partition or sorting key columns."},
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
            {"shared_merge_tree_merge_coordinator_factor", 2, 2, "New setting"},
            {"shared_merge_tree_merge_worker_fast_timeout_ms", 100, 100, "New setting"},
            {"shared_merge_tree_merge_worker_regular_timeout_ms", 10000, 10000, "New setting"},
            {"apply_patches_on_merge", true, true, "New setting"},
            {"remove_unused_patch_parts", true, true, "New setting"},
            {"write_marks_for_substreams_in_compact_parts", false, false, "New setting"},
            /// Release closed. Please use 25.6
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
