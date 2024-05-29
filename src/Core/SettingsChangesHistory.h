#pragma once

#include <Core/Field.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <boost/algorithm/string.hpp>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ClickHouseVersion
{
public:
    ClickHouseVersion(const String & version) /// NOLINT(google-explicit-constructor)
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

    ClickHouseVersion(const char * version) : ClickHouseVersion(String(version)) {} /// NOLINT(google-explicit-constructor)

    String toString() const
    {
        String version = std::to_string(components[0]);
        for (size_t i = 1; i < components.size(); ++i)
            version += "." + std::to_string(components[i]);

        return version;
    }

    bool operator<(const ClickHouseVersion & other) const
    {
        return components < other.components;
    }

    bool operator>=(const ClickHouseVersion & other) const
    {
        return components >= other.components;
    }

private:
    std::vector<size_t> components;
};

namespace SettingsChangesHistory
{
    struct SettingChange
    {
        String name;
        Field previous_value;
        Field new_value;
        String reason;
    };

    using SettingsChanges = std::vector<SettingChange>;
}

/// History of settings changes that controls some backward incompatible changes
/// across all ClickHouse versions. It maps ClickHouse version to settings changes that were done
/// in this version. This history contains both changes to existing settings and newly added settings.
/// Settings changes is a vector of structs
///     {setting_name, previous_value, new_value, reason}.
/// For newly added setting choose the most appropriate previous_value (for example, if new setting
/// controls new feature and it's 'true' by default, use 'false' as previous_value).
/// It's used to implement `compatibility` setting (see https://github.com/ClickHouse/ClickHouse/issues/35972)
static std::map<ClickHouseVersion, SettingsChangesHistory::SettingsChanges> settings_changes_history =
{
    {"24.6", {{"input_format_parquet_use_native_reader", false, false, "When reading Parquet files, to use native reader instead of arrow reader."},
              {"hdfs_throw_on_zero_files_match", false, false, "Allow to throw an error when ListObjects request cannot match any files in HDFS engine instead of empty query result"},
              {"azure_throw_on_zero_files_match", false, false, "Allow to throw an error when ListObjects request cannot match any files in AzureBlobStorage engine instead of empty query result"},
              {"s3_validate_request_settings", true, true, "Allow to disable S3 request settings validation"},
              {"azure_skip_empty_files", false, false, "Allow to skip empty files in azure table engine"},
              {"hdfs_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in HDFS table engine"},
              {"azure_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in AzureBlobStorage table engine"},
              {"s3_ignore_file_doesnt_exist", false, false, "Allow to return 0 rows when the requested files don't exist instead of throwing an exception in S3 table engine"},
              }},
    {"24.5", {{"allow_deprecated_error_prone_window_functions", true, false, "Allow usage of deprecated error prone window functions (neighbor, runningAccumulate, runningDifferenceStartingWithFirstValue, runningDifference)"},
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
              }},
    {"24.4", {{"input_format_json_throw_on_bad_escape_sequence", true, true, "Allow to save JSON strings with bad escape sequences"},
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
              }},
    {"24.3", {{"s3_connect_timeout_ms", 1000, 1000, "Introduce new dedicated setting for s3 connection timeout"},
              {"allow_experimental_shared_merge_tree", false, true, "The setting is obsolete"},
              {"use_page_cache_for_disks_without_file_cache", false, false, "Added userspace page cache"},
              {"read_from_page_cache_if_exists_otherwise_bypass_cache", false, false, "Added userspace page cache"},
              {"page_cache_inject_eviction", false, false, "Added userspace page cache"},
              {"default_table_engine", "None", "MergeTree", "Set default table engine to MergeTree for better usability"},
              {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", false, false, "Allow to use String type for ambiguous paths during named tuple inference from JSON objects"},
              {"traverse_shadow_remote_data_paths", false, false, "Traverse shadow directory when query system.remote_data_paths."},
              {"throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert", false, true, "Deduplication is dependent materialized view cannot work together with async inserts."},
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
              }},
    {"24.2", {{"allow_suspicious_variant_types", true, false, "Don't allow creating Variant type with suspicious variants by default"},
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
              }},
    {"24.1", {{"print_pretty_type_names", false, true, "Better user experience."},
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
              {"split_intersecting_parts_ranges_into_layers_final", true, true, "Allow to split intersecting parts ranges into layers during FINAL optimization"}}},
    {"23.12", {{"allow_suspicious_ttl_expressions", true, false, "It is a new setting, and in previous versions the behavior was equivalent to allowing."},
              {"input_format_parquet_allow_missing_columns", false, true, "Allow missing columns in Parquet files by default"},
              {"input_format_orc_allow_missing_columns", false, true, "Allow missing columns in ORC files by default"},
              {"input_format_arrow_allow_missing_columns", false, true, "Allow missing columns in Arrow files by default"}}},
    {"23.9", {{"optimize_group_by_constant_keys", false, true, "Optimize group by constant keys by default"},
              {"input_format_json_try_infer_named_tuples_from_objects", false, true, "Try to infer named Tuples from JSON objects by default"},
              {"input_format_json_read_numbers_as_strings", false, true, "Allow to read numbers as strings in JSON formats by default"},
              {"input_format_json_read_arrays_as_strings", false, true, "Allow to read arrays as strings in JSON formats by default"},
              {"input_format_json_infer_incomplete_types_as_strings", false, true, "Allow to infer incomplete types as Strings in JSON formats by default"},
              {"input_format_json_try_infer_numbers_from_strings", true, false, "Don't infer numbers from strings in JSON formats by default to prevent possible parsing errors"},
              {"http_write_exception_in_output_format", false, true, "Output valid JSON/XML on exception in HTTP streaming."}}},
    {"23.8", {{"rewrite_count_distinct_if_with_count_distinct_implementation", false, true, "Rewrite countDistinctIf with count_distinct_implementation configuration"}}},
    {"23.7", {{"function_sleep_max_microseconds_per_block", 0, 3000000, "In previous versions, the maximum sleep time of 3 seconds was applied only for `sleep`, but not for `sleepEachRow` function. In the new version, we introduce this setting. If you set compatibility with the previous versions, we will disable the limit altogether."}}},
    {"23.6", {{"http_send_timeout", 180, 30, "3 minutes seems crazy long. Note that this is timeout for a single network write call, not for the whole upload operation."},
              {"http_receive_timeout", 180, 30, "See http_send_timeout."}}},
    {"23.5", {{"input_format_parquet_preserve_order", true, false, "Allow Parquet reader to reorder rows for better parallelism."},
              {"parallelize_output_from_storages", false, true, "Allow parallelism when executing queries that read from file/url/s3/etc. This may reorder rows."},
              {"use_with_fill_by_sorting_prefix", false, true, "Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently"},
              {"output_format_parquet_compliant_nested_types", false, true, "Change an internal field name in output Parquet file schema."}}},
    {"23.4", {{"allow_suspicious_indices", true, false, "If true, index can defined with identical expressions"},
              {"allow_nonconst_timezone_arguments", true, false, "Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()."},
              {"connect_timeout_with_failover_ms", 50, 1000, "Increase default connect timeout because of async connect"},
              {"connect_timeout_with_failover_secure_ms", 100, 1000, "Increase default secure connect timeout because of async connect"},
              {"hedged_connection_timeout_ms", 100, 50, "Start new connection in hedged requests after 50 ms instead of 100 to correspond with previous connect timeout"}}},
    {"23.3", {{"output_format_parquet_version", "1.0", "2.latest", "Use latest Parquet format version for output format"},
              {"input_format_json_ignore_unknown_keys_in_named_tuple", false, true, "Improve parsing JSON objects as named tuples"},
              {"input_format_native_allow_types_conversion", false, true, "Allow types conversion in Native input forma"},
              {"output_format_arrow_compression_method", "none", "lz4_frame", "Use lz4 compression in Arrow output format by default"},
              {"output_format_parquet_compression_method", "snappy", "lz4", "Use lz4 compression in Parquet output format by default"},
              {"output_format_orc_compression_method", "none", "lz4_frame", "Use lz4 compression in ORC output format by default"},
              {"async_query_sending_for_remote", false, true, "Create connections and send query async across shards"}}},
    {"23.2", {{"output_format_parquet_fixed_string_as_fixed_byte_array", false, true, "Use Parquet FIXED_LENGTH_BYTE_ARRAY type for FixedString by default"},
              {"output_format_arrow_fixed_string_as_fixed_byte_array", false, true, "Use Arrow FIXED_SIZE_BINARY type for FixedString by default"},
              {"query_plan_remove_redundant_distinct", false, true, "Remove redundant Distinct step in query plan"},
              {"optimize_duplicate_order_by_and_distinct", true, false, "Remove duplicate ORDER BY and DISTINCT if it's possible"},
              {"insert_keeper_max_retries", 0, 20, "Enable reconnections to Keeper on INSERT, improve reliability"}}},
    {"23.1", {{"input_format_json_read_objects_as_strings", 0, 1, "Enable reading nested json objects as strings while object type is experimental"},
              {"input_format_json_defaults_for_missing_elements_in_named_tuple", false, true, "Allow missing elements in JSON objects while reading named tuples by default"},
              {"input_format_csv_detect_header", false, true, "Detect header in CSV format by default"},
              {"input_format_tsv_detect_header", false, true, "Detect header in TSV format by default"},
              {"input_format_custom_detect_header", false, true, "Detect header in CustomSeparated format by default"},
              {"query_plan_remove_redundant_sorting", false, true, "Remove redundant sorting in query plan. For example, sorting steps related to ORDER BY clauses in subqueries"}}},
    {"22.12", {{"max_size_to_preallocate_for_aggregation", 10'000'000, 100'000'000, "This optimizes performance"},
               {"query_plan_aggregation_in_order", 0, 1, "Enable some refactoring around query plan"},
               {"format_binary_max_string_size", 0, 1_GiB, "Prevent allocating large amount of memory"}}},
    {"22.11", {{"use_structure_from_insertion_table_in_table_functions", 0, 2, "Improve using structure from insertion table in table functions"}}},
    {"23.4", {{"formatdatetime_f_prints_single_zero", true, false, "Improved compatibility with MySQL DATE_FORMAT()/STR_TO_DATE()"}}},
    {"23.4", {{"formatdatetime_parsedatetime_m_is_month_name", false, true, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"}}},
    {"23.11", {{"parsedatetime_parse_without_leading_zeros", false, true, "Improved compatibility with MySQL DATE_FORMAT/STR_TO_DATE"}}},
    {"22.9", {{"force_grouping_standard_compatibility", false, true, "Make GROUPING function output the same as in SQL standard and other DBMS"}}},
    {"22.7", {{"cross_to_inner_join_rewrite", 1, 2, "Force rewrite comma join to inner"},
              {"enable_positional_arguments", false, true, "Enable positional arguments feature by default"},
              {"format_csv_allow_single_quotes", true, false, "Most tools don't treat single quote in CSV specially, don't do it by default too"}}},
    {"22.6", {{"output_format_json_named_tuples_as_objects", false, true, "Allow to serialize named tuples as JSON objects in JSON formats by default"},
              {"input_format_skip_unknown_fields", false, true, "Optimize reading subset of columns for some input formats"}}},
    {"22.5", {{"memory_overcommit_ratio_denominator", 0, 1073741824, "Enable memory overcommit feature by default"},
              {"memory_overcommit_ratio_denominator_for_user", 0, 1073741824, "Enable memory overcommit feature by default"}}},
    {"22.4", {{"allow_settings_after_format_in_insert", true, false, "Do not allow SETTINGS after FORMAT for INSERT queries because ClickHouse interpret SETTINGS as some values, which is misleading"}}},
    {"22.3", {{"cast_ipv4_ipv6_default_on_conversion_error", true, false, "Make functions cast(value, 'IPv4') and cast(value, 'IPv6') behave same as toIPv4 and toIPv6 functions"}}},
    {"21.12", {{"stream_like_engine_allow_direct_select", true, false, "Do not allow direct select for Kafka/RabbitMQ/FileLog by default"}}},
    {"21.9", {{"output_format_decimal_trailing_zeros", true, false, "Do not output trailing zeros in text representation of Decimal types by default for better looking output"},
              {"use_hedged_requests", false, true, "Enable Hedged Requests feature by default"}}},
    {"21.7", {{"legacy_column_name_of_tuple_literal", true, false, "Add this setting only for compatibility reasons. It makes sense to set to 'true', while doing rolling update of cluster from version lower than 21.7 to higher"}}},
    {"21.5", {{"async_socket_for_remote", false, true, "Fix all problems and turn on asynchronous reads from socket for remote queries by default again"}}},
    {"21.3", {{"async_socket_for_remote", true, false, "Turn off asynchronous reads from socket for remote queries because of some problems"},
              {"optimize_normalize_count_variants", false, true, "Rewrite aggregate functions that semantically equals to count() as count() by default"},
              {"normalize_function_names", false, true, "Normalize function names to their canonical names, this was needed for projection query routing"}}},
    {"21.2", {{"enable_global_with_statement", false, true, "Propagate WITH statements to UNION queries and all subqueries by default"}}},
    {"21.1", {{"insert_quorum_parallel", false, true, "Use parallel quorum inserts by default. It is significantly more convenient to use than sequential quorum inserts"},
              {"input_format_null_as_default", false, true, "Allow to insert NULL as default for input formats by default"},
              {"optimize_on_insert", false, true, "Enable data optimization on INSERT by default for better user experience"},
              {"use_compact_format_in_distributed_parts_names", false, true, "Use compact format for async INSERT into Distributed tables by default"}}},
    {"20.10", {{"format_regexp_escaping_rule", "Escaped", "Raw", "Use Raw as default escaping rule for Regexp format to male the behaviour more like to what users expect"}}},
    {"20.7", {{"show_table_uuid_in_table_create_query_if_not_nil", true, false, "Stop showing  UID of the table in its CREATE query for Engine=Atomic"}}},
    {"20.5", {{"input_format_with_names_use_header", false, true, "Enable using header with names for formats with WithNames/WithNamesAndTypes suffixes"},
              {"allow_suspicious_codecs", true, false, "Don't allow to specify meaningless compression codecs"}}},
    {"20.4", {{"validate_polygons", false, true, "Throw exception if polygon is invalid in function pointInPolygon by default instead of returning possibly wrong results"}}},
    {"19.18", {{"enable_scalar_subquery_optimization", false, true, "Prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once"}}},
    {"19.14", {{"any_join_distinct_right_table_keys", true, false, "Disable ANY RIGHT and ANY FULL JOINs by default to avoid inconsistency"}}},
    {"19.12", {{"input_format_defaults_for_omitted_fields", false, true, "Enable calculation of complex default expressions for omitted fields for some input formats, because it should be the expected behaviour"}}},
    {"19.5", {{"max_partitions_per_insert_block", 0, 100, "Add a limit for the number of partitions in one block"}}},
    {"18.12.17", {{"enable_optimize_predicate_expression", 0, 1, "Optimize predicates to subqueries by default"}}},
};

}
