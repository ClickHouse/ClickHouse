#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static std::vector<std::string> settings_timezones;

std::map<std::string, CHSetting> serverSettings = {
    {"aggregate_functions_null_for_empty", CHSetting(trueOrFalse, {"0", "1"})},
    {"aggregation_in_order_max_block_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"0", "1", "100", "10000"})},
    {"allow_aggregate_partitions_independently", CHSetting(trueOrFalse, {"0", "1"})},
    {"allow_asynchronous_read_from_io_pool_for_merge_tree", CHSetting(trueOrFalse, {"0", "1"})},
    {"allow_changing_replica_until_first_data_packet", CHSetting(trueOrFalse, {})},
    {"allow_create_index_without_type", CHSetting(trueOrFalse, {})},
    {"allow_experimental_parallel_reading_from_replicas", CHSetting(zeroOneTwo, {"0", "1"})},
    {"allow_introspection_functions", CHSetting(trueOrFalse, {"0", "1"})},
    {"allow_prefetched_read_pool_for_remote_filesystem", CHSetting(trueOrFalse, {})},
    {"allow_reorder_prewhere_conditions", CHSetting(trueOrFalse, {"0", "1"})},
    {"alter_move_to_space_execute_async", CHSetting(trueOrFalse, {})},
    {"alter_partition_verbose_result", CHSetting(trueOrFalse, {})},
    {"alter_sync", CHSetting(zeroOneTwo, {})},
    {"analyze_index_with_space_filling_curves", CHSetting(trueOrFalse, {"0", "1"})},
    {"analyzer_compatibility_join_using_top_level_identifier", CHSetting(trueOrFalse, {"0", "1"})},
    {"apply_deleted_mask", CHSetting(trueOrFalse, {})},
    {"apply_mutations_on_fly", CHSetting(trueOrFalse, {})},
    {"any_join_distinct_right_table_keys", CHSetting(trueOrFalse, {"0", "1"})},
    {"asterisk_include_alias_columns", CHSetting(trueOrFalse, {})},
    {"async_insert", CHSetting(trueOrFalse, {})},
    {"async_insert_deduplicate", CHSetting(trueOrFalse, {})},
    {"async_insert_threads",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); },
         {})},
    {"async_insert_use_adaptive_busy_timeout", CHSetting(trueOrFalse, {})},
    {"async_query_sending_for_remote", CHSetting(trueOrFalse, {"0", "1"})},
    {"async_socket_for_remote", CHSetting(trueOrFalse, {"0", "1"})},
    {"calculate_text_stack_trace", CHSetting(trueOrFalse, {})},
    {"cancel_http_readonly_queries_on_client_close", CHSetting(trueOrFalse, {})},
    {"cast_ipv4_ipv6_default_on_conversion_error", CHSetting(trueOrFalse, {})},
    {"cast_keep_nullable", CHSetting(trueOrFalse, {})},
    {"cast_string_to_dynamic_use_inference", CHSetting(trueOrFalse, {})},
    {"check_query_single_value_result", CHSetting(trueOrFalse, {"0", "1"})},
    {"check_referential_table_dependencies", CHSetting(trueOrFalse, {})},
    {"check_table_dependencies", CHSetting(trueOrFalse, {})},
    {"checksum_on_read", CHSetting(trueOrFalse, {})},
    {"cloud_mode", CHSetting(trueOrFalse, {"0", "1"})},
    {"collect_hash_table_stats_during_aggregation", CHSetting(trueOrFalse, {"0", "1"})},
    {"collect_hash_table_stats_during_joins", CHSetting(trueOrFalse, {"0", "1"})},
    {"compatibility_ignore_auto_increment_in_create_table", CHSetting(trueOrFalse, {})},
    {"compatibility_ignore_collation_in_create_table", CHSetting(trueOrFalse, {})},
    {"compile_aggregate_expressions", CHSetting(trueOrFalse, {"0", "1"})},
    {"compile_expressions", CHSetting(trueOrFalse, {"0", "1"})},
    {"compile_sort_description", CHSetting(trueOrFalse, {"0", "1"})},
    {"composed_data_type_output_format_mode",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += "'";
             ret += rg.nextBool() ? "default" : "spark";
             ret += "'";
         },
         {})},
    {"convert_query_to_cnf", CHSetting(trueOrFalse, {})},
    {"count_distinct_optimization", CHSetting(trueOrFalse, {"0", "1"})},
    {"create_table_empty_primary_key_by_default", CHSetting(trueOrFalse, {})},
    {"cross_join_min_bytes_to_compress",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "100000000"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {"0", "1", "10000"})},
    {"cross_join_min_rows_to_compress",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "100000000"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {"0", "1", "100", "10000"})},
    {"database_atomic_wait_for_drop_and_detach_synchronously", CHSetting(trueOrFalse, {})},
    {"database_replicated_allow_heavy_create", CHSetting(trueOrFalse, {})},
    {"database_replicated_enforce_synchronous_settings", CHSetting(trueOrFalse, {})},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", CHSetting(trueOrFalse, {})},
    {"date_time_output_format",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices
                 = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {})},
    {"decimal_check_overflow", CHSetting(trueOrFalse, {})},
    //{"deduplicate_blocks_in_dependent_materialized_views", CHSetting(trueOrFalse, {})},
    //{"describe_compact_output", CHSetting(trueOrFalse, {})},
    {"describe_extend_object_types", CHSetting(trueOrFalse, {})},
    {"describe_include_subcolumns", CHSetting(trueOrFalse, {})},
    {"dictionary_use_async_executor", CHSetting(trueOrFalse, {})},
    {"distributed_aggregation_memory_efficient", CHSetting(trueOrFalse, {"0", "1"})},
    {"distributed_background_insert_batch", CHSetting(trueOrFalse, {})},
    {"distributed_background_insert_split_batch_on_failure", CHSetting(trueOrFalse, {})},
    {"distributed_cache_bypass_connection_pool", CHSetting(trueOrFalse, {})},
    {"distributed_cache_discard_connection_if_unread_data", CHSetting(trueOrFalse, {})},
    {"distributed_cache_fetch_metrics_only_from_current_az", CHSetting(trueOrFalse, {})},
    {"distributed_cache_throw_on_error", CHSetting(trueOrFalse, {})},
    {"distributed_foreground_insert", CHSetting(trueOrFalse, {})},
    {"distributed_group_by_no_merge", CHSetting(zeroOneTwo, {})},
    {"distributed_insert_skip_read_only_replicas", CHSetting(trueOrFalse, {})},
    {"do_not_merge_across_partitions_select_final", CHSetting(trueOrFalse, {})},
    {"empty_result_for_aggregation_by_constant_keys_on_empty_set", CHSetting(trueOrFalse, {})},
    {"enable_analyzer", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_blob_storage_log", CHSetting(trueOrFalse, {})},
    {"enable_early_constant_folding", CHSetting(trueOrFalse, {})},
    {"enable_extended_results_for_datetime_functions", CHSetting(trueOrFalse, {})},
    {"enable_filesystem_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_filesystem_cache_log", CHSetting(trueOrFalse, {})},
    {"enable_filesystem_cache_on_write_operations", CHSetting(trueOrFalse, {})},
    {"enable_filesystem_read_prefetches_log", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_global_with_statement", CHSetting(trueOrFalse, {})},
    {"enable_http_compression", CHSetting(trueOrFalse, {})},
    {"enable_job_stack_trace", CHSetting(trueOrFalse, {})},
    {"enable_memory_bound_merging_of_aggregation_results", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_multiple_prewhere_read_steps", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_named_columns_in_function_tuple", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_optimize_predicate_expression", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_optimize_predicate_expression_to_final_subquery", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_parallel_replicas", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_parsing_to_custom_serialization", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_reads_from_query_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_s3_requests_logging", CHSetting(trueOrFalse, {})},
    {"enable_scalar_subquery_optimization", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_sharing_sets_for_mutations", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_software_prefetch_in_aggregation", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_unaligned_array_join", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_url_encoding", CHSetting(trueOrFalse, {})},
    {"enable_vertical_final", CHSetting(trueOrFalse, {"0", "1"})},
    {"enable_writes_to_query_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"engine_file_allow_create_multiple_files", CHSetting(trueOrFalse, {})},
    {"engine_file_empty_if_not_exists", CHSetting(trueOrFalse, {})},
    {"engine_file_skip_empty_files", CHSetting(trueOrFalse, {})},
    {"engine_url_skip_empty_files", CHSetting(trueOrFalse, {})},
    {"exact_rows_before_limit", CHSetting(trueOrFalse, {"0", "1"})},
    //{"external_table_functions_use_nulls", CHSetting(trueOrFalse, {})},
    {"external_table_strict_query", CHSetting(trueOrFalse, {})},
    {"extremes", CHSetting(trueOrFalse, {})},
    {"fallback_to_stale_replicas_for_distributed_queries", CHSetting(trueOrFalse, {"0", "1"})},
    {"filesystem_cache_enable_background_download_during_fetch", CHSetting(trueOrFalse, {})},
    {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", CHSetting(trueOrFalse, {})},
    {"filesystem_cache_name",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"cache_for_s3"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"filesystem_cache_prefer_bigger_buffer_size", CHSetting(trueOrFalse, {"0", "1"})},
    {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", CHSetting(trueOrFalse, {})},
    {"filesystem_cache_segments_batch_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             std::vector<uint32_t> choices{0, 3, 10, 50};
             ret += std::to_string(rg.pickRandomlyFromVector(choices));
         },
         {})},
    {"filesystem_prefetch_max_memory_usage",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"32Mi", "64Mi", "128Mi"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"filesystem_prefetch_min_bytes_for_single_read_task",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"1Mi", "8Mi", "16Mi"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"filesystem_prefetch_step_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += "'";
             ret += rg.nextBool() ? "0" : "100Mi";
             ret += "'";
         },
         {})},
    {"filesystem_prefetch_step_marks", CHSetting([](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "0" : "50"; }, {})},
    {"filesystem_prefetches_limit", CHSetting([](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "0" : "10"; }, {})},
    {"final", CHSetting(trueOrFalse, {})},
    {"flatten_nested", CHSetting(trueOrFalse, {"0", "1"})},
    {"force_aggregate_partitions_independently", CHSetting(trueOrFalse, {"0", "1"})},
    {"force_grouping_standard_compatibility", CHSetting(trueOrFalse, {})},
    {"force_index_by_date", CHSetting(trueOrFalse, {})},
    {"force_optimize_projection", CHSetting(trueOrFalse, {"0", "1"})},
    {"force_remove_data_recursively_on_drop", CHSetting(trueOrFalse, {})},
    {"format_capn_proto_use_autogenerated_schema", CHSetting(trueOrFalse, {})},
    {"format_csv_allow_single_quotes", CHSetting(trueOrFalse, {})},
    {"format_display_secrets_in_show_and_select", CHSetting(trueOrFalse, {})},
    {"format_protobuf_use_autogenerated_schema", CHSetting(trueOrFalse, {})},
    {"format_regexp_skip_unmatched", CHSetting(trueOrFalse, {})},
    {"fsync_metadata", CHSetting(trueOrFalse, {"0", "1"})},
    {"function_json_value_return_type_allow_complex", CHSetting(trueOrFalse, {})},
    {"function_json_value_return_type_allow_nullable", CHSetting(trueOrFalse, {})},
    {"function_locate_has_mysql_compatible_argument_order", CHSetting(trueOrFalse, {})},
    {"geo_distance_returns_float64_on_float64_arguments", CHSetting(trueOrFalse, {})},
    {"grace_hash_join_initial_buckets",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1024)); }, {})},
    {"grace_hash_join_max_buckets",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1024)); }, {})},
    /*{"group_by_overflow_mode",
     CHSetting([](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"throw", "break", "any"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }, {})},*/
    {"group_by_two_level_threshold",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 100000)); },
         {"0", "1", "100", "1000"})},
    {"group_by_two_level_threshold_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 500000)); },
         {"0", "1", "100", "1000"})},
    {"group_by_use_nulls", CHSetting(trueOrFalse, {})},
    {"hdfs_create_new_file_on_insert", CHSetting(trueOrFalse, {})},
    {"hdfs_ignore_file_doesnt_exist", CHSetting(trueOrFalse, {})},
    {"hdfs_skip_empty_files", CHSetting(trueOrFalse, {})},
    {"hdfs_throw_on_zero_files_match", CHSetting(trueOrFalse, {})},
    {"http_make_head_request", CHSetting(trueOrFalse, {})},
    {"http_native_compression_disable_checksumming_on_decompress", CHSetting(trueOrFalse, {})},
    {"http_response_buffer_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"http_skip_not_found_url_for_globs", CHSetting(trueOrFalse, {})},
    {"http_wait_end_of_query", CHSetting(trueOrFalse, {})},
    {"http_write_exception_in_output_format", CHSetting(trueOrFalse, {})},
    {"ignore_materialized_views_with_dropped_target_table", CHSetting(trueOrFalse, {})},
    {"ignore_on_cluster_for_replicated_access_entities_queries", CHSetting(trueOrFalse, {})},
    {"ignore_on_cluster_for_replicated_named_collections_queries", CHSetting(trueOrFalse, {})},
    {"ignore_on_cluster_for_replicated_udf_queries", CHSetting(trueOrFalse, {})},
    {"input_format_allow_seeks", CHSetting(trueOrFalse, {})},
    {"input_format_arrow_allow_missing_columns", CHSetting(trueOrFalse, {})},
    {"input_format_arrow_case_insensitive_column_matching", CHSetting(trueOrFalse, {})},
    {"input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_avro_allow_missing_fields", CHSetting(trueOrFalse, {})},
    {"input_format_avro_null_as_default", CHSetting(trueOrFalse, {})},
    {"input_format_binary_read_json_as_string", CHSetting(trueOrFalse, {})},
    {"input_format_bson_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_csv_allow_cr_end_of_line", CHSetting(trueOrFalse, {})},
    {"input_format_csv_allow_variable_number_of_columns", CHSetting(trueOrFalse, {})},
    {"input_format_csv_allow_whitespace_or_tab_as_delimiter", CHSetting(trueOrFalse, {})},
    {"input_format_csv_deserialize_separate_columns_into_tuple", CHSetting(trueOrFalse, {})},
    {"input_format_csv_empty_as_default", CHSetting(trueOrFalse, {})},
    {"input_format_csv_enum_as_number", CHSetting(trueOrFalse, {})},
    {"input_format_csv_skip_trailing_empty_lines", CHSetting(trueOrFalse, {})},
    {"input_format_csv_trim_whitespaces", CHSetting(trueOrFalse, {})},
    {"input_format_csv_try_infer_numbers_from_strings", CHSetting(trueOrFalse, {})},
    {"input_format_csv_try_infer_strings_from_quoted_tuples", CHSetting(trueOrFalse, {})},
    {"input_format_csv_use_best_effort_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_csv_use_default_on_bad_values", CHSetting(trueOrFalse, {})},
    {"input_format_custom_allow_variable_number_of_columns", CHSetting(trueOrFalse, {})},
    {"input_format_custom_skip_trailing_empty_lines", CHSetting(trueOrFalse, {})},
    {"input_format_force_null_for_omitted_fields", CHSetting(trueOrFalse, {})},
    {"input_format_hive_text_allow_variable_number_of_columns", CHSetting(trueOrFalse, {})},
    {"input_format_import_nested_json", CHSetting(trueOrFalse, {})},
    {"input_format_ipv4_default_on_conversion_error", CHSetting(trueOrFalse, {})},
    {"input_format_ipv6_default_on_conversion_error", CHSetting(trueOrFalse, {})},
    {"input_format_json_compact_allow_variable_number_of_columns", CHSetting(trueOrFalse, {})},
    {"input_format_json_defaults_for_missing_elements_in_named_tuple", CHSetting(trueOrFalse, {})},
    {"input_format_json_ignore_unknown_keys_in_named_tuple", CHSetting(trueOrFalse, {})},
    {"input_format_json_ignore_unnecessary_fields", CHSetting(trueOrFalse, {})},
    {"input_format_json_infer_incomplete_types_as_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_named_tuples_as_objects", CHSetting(trueOrFalse, {})},
    {"input_format_json_read_arrays_as_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_read_bools_as_numbers", CHSetting(trueOrFalse, {})},
    {"input_format_json_read_bools_as_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_read_numbers_as_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_read_objects_as_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_throw_on_bad_escape_sequence", CHSetting(trueOrFalse, {})},
    {"input_format_json_try_infer_named_tuples_from_objects", CHSetting(trueOrFalse, {})},
    {"input_format_json_try_infer_numbers_from_strings", CHSetting(trueOrFalse, {})},
    {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", CHSetting(trueOrFalse, {})},
    {"input_format_json_validate_types_from_metadata", CHSetting(trueOrFalse, {})},
    {"input_format_mysql_dump_map_column_names", CHSetting(trueOrFalse, {})},
    {"input_format_native_allow_types_conversion", CHSetting(trueOrFalse, {})},
    //{"input_format_native_decode_types_in_binary_format", CHSetting(trueOrFalse, {})},
    {"input_format_null_as_default", CHSetting(trueOrFalse, {})},
    {"input_format_orc_allow_missing_columns", CHSetting(trueOrFalse, {})},
    {"input_format_orc_case_insensitive_column_matching", CHSetting(trueOrFalse, {})},
    {"input_format_orc_dictionary_as_low_cardinality", CHSetting(trueOrFalse, {})},
    {"input_format_orc_filter_push_down", CHSetting(trueOrFalse, {"0", "1"})},
    {"input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_orc_use_fast_decoder", CHSetting(trueOrFalse, {})},
    {"input_format_parallel_parsing", CHSetting(trueOrFalse, {"0", "1"})},
    {"input_format_parquet_allow_missing_columns", CHSetting(trueOrFalse, {})},
    {"input_format_parquet_bloom_filter_push_down", CHSetting(trueOrFalse, {"0", "1"})},
    {"input_format_parquet_case_insensitive_column_matching", CHSetting(trueOrFalse, {})},
    {"input_format_parquet_enable_row_group_prefetch", CHSetting(trueOrFalse, {})},
    {"input_format_parquet_filter_push_down", CHSetting(trueOrFalse, {"0", "1"})},
    {"input_format_parquet_preserve_order", CHSetting(trueOrFalse, {})},
    {"input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_protobuf_flatten_google_wrappers", CHSetting(trueOrFalse, {})},
    {"input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_skip_unknown_fields", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_dates", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_datetimes", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_datetimes_only_datetime64", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_exponent_floats", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_integers", CHSetting(trueOrFalse, {})},
    {"input_format_try_infer_variants", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_allow_variable_number_of_columns", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_crlf_end_of_line", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_empty_as_default", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_enum_as_number", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_skip_trailing_empty_lines", CHSetting(trueOrFalse, {})},
    {"input_format_tsv_use_best_effort_in_schema_inference", CHSetting(trueOrFalse, {})},
    {"input_format_values_accurate_types_of_literals", CHSetting(trueOrFalse, {})},
    {"input_format_values_deduce_templates_of_expressions", CHSetting(trueOrFalse, {})},
    {"input_format_values_interpret_expressions", CHSetting(trueOrFalse, {})},
    {"insert_deduplicate", CHSetting(trueOrFalse, {})},
    {"insert_distributed_one_random_shard", CHSetting(trueOrFalse, {})},
    {"insert_null_as_default", CHSetting(trueOrFalse, {})},
    {"insert_quorum", CHSetting(zeroOneTwo, {})},
    {"insert_quorum_parallel", CHSetting(trueOrFalse, {})},
    {"interval_output_format",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += "'";
             ret += rg.nextBool() ? "kusto" : "numeric";
             ret += "'";
         },
         {})},
    {"join_algorithm",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices
                 = {"default",
                    "grace_hash",
                    "direct, hash",
                    "hash",
                    "parallel_hash",
                    "partial_merge",
                    "direct",
                    "auto",
                    "full_sorting_merge",
                    "prefer_partial_merge"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {"'default'",
          "'grace_hash'",
          "'direct, hash'",
          "'hash'",
          "'parallel_hash'",
          "'partial_merge'",
          "'direct'",
          "'auto'",
          "'full_sorting_merge'",
          "'prefer_partial_merge'"})},
    {"join_any_take_last_row", CHSetting(trueOrFalse, {"0", "1"})},
    /*{"join_overflow_mode",
     CHSetting([](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "throw" : "break";
         ret += "'";
     }, {})},*/
    {"join_use_nulls", CHSetting(trueOrFalse, {})},
    {"keeper_map_strict_mode", CHSetting(trueOrFalse, {})},
    {"legacy_column_name_of_tuple_literal", CHSetting(trueOrFalse, {})},
    //{"lightweight_deletes_sync", CHSetting(zeroOneTwo, {})}, FINAL queries don't cover these
    {"load_marks_asynchronously", CHSetting(trueOrFalse, {})},
    {"local_filesystem_read_method",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"read", "pread", "mmap", "pread_threadpool", "io_uring"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"})},
    {"local_filesystem_read_prefetch", CHSetting(trueOrFalse, {"0", "1"})},
    {"log_formatted_queries", CHSetting(trueOrFalse, {})},
    {"log_processors_profiles", CHSetting(trueOrFalse, {})},
    {"log_profile_events", CHSetting(trueOrFalse, {})},
    {"log_queries", CHSetting(trueOrFalse, {})},
    {"log_query_settings", CHSetting(trueOrFalse, {})},
    {"log_query_threads", CHSetting(trueOrFalse, {"0", "1"})},
    {"log_query_views", CHSetting(trueOrFalse, {"0", "1"})},
    //{"low_cardinality_allow_in_native_format", CHSetting(trueOrFalse, {})},
    {"low_cardinality_max_dictionary_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"low_cardinality_use_single_dictionary_for_part", CHSetting(trueOrFalse, {"0", "1"})},
    {"materialize_skip_indexes_on_insert", CHSetting(trueOrFalse, {})},
    {"materialize_statistics_on_insert", CHSetting(trueOrFalse, {})},
    {"materialize_ttl_after_modify", CHSetting(trueOrFalse, {})},
    {"materialized_views_ignore_errors", CHSetting(trueOrFalse, {})},
    {"max_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"32", "64", "1024", "1000000"})},
    {"max_bytes_before_external_group_by",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {"0", "1", "1000", "1000000"})},
    {"max_bytes_before_external_sort",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {"0", "1", "1000", "1000000"})},
    {"max_bytes_before_remerge_sort",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"0", "1", "1000", "1000000"})},
    /*{"max_bytes_in_distinct",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_in_join",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_in_set",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_to_read",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_to_read_leaf",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_to_sort",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_bytes_to_transfer",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_columns_to_read",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {})},*/
    {"max_compress_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"0", "32", "64", "1024", "1000000"})},
    {"max_final_threads",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); },
         {"1", std::to_string(std::thread::hardware_concurrency())})},
    {"max_insert_block_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_insert_delayed_streams_for_parallel_write",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 12)); }, {})},
    {"max_insert_threads",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); },
         {})},
    {"max_joined_block_size_rows",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"8", "32", "64", "1024", "1000000"})},
    /*{"max_memory_usage",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }, {})},
    {"max_memory_usage_for_user",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }, {})},*/
    {"max_number_of_partitions_for_independent_aggregation",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 15)); },
         {"0", "1", "100", "1000"})},
    {"max_parallel_replicas",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }, {})},
    {"max_parsing_threads",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "10"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {"1", std::to_string(std::thread::hardware_concurrency())})},
    {"max_parts_to_move",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(4096))); },
         {"0", "1", "100", "1000"})},
    {"max_read_buffer_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"32", "64", "1024", "1000000"})},
    /*{"max_result_bytes",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_result_rows",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_in_distinct",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_in_join",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_in_set",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_to_group_by",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_to_read",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_to_read_leaf",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_to_sort",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_rows_to_transfer",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"max_temporary_columns",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {})},
    {"max_temporary_non_const_columns",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {})},*/
    {"max_threads",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(rg.RandomInt<uint32_t>(1, std::thread::hardware_concurrency())); },
         {"1", std::to_string(std::thread::hardware_concurrency())})},
    {"merge_tree_coarse_index_granularity",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(2, 32)); }, {})},
    {"merge_tree_compact_parts_min_granules_to_multibuffer_read",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 128)); }, {})},
    {"merge_tree_determine_task_size_by_prewhere_columns", CHSetting(trueOrFalse, {"0", "1"})},
    {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(std::ceil(rg.RandomZeroOne() * 100.0) / 100.0); }, {})},
    {"merge_tree_use_const_size_tasks_for_remote_reading", CHSetting(trueOrFalse, {})},
    {"merge_tree_use_v1_object_and_dynamic_serialization", CHSetting(trueOrFalse, {})},
    {"metrics_perf_events_enabled", CHSetting(trueOrFalse, {})},
    {"min_bytes_to_use_direct_io",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {"0", "100", "1000", "100000"})},
    {"min_bytes_to_use_mmap_io",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += std::to_string(
                 rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
         },
         {"0", "100", "1000", "100000"})},
    {"min_chunk_bytes_for_parallel_parsing",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         { ret += std::to_string(std::max(1024, static_cast<int>(rg.RandomGauss(10 * 1024 * 1024, 5 * 1000 * 1000)))); },
         {"0", "100", "1000", "100000"})},
    {"min_compress_block_size",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); },
         {"0", "32", "64", "1024", "1000000"})},
    {"min_count_to_compile_aggregate_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"})},
    {"min_count_to_compile_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"})},
    {"min_count_to_compile_sort_description", CHSetting(zeroToThree, {"0", "1", "2", "3"})},
    {"min_external_table_block_size_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "100000000"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {"0", "100", "1000", "100000"})},
    {"min_hit_rate_to_use_consecutive_keys_optimization",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); },
         {"0", "0.1", "0.5", "0.9"})},
    {"min_insert_block_size_bytes",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"min_insert_block_size_bytes_for_materialized_views",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"min_insert_block_size_rows",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"min_insert_block_size_rows_for_materialized_views",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"mongodb_throw_on_unsupported_query", CHSetting(trueOrFalse, {})},
    {"move_all_conditions_to_prewhere", CHSetting(trueOrFalse, {"0", "1"})},
    {"move_primary_key_columns_to_end_of_prewhere", CHSetting(trueOrFalse, {"0", "1"})},
    {"multiple_joins_try_to_keep_original_names", CHSetting(trueOrFalse, {"0", "1"})},
    {"mutations_execute_nondeterministic_on_initiator", CHSetting(trueOrFalse, {"0", "1"})},
    {"mutations_execute_subqueries_on_initiator", CHSetting(trueOrFalse, {"0", "1"})},
    {"mutations_sync", CHSetting(zeroOneTwo, {})},
    {"mysql_map_fixed_string_to_text_in_show_columns", CHSetting(trueOrFalse, {})},
    {"mysql_map_string_to_text_in_show_columns", CHSetting(trueOrFalse, {})},
    {"optimize_aggregation_in_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_aggregators_of_group_by_keys", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_append_index", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_arithmetic_operations_in_aggregate_functions", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_count_from_files", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_distinct_in_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_distributed_group_by_sharding_key", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_extract_common_expressions", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_functions_to_subcolumns", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_group_by_constant_keys", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_group_by_function_keys", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_if_chain_to_multiif", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_if_transform_strings_to_enum", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_injective_functions_in_group_by", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_injective_functions_inside_uniq", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_move_to_prewhere", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_move_to_prewhere_if_final", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_multiif_to_if", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_normalize_count_variants", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_on_insert", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_or_like_chain", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_read_in_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_read_in_window_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_redundant_functions_in_order_by", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_respect_aliases", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_rewrite_aggregate_function_with_if", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_rewrite_array_exists_to_has", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_rewrite_sum_if_to_count_if", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_skip_merged_partitions", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_skip_unused_shards", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_skip_unused_shards_rewrite_in", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_sorting_by_input_stream_properties", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_substitute_columns", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_syntax_fuse_functions", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_throw_if_noop", CHSetting(trueOrFalse, {})},
    {"optimize_time_filter_with_preimage", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_trivial_approximate_count_query", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_trivial_count_query", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_trivial_insert_select", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_uniq_to_count", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_use_implicit_projections", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_use_projections", CHSetting(trueOrFalse, {"0", "1"})},
    {"optimize_using_constraints", CHSetting(trueOrFalse, {"0", "1"})},
    {"output_format_arrow_compression_method",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"lz4_frame", "zstd", "none"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_arrow_fixed_string_as_fixed_byte_array", CHSetting(trueOrFalse, {})},
    {"output_format_arrow_low_cardinality_as_dictionary", CHSetting(trueOrFalse, {})},
    {"output_format_arrow_string_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_arrow_use_64_bit_indexes_for_dictionary", CHSetting(trueOrFalse, {})},
    {"output_format_arrow_use_signed_indexes_for_dictionary", CHSetting(trueOrFalse, {})},
    //{"output_format_binary_encode_types_in_binary_format", CHSetting(trueOrFalse, {})},
    {"output_format_binary_write_json_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_bson_string_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_csv_crlf_end_of_line", CHSetting(trueOrFalse, {})},
    {"output_format_csv_serialize_tuple_into_separate_columns", CHSetting(trueOrFalse, {})},
    {"output_format_decimal_trailing_zeros", CHSetting(trueOrFalse, {})},
    {"output_format_enable_streaming", CHSetting(trueOrFalse, {})},
    {"output_format_avro_codec",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"null", "deflate", "snappy", "zstd"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_json_array_of_rows", CHSetting(trueOrFalse, {})},
    {"output_format_json_escape_forward_slashes", CHSetting(trueOrFalse, {})},
    {"output_format_json_named_tuples_as_objects", CHSetting(trueOrFalse, {})},
    {"output_format_json_quote_64bit_floats", CHSetting(trueOrFalse, {})},
    {"output_format_json_quote_64bit_integers", CHSetting(trueOrFalse, {})},
    {"output_format_json_quote_decimals", CHSetting(trueOrFalse, {})},
    {"output_format_json_quote_denormals", CHSetting(trueOrFalse, {})},
    {"output_format_json_skip_null_value_in_named_tuples", CHSetting(trueOrFalse, {})},
    {"output_format_json_validate_utf8", CHSetting(trueOrFalse, {})},
    {"output_format_markdown_escape_special_characters", CHSetting(trueOrFalse, {})},
    //{"output_format_native_encode_types_in_binary_format", CHSetting(trueOrFalse, {})},
    {"output_format_native_write_json_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_orc_compression_method",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"lz4", "snappy", "zlib", "zstd", "none"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_orc_string_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_parallel_formatting", CHSetting(trueOrFalse, {"0", "1"})},
    {"output_format_parquet_compliant_nested_types", CHSetting(trueOrFalse, {})},
    {"output_format_parquet_compression_method",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"snappy", "lz4", "brotli", "zstd", "gzip", "none"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_parquet_fixed_string_as_fixed_byte_array", CHSetting(trueOrFalse, {})},
    {"output_format_parquet_parallel_encoding", CHSetting(trueOrFalse, {})},
    {"output_format_parquet_string_as_string", CHSetting(trueOrFalse, {})},
    {"output_format_parquet_use_custom_encoder", CHSetting(trueOrFalse, {})},
    {"output_format_parquet_version",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"1.0", "2.4", "2.6", "2.latest"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_parquet_write_page_index", CHSetting(trueOrFalse, {})},
    {"output_format_pretty_color",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "auto"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"output_format_pretty_grid_charset",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += "'";
             ret += rg.nextBool() ? "UTF-8" : "ASCII";
             ret += "'";
         },
         {})},
    {"output_format_pretty_highlight_digit_groups", CHSetting(trueOrFalse, {})},
    {"output_format_pretty_row_numbers", CHSetting(trueOrFalse, {})},
    {"output_format_protobuf_nullables_with_google_wrappers", CHSetting(trueOrFalse, {})},
    {"output_format_sql_insert_include_column_names", CHSetting(trueOrFalse, {})},
    {"output_format_sql_insert_quote_names", CHSetting(trueOrFalse, {})},
    {"output_format_sql_insert_use_replace", CHSetting(trueOrFalse, {})},
    {"output_format_tsv_crlf_end_of_line", CHSetting(trueOrFalse, {})},
    {"output_format_values_escape_quote_with_quote", CHSetting(trueOrFalse, {})},
    {"output_format_write_statistics", CHSetting(trueOrFalse, {})},
    {"page_cache_inject_eviction", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallel_replica_offset",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
    {"parallel_replicas_allow_in_with_subquery", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallel_replicas_count",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
    {"parallel_replicas_for_non_replicated_merge_tree", CHSetting(trueOrFalse, {})},
    {"parallel_replicas_index_analysis_only_on_coordinator", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallel_replicas_custom_key_range_lower",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"parallel_replicas_custom_key_range_upper",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"parallel_replicas_local_plan", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallel_replicas_mark_segment_size",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"parallel_replicas_min_number_of_rows_per_replica",
     CHSetting([](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }, {})},
    {"parallel_replicas_mode",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"sampling_key", "read_tasks", "custom_key_range", "custom_key_sampling", "auto"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"})},
    {"parallel_replicas_prefer_local_join", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallel_view_processing", CHSetting(trueOrFalse, {"0", "1"})},
    {"parallelize_output_from_storages", CHSetting(trueOrFalse, {"0", "1"})},
    {"partial_merge_join_optimizations", CHSetting(trueOrFalse, {"0", "1"})},
    {"partial_result_on_first_cancel", CHSetting(trueOrFalse, {})},
    {"precise_float_parsing", CHSetting(trueOrFalse, {})},
    {"prefer_external_sort_block_bytes",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"0", "1", "1000", "1000000"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {"0", "1", "1000", "1000000"})},
    {"prefer_global_in_and_join", CHSetting(trueOrFalse, {"0", "1"})},
    {"prefer_localhost_replica", CHSetting(trueOrFalse, {"0", "1"})},
    {"print_pretty_type_names", CHSetting(trueOrFalse, {})},
    {"push_external_roles_in_interserver_queries", CHSetting(trueOrFalse, {})},
    {"query_cache_compress_entries", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_cache_share_between_users", CHSetting(trueOrFalse, {})},
    {"query_cache_squash_partial_results", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_aggregation_in_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_convert_outer_join_to_inner_join", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_enable_multithreading_after_window_functions", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_enable_optimizations", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_execute_functions_after_sorting", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_filter_push_down", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_join_swap_table",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"false", "true", "auto"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {"'false'", "'true'", "'auto'"})},
    {"query_plan_lift_up_array_join", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_lift_up_union", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_merge_expressions", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_merge_filters", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_optimize_prewhere", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_push_down_limit", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_read_in_order", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_remove_redundant_distinct", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_remove_redundant_sorting", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_reuse_storage_ordering_for_window_functions", CHSetting(trueOrFalse, {"0", "1"})},
    {"query_plan_split_filter", CHSetting(trueOrFalse, {"0", "1"})},
    {"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"read_from_page_cache_if_exists_otherwise_bypass_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"read_in_order_two_level_merge_threshold",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 100)); }, {"0", "1", "10", "100"})},
    {"read_in_order_use_buffering", CHSetting(trueOrFalse, {"0", "1"})},
    {"read_in_order_use_virtual_row", CHSetting(trueOrFalse, {"0", "1"})},
    {"read_through_distributed_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"regexp_dict_allow_hyperscan", CHSetting(trueOrFalse, {"0", "1"})},
    {"regexp_dict_flag_case_insensitive", CHSetting(trueOrFalse, {})},
    {"regexp_dict_flag_dotall", CHSetting(trueOrFalse, {})},
    {"remote_filesystem_read_method",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             ret += "'";
             ret += rg.nextBool() ? "read" : "threadpool";
             ret += "'";
         },
         {"'read'", "'threadpool'"})},
    {"reject_expensive_hyperscan_regexps", CHSetting(trueOrFalse, {"0", "1"})},
    {"remote_filesystem_read_prefetch", CHSetting(trueOrFalse, {"0", "1"})},
    {"replace_running_query", CHSetting(trueOrFalse, {})},
    {"rewrite_count_distinct_if_with_count_distinct_implementation", CHSetting(trueOrFalse, {"0", "1"})},
    {"rows_before_aggregation", CHSetting(trueOrFalse, {"0", "1"})},
    {"s3_allow_parallel_part_upload", CHSetting(trueOrFalse, {})},
    {"s3_check_objects_after_upload", CHSetting(trueOrFalse, {})},
    {"s3_create_new_file_on_insert", CHSetting(trueOrFalse, {})},
    {"s3_disable_checksum", CHSetting(trueOrFalse, {})},
    {"s3_ignore_file_doesnt_exist", CHSetting(trueOrFalse, {})},
    {"s3_skip_empty_files", CHSetting(trueOrFalse, {})},
    {"s3_throw_on_zero_files_match", CHSetting(trueOrFalse, {})},
    {"s3_truncate_on_insert", CHSetting(trueOrFalse, {})},
    {"s3_use_adaptive_timeouts", CHSetting(trueOrFalse, {})},
    {"s3_validate_request_settings", CHSetting(trueOrFalse, {})},
    {"s3queue_enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {})},
    {"schema_inference_cache_require_modification_time_for_url", CHSetting(trueOrFalse, {})},
    {"schema_inference_use_cache_for_file", CHSetting(trueOrFalse, {})},
    {"schema_inference_use_cache_for_s3", CHSetting(trueOrFalse, {})},
    {"schema_inference_use_cache_for_url", CHSetting(trueOrFalse, {})},
    {"select_sequential_consistency", CHSetting(trueOrFalse, {"0", "1"})},
    {"send_logs_level",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices = {"debug", "information", "trace", "error", "test", "warning", "fatal", "none"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"send_progress_in_http_headers", CHSetting(trueOrFalse, {})},
    {"show_table_uuid_in_table_create_query_if_not_nil", CHSetting(trueOrFalse, {})},
    {"single_join_prefer_left_table", CHSetting(trueOrFalse, {"0", "1"})},
    {"skip_unavailable_shards", CHSetting(trueOrFalse, {})},
    /*{"set_overflow_mode",
     CHSetting([](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "break" : "throw";
         ret += "'";
     }, {})},*/
    {"split_intersecting_parts_ranges_into_layers_final", CHSetting(trueOrFalse, {"0", "1"})},
    {"split_parts_ranges_into_intersecting_and_non_intersecting_final", CHSetting(trueOrFalse, {"0", "1"})},
    {"splitby_max_substrings_includes_remaining_string", CHSetting(trueOrFalse, {})},
    {"stream_like_engine_allow_direct_select", CHSetting(trueOrFalse, {"0", "1"})},
    {"system_events_show_zero_values", CHSetting(trueOrFalse, {})},
    {"throw_on_error_from_cache_on_write_operations", CHSetting(trueOrFalse, {})},
    {"totals_mode",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices
                 = {"before_having", "after_having_exclusive", "after_having_inclusive", "after_having_auto"};
             ret += "'";
             ret += rg.pickRandomlyFromVector(choices);
             ret += "'";
         },
         {})},
    {"trace_profile_events", CHSetting(trueOrFalse, {})},
    {"transform_null_in", CHSetting(trueOrFalse, {"0", "1"})},
    {"traverse_shadow_remote_data_paths", CHSetting(trueOrFalse, {})},
    {"type_json_skip_duplicated_paths", CHSetting(trueOrFalse, {})},
    {"update_insert_deduplication_token_in_dependent_materialized_views", CHSetting(trueOrFalse, {})},
    {"use_async_executor_for_materialized_views", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_cache_for_count_from_files", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_client_time_zone", CHSetting(trueOrFalse, {})},
    {"use_compact_format_in_distributed_parts_names", CHSetting(trueOrFalse, {})},
    {"use_concurrency_control", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_hedged_requests", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_hive_partitioning", CHSetting(trueOrFalse, {})},
    {"use_index_for_in_with_subqueries", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_local_cache_for_remote_storage", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_page_cache_for_disks_without_file_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_query_cache",
     CHSetting(
         [](RandomGenerator & rg, std::string & ret)
         {
             const std::vector<std::string> & choices
                 = {"1, set_overflow_mode = 'throw', group_by_overflow_mode = 'throw', join_overflow_mode = 'throw'",
                    "0, set_overflow_mode = 'break', group_by_overflow_mode = 'break', join_overflow_mode = 'break'"};
             ret += rg.pickRandomlyFromVector(choices);
         },
         {})},
    {"use_skip_indexes", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_skip_indexes_if_final", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_structure_from_insertion_table_in_table_functions", CHSetting(zeroOneTwo, {})},
    {"use_uncompressed_cache", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_variant_as_common_type", CHSetting(trueOrFalse, {"0", "1"})},
    {"use_with_fill_by_sorting_prefix", CHSetting(trueOrFalse, {"0", "1"})},
    {"validate_experimental_and_suspicious_types_inside_nested_types", CHSetting(trueOrFalse, {})},
    {"validate_mutation_query", CHSetting(trueOrFalse, {})},
    {"validate_polygons", CHSetting(trueOrFalse, {})},
    //{"wait_for_async_insert", CHSetting(trueOrFalse, {})},
    {"write_through_distributed_cache", CHSetting(trueOrFalse, {})}};

std::map<std::string, CHSetting> queryOracleSettings;

void loadFuzzerServerSettings(const FuzzConfig & fc)
{
    if (!fc.timezones.empty())
    {
        settings_timezones.insert(settings_timezones.end(), fc.timezones.begin(), fc.timezones.end());
        serverSettings.insert(
            {{"session_timezone",
              CHSetting(
                  [&](RandomGenerator & rg, std::string & ret)
                  {
                      ret += "'";
                      ret += rg.pickRandomlyFromVector(settings_timezones);
                      ret += "'";
                  },
                  {})}});
    }
    for (auto & [key, value] : serverSettings)
    {
        if (!value.oracle_values.empty())
        {
            queryOracleSettings.insert({{key, value}});
        }
    }
}

}
