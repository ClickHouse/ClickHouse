#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

void setRandomSetting(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    std::string & ret,
    SetValue * set)
{
    std::string first;
    std::function<void(RandomGenerator &, std::string &)> second;
    std::tie(first, second) = rg.pickPairRandomlyFromMap(settings);

    set->set_property(first);
    ret.resize(0);
    second(rg, ret);
    set->set_value(ret);
}

static std::vector<std::string> merge_storage_policies, settings_timezones;

std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> serverSettings = {
    {"aggregate_functions_null_for_empty", trueOrFalse},
    {"aggregation_in_order_max_block_bytes",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"allow_aggregate_partitions_independently", trueOrFalse},
    {"allow_asynchronous_read_from_io_pool_for_merge_tree", trueOrFalse},
    {"allow_changing_replica_until_first_data_packet", trueOrFalse},
    {"allow_create_index_without_type", trueOrFalse},
    {"allow_experimental_parallel_reading_from_replicas", zeroOneTwo},
    {"allow_experimental_shared_set_join", trueOrFalse},
    {"allow_introspection_functions", trueOrFalse},
    {"allow_prefetched_read_pool_for_remote_filesystem", trueOrFalse},
    {"allow_reorder_prewhere_conditions", trueOrFalse},
    {"alter_move_to_space_execute_async", trueOrFalse},
    {"alter_partition_verbose_result", trueOrFalse},
    {"alter_sync", zeroOneTwo},
    {"analyze_index_with_space_filling_curves", trueOrFalse},
    {"analyzer_compatibility_join_using_top_level_identifier", trueOrFalse},
    {"apply_deleted_mask", trueOrFalse},
    {"apply_mutations_on_fly", trueOrFalse},
    {"any_join_distinct_right_table_keys", trueOrFalse},
    {"asterisk_include_alias_columns", trueOrFalse},
    {"async_insert", trueOrFalse},
    {"async_insert_deduplicate", trueOrFalse},
    {"async_insert_threads",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); }},
    {"async_insert_use_adaptive_busy_timeout", trueOrFalse},
    {"async_query_sending_for_remote", trueOrFalse},
    {"async_socket_for_remote", trueOrFalse},
    {"calculate_text_stack_trace", trueOrFalse},
    {"cancel_http_readonly_queries_on_client_close", trueOrFalse},
    {"cast_ipv4_ipv6_default_on_conversion_error", trueOrFalse},
    {"cast_keep_nullable", trueOrFalse},
    {"cast_string_to_dynamic_use_inference", trueOrFalse},
    {"check_query_single_value_result", trueOrFalse},
    {"check_referential_table_dependencies", trueOrFalse},
    {"check_table_dependencies", trueOrFalse},
    {"checksum_on_read", trueOrFalse},
    {"cloud_mode", trueOrFalse},
    {"collect_hash_table_stats_during_aggregation", trueOrFalse},
    {"collect_hash_table_stats_during_joins", trueOrFalse},
    {"compatibility_ignore_auto_increment_in_create_table", trueOrFalse},
    {"compatibility_ignore_collation_in_create_table", trueOrFalse},
    {"compile_aggregate_expressions", trueOrFalse},
    {"compile_expressions", trueOrFalse},
    {"compile_sort_description", trueOrFalse},
    {"composed_data_type_output_format_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "default" : "spark";
         ret += "'";
     }},
    {"convert_query_to_cnf", trueOrFalse},
    {"count_distinct_optimization", trueOrFalse},
    {"create_table_empty_primary_key_by_default", trueOrFalse},
    {"cross_join_min_bytes_to_compress",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "100000000"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"cross_join_min_rows_to_compress",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "100000000"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"database_atomic_wait_for_drop_and_detach_synchronously", trueOrFalse},
    {"database_replicated_allow_heavy_create", trueOrFalse},
    {"database_replicated_enforce_synchronous_settings", trueOrFalse},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", trueOrFalse},
    {"date_time_output_format",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices
             = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"decimal_check_overflow", trueOrFalse},
    //{"deduplicate_blocks_in_dependent_materialized_views", trueOrFalse},
    //{"describe_compact_output", trueOrFalse},
    {"describe_extend_object_types", trueOrFalse},
    {"describe_include_subcolumns", trueOrFalse},
    {"dictionary_use_async_executor", trueOrFalse},
    {"distributed_aggregation_memory_efficient", trueOrFalse},
    {"distributed_background_insert_batch", trueOrFalse},
    {"distributed_background_insert_split_batch_on_failure", trueOrFalse},
    {"distributed_cache_bypass_connection_pool", trueOrFalse},
    {"distributed_cache_discard_connection_if_unread_data", trueOrFalse},
    {"distributed_cache_fetch_metrics_only_from_current_az", trueOrFalse},
    {"distributed_cache_throw_on_error", trueOrFalse},
    {"distributed_foreground_insert", trueOrFalse},
    {"distributed_group_by_no_merge", zeroOneTwo},
    {"distributed_insert_skip_read_only_replicas", trueOrFalse},
    {"do_not_merge_across_partitions_select_final", trueOrFalse},
    {"empty_result_for_aggregation_by_constant_keys_on_empty_set", trueOrFalse},
    {"enable_blob_storage_log", trueOrFalse},
    {"enable_early_constant_folding", trueOrFalse},
    {"enable_extended_results_for_datetime_functions", trueOrFalse},
    {"enable_filesystem_cache", trueOrFalse},
    {"enable_filesystem_cache_log", trueOrFalse},
    {"enable_filesystem_cache_on_write_operations", trueOrFalse},
    {"enable_filesystem_read_prefetches_log", trueOrFalse},
    {"enable_global_with_statement", trueOrFalse},
    {"enable_http_compression", trueOrFalse},
    {"enable_job_stack_trace", trueOrFalse},
    {"enable_memory_bound_merging_of_aggregation_results", trueOrFalse},
    {"enable_multiple_prewhere_read_steps", trueOrFalse},
    {"enable_named_columns_in_function_tuple", trueOrFalse},
    {"enable_optimize_predicate_expression", trueOrFalse},
    {"enable_optimize_predicate_expression_to_final_subquery", trueOrFalse},
    {"enable_parallel_replicas", trueOrFalse},
    {"enable_parsing_to_custom_serialization", trueOrFalse},
    {"enable_reads_from_query_cache", trueOrFalse},
    {"enable_s3_requests_logging", trueOrFalse},
    {"enable_scalar_subquery_optimization", trueOrFalse},
    {"enable_sharing_sets_for_mutations", trueOrFalse},
    {"enable_software_prefetch_in_aggregation", trueOrFalse},
    {"enable_unaligned_array_join", trueOrFalse},
    {"enable_url_encoding", trueOrFalse},
    {"enable_vertical_final", trueOrFalse},
    {"enable_writes_to_query_cache", trueOrFalse},
    {"engine_file_allow_create_multiple_files", trueOrFalse},
    {"engine_file_empty_if_not_exists", trueOrFalse},
    {"engine_file_skip_empty_files", trueOrFalse},
    {"engine_url_skip_empty_files", trueOrFalse},
    {"exact_rows_before_limit", trueOrFalse},
    //{"external_table_functions_use_nulls", trueOrFalse},
    {"external_table_strict_query", trueOrFalse},
    {"extremes", trueOrFalse},
    {"fallback_to_stale_replicas_for_distributed_queries", trueOrFalse},
    {"filesystem_cache_enable_background_download_during_fetch", trueOrFalse},
    {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", trueOrFalse},
    {"filesystem_cache_name",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"cache_for_s3"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"filesystem_cache_prefer_bigger_buffer_size", trueOrFalse},
    {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", trueOrFalse},
    {"filesystem_cache_segments_batch_size",
     [](RandomGenerator & rg, std::string & ret)
     {
         std::vector<uint32_t> choices{0, 3, 10, 50};
         ret += std::to_string(rg.pickRandomlyFromVector(choices));
     }},
    {"filesystem_prefetch_max_memory_usage",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"32Mi", "64Mi", "128Mi"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"filesystem_prefetch_min_bytes_for_single_read_task",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"1Mi", "8Mi", "16Mi"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"filesystem_prefetch_step_bytes",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "0" : "100Mi";
         ret += "'";
     }},
    {"filesystem_prefetch_step_marks", [](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "0" : "50"; }},
    {"filesystem_prefetches_limit", [](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "0" : "10"; }},
    {"final", trueOrFalse},
    {"flatten_nested", trueOrFalse},
    {"force_aggregate_partitions_independently", trueOrFalse},
    {"force_grouping_standard_compatibility", trueOrFalse},
    {"force_index_by_date", trueOrFalse},
    {"force_optimize_projection", trueOrFalse},
    {"force_remove_data_recursively_on_drop", trueOrFalse},
    {"format_capn_proto_use_autogenerated_schema", trueOrFalse},
    {"format_csv_allow_single_quotes", trueOrFalse},
    {"format_display_secrets_in_show_and_select", trueOrFalse},
    {"format_protobuf_use_autogenerated_schema", trueOrFalse},
    {"format_regexp_skip_unmatched", trueOrFalse},
    {"fsync_metadata", trueOrFalse},
    {"function_json_value_return_type_allow_complex", trueOrFalse},
    {"function_json_value_return_type_allow_nullable", trueOrFalse},
    {"function_locate_has_mysql_compatible_argument_order", trueOrFalse},
    {"geo_distance_returns_float64_on_float64_arguments", trueOrFalse},
    {"grace_hash_join_initial_buckets",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1024)); }},
    {"grace_hash_join_max_buckets",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1024)); }},
    /*{"group_by_overflow_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"throw", "break", "any"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},*/
    {"group_by_two_level_threshold",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 100000)); }},
    {"group_by_two_level_threshold_bytes",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 500000)); }},
    {"group_by_use_nulls", trueOrFalse},
    {"hdfs_create_new_file_on_insert", trueOrFalse},
    {"hdfs_ignore_file_doesnt_exist", trueOrFalse},
    {"hdfs_skip_empty_files", trueOrFalse},
    {"hdfs_throw_on_zero_files_match", trueOrFalse},
    {"http_make_head_request", trueOrFalse},
    {"http_native_compression_disable_checksumming_on_decompress", trueOrFalse},
    {"http_response_buffer_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"http_skip_not_found_url_for_globs", trueOrFalse},
    {"http_wait_end_of_query", trueOrFalse},
    {"http_write_exception_in_output_format", trueOrFalse},
    {"ignore_materialized_views_with_dropped_target_table", trueOrFalse},
    {"ignore_on_cluster_for_replicated_access_entities_queries", trueOrFalse},
    {"ignore_on_cluster_for_replicated_named_collections_queries", trueOrFalse},
    {"ignore_on_cluster_for_replicated_udf_queries", trueOrFalse},
    {"input_format_allow_seeks", trueOrFalse},
    {"input_format_arrow_allow_missing_columns", trueOrFalse},
    {"input_format_arrow_case_insensitive_column_matching", trueOrFalse},
    {"input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_avro_allow_missing_fields", trueOrFalse},
    {"input_format_avro_null_as_default", trueOrFalse},
    {"input_format_binary_read_json_as_string", trueOrFalse},
    {"input_format_bson_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_csv_allow_cr_end_of_line", trueOrFalse},
    {"input_format_csv_allow_variable_number_of_columns", trueOrFalse},
    {"input_format_csv_allow_whitespace_or_tab_as_delimiter", trueOrFalse},
    {"input_format_csv_deserialize_separate_columns_into_tuple", trueOrFalse},
    {"input_format_csv_empty_as_default", trueOrFalse},
    {"input_format_csv_enum_as_number", trueOrFalse},
    {"input_format_csv_skip_trailing_empty_lines", trueOrFalse},
    {"input_format_csv_trim_whitespaces", trueOrFalse},
    {"input_format_csv_try_infer_numbers_from_strings", trueOrFalse},
    {"input_format_csv_try_infer_strings_from_quoted_tuples", trueOrFalse},
    {"input_format_csv_use_best_effort_in_schema_inference", trueOrFalse},
    {"input_format_csv_use_default_on_bad_values", trueOrFalse},
    {"input_format_custom_allow_variable_number_of_columns", trueOrFalse},
    {"input_format_custom_skip_trailing_empty_lines", trueOrFalse},
    {"input_format_force_null_for_omitted_fields", trueOrFalse},
    {"input_format_hive_text_allow_variable_number_of_columns", trueOrFalse},
    {"input_format_import_nested_json", trueOrFalse},
    {"input_format_ipv4_default_on_conversion_error", trueOrFalse},
    {"input_format_ipv6_default_on_conversion_error", trueOrFalse},
    {"input_format_json_compact_allow_variable_number_of_columns", trueOrFalse},
    {"input_format_json_defaults_for_missing_elements_in_named_tuple", trueOrFalse},
    {"input_format_json_ignore_unknown_keys_in_named_tuple", trueOrFalse},
    {"input_format_json_ignore_unnecessary_fields", trueOrFalse},
    {"input_format_json_infer_incomplete_types_as_strings", trueOrFalse},
    {"input_format_json_named_tuples_as_objects", trueOrFalse},
    {"input_format_json_read_arrays_as_strings", trueOrFalse},
    {"input_format_json_read_bools_as_numbers", trueOrFalse},
    {"input_format_json_read_bools_as_strings", trueOrFalse},
    {"input_format_json_read_numbers_as_strings", trueOrFalse},
    {"input_format_json_read_objects_as_strings", trueOrFalse},
    {"input_format_json_throw_on_bad_escape_sequence", trueOrFalse},
    {"input_format_json_try_infer_named_tuples_from_objects", trueOrFalse},
    {"input_format_json_try_infer_numbers_from_strings", trueOrFalse},
    {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", trueOrFalse},
    {"input_format_json_validate_types_from_metadata", trueOrFalse},
    {"input_format_mysql_dump_map_column_names", trueOrFalse},
    {"input_format_native_allow_types_conversion", trueOrFalse},
    //{"input_format_native_decode_types_in_binary_format", trueOrFalse},
    {"input_format_null_as_default", trueOrFalse},
    {"input_format_orc_allow_missing_columns", trueOrFalse},
    {"input_format_orc_case_insensitive_column_matching", trueOrFalse},
    {"input_format_orc_dictionary_as_low_cardinality", trueOrFalse},
    {"input_format_orc_filter_push_down", trueOrFalse},
    {"input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_orc_use_fast_decoder", trueOrFalse},
    {"input_format_parallel_parsing", trueOrFalse},
    {"input_format_parquet_allow_missing_columns", trueOrFalse},
    {"input_format_parquet_bloom_filter_push_down", trueOrFalse},
    {"input_format_parquet_case_insensitive_column_matching", trueOrFalse},
    {"input_format_parquet_enable_row_group_prefetch", trueOrFalse},
    {"input_format_parquet_filter_push_down", trueOrFalse},
    {"input_format_parquet_preserve_order", trueOrFalse},
    {"input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_protobuf_flatten_google_wrappers", trueOrFalse},
    {"input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_skip_unknown_fields", trueOrFalse},
    {"input_format_try_infer_dates", trueOrFalse},
    {"input_format_try_infer_datetimes", trueOrFalse},
    {"input_format_try_infer_datetimes_only_datetime64", trueOrFalse},
    {"input_format_try_infer_exponent_floats", trueOrFalse},
    {"input_format_try_infer_integers", trueOrFalse},
    {"input_format_try_infer_variants", trueOrFalse},
    {"input_format_tsv_allow_variable_number_of_columns", trueOrFalse},
    {"input_format_tsv_crlf_end_of_line", trueOrFalse},
    {"input_format_tsv_empty_as_default", trueOrFalse},
    {"input_format_tsv_enum_as_number", trueOrFalse},
    {"input_format_tsv_skip_trailing_empty_lines", trueOrFalse},
    {"input_format_tsv_use_best_effort_in_schema_inference", trueOrFalse},
    {"input_format_values_accurate_types_of_literals", trueOrFalse},
    {"input_format_values_deduce_templates_of_expressions", trueOrFalse},
    {"input_format_values_interpret_expressions", trueOrFalse},
    {"insert_deduplicate", trueOrFalse},
    {"insert_distributed_one_random_shard", trueOrFalse},
    {"insert_null_as_default", trueOrFalse},
    {"insert_quorum", zeroOneTwo},
    {"insert_quorum_parallel", trueOrFalse},
    {"interval_output_format",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "kusto" : "numeric";
         ret += "'";
     }},
    {"join_algorithm",
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
     }},
    {"join_any_take_last_row", trueOrFalse},
    /*{"join_overflow_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "throw" : "break";
         ret += "'";
     }},*/
    {"join_use_nulls", trueOrFalse},
    {"keeper_map_strict_mode", trueOrFalse},
    {"legacy_column_name_of_tuple_literal", trueOrFalse},
    //{"lightweight_deletes_sync", zeroOneTwo}, FINAL queries don't cover these
    {"load_marks_asynchronously", trueOrFalse},
    {"local_filesystem_read_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"read", "pread", "mmap", "pread_threadpool", "io_uring"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"local_filesystem_read_prefetch", trueOrFalse},
    {"log_formatted_queries", trueOrFalse},
    {"log_processors_profiles", trueOrFalse},
    {"log_profile_events", trueOrFalse},
    {"log_queries", trueOrFalse},
    {"log_query_settings", trueOrFalse},
    {"log_query_threads", trueOrFalse},
    {"log_query_views", trueOrFalse},
    //{"low_cardinality_allow_in_native_format", trueOrFalse},
    {"low_cardinality_max_dictionary_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"low_cardinality_use_single_dictionary_for_part", trueOrFalse},
    {"materialize_skip_indexes_on_insert", trueOrFalse},
    {"materialize_statistics_on_insert", trueOrFalse},
    {"materialize_ttl_after_modify", trueOrFalse},
    {"materialized_views_ignore_errors", trueOrFalse},
    {"max_block_size", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_before_external_group_by",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += std::to_string(
             rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
     }},
    {"max_bytes_before_external_sort",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += std::to_string(
             rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
     }},
    {"max_bytes_before_remerge_sort",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    /*{"max_bytes_in_distinct",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_in_join",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_in_set",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_to_read",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_to_read_leaf",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_to_sort",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_bytes_to_transfer",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_columns_to_read",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }},*/
    {"max_compress_block_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_final_threads",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); }},
    {"max_insert_block_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_insert_delayed_streams_for_parallel_write",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 12)); }},
    {"max_insert_threads",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); }},
    {"max_joined_block_size_rows",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    /*{"max_memory_usage",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }},
    {"max_memory_usage_for_user",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }},*/
    {"max_number_of_partitions_for_independent_aggregation",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 15)); }},
    {"max_parallel_replicas", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }},
    {"max_parsing_threads",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "10"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"max_parts_to_move",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(4096))); }},
    {"max_read_buffer_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    /*{"max_result_bytes",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_result_rows",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_in_distinct",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_in_join",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_in_set",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_to_group_by",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_to_read",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_to_read_leaf",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_to_sort",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_rows_to_transfer",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"max_temporary_columns",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }},
    {"max_temporary_non_const_columns",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }},*/
    {"max_threads",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(rg.RandomInt<uint32_t>(1, std::thread::hardware_concurrency())); }},
    {"merge_tree_coarse_index_granularity",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(2, 32)); }},
    {"merge_tree_compact_parts_min_granules_to_multibuffer_read",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 128)); }},
    {"merge_tree_determine_task_size_by_prewhere_columns", trueOrFalse},
    {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(std::ceil(rg.RandomZeroOne() * 100.0) / 100.0); }},
    {"merge_tree_use_const_size_tasks_for_remote_reading", trueOrFalse},
    {"merge_tree_use_v1_object_and_dynamic_serialization", trueOrFalse},
    {"metrics_perf_events_enabled", trueOrFalse},
    {"min_bytes_to_use_direct_io",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += std::to_string(
             rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
     }},
    {"min_bytes_to_use_mmap_io",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += std::to_string(
             rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
     }},
    {"min_chunk_bytes_for_parallel_parsing",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(std::max(1024, static_cast<int>(rg.RandomGauss(10 * 1024 * 1024, 5 * 1000 * 1000)))); }},
    {"min_compress_block_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"min_count_to_compile_aggregate_expression", zeroToThree},
    {"min_count_to_compile_expression", zeroToThree},
    {"min_count_to_compile_sort_description", zeroToThree},
    {"min_external_table_block_size_bytes",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "100000000"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"min_hit_rate_to_use_consecutive_keys_optimization",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }},
    {"min_insert_block_size_bytes",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"min_insert_block_size_bytes_for_materialized_views",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"min_insert_block_size_rows",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"min_insert_block_size_rows_for_materialized_views",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"mongodb_throw_on_unsupported_query", trueOrFalse},
    {"move_all_conditions_to_prewhere", trueOrFalse},
    {"move_primary_key_columns_to_end_of_prewhere", trueOrFalse},
    {"multiple_joins_try_to_keep_original_names", trueOrFalse},
    {"mutations_execute_nondeterministic_on_initiator", trueOrFalse},
    {"mutations_execute_subqueries_on_initiator", trueOrFalse},
    {"mutations_sync", zeroOneTwo},
    {"mysql_map_fixed_string_to_text_in_show_columns", trueOrFalse},
    {"mysql_map_string_to_text_in_show_columns", trueOrFalse},
    {"optimize_aggregation_in_order", trueOrFalse},
    {"optimize_aggregators_of_group_by_keys", trueOrFalse},
    {"optimize_append_index", trueOrFalse},
    {"optimize_arithmetic_operations_in_aggregate_functions", trueOrFalse},
    {"optimize_count_from_files", trueOrFalse},
    {"optimize_distinct_in_order", trueOrFalse},
    {"optimize_distributed_group_by_sharding_key", trueOrFalse},
    {"optimize_extract_common_expressions", trueOrFalse},
    {"optimize_functions_to_subcolumns", trueOrFalse},
    {"optimize_group_by_constant_keys", trueOrFalse},
    {"optimize_group_by_function_keys", trueOrFalse},
    {"optimize_if_chain_to_multiif", trueOrFalse},
    {"optimize_if_transform_strings_to_enum", trueOrFalse},
    {"optimize_injective_functions_in_group_by", trueOrFalse},
    {"optimize_injective_functions_inside_uniq", trueOrFalse},
    {"optimize_move_to_prewhere", trueOrFalse},
    {"optimize_move_to_prewhere_if_final", trueOrFalse},
    {"optimize_multiif_to_if", trueOrFalse},
    {"optimize_normalize_count_variants", trueOrFalse},
    {"optimize_on_insert", trueOrFalse},
    {"optimize_or_like_chain", trueOrFalse},
    {"optimize_read_in_order", trueOrFalse},
    {"optimize_read_in_window_order", trueOrFalse},
    {"optimize_redundant_functions_in_order_by", trueOrFalse},
    {"optimize_respect_aliases", trueOrFalse},
    {"optimize_rewrite_aggregate_function_with_if", trueOrFalse},
    {"optimize_rewrite_array_exists_to_has", trueOrFalse},
    {"optimize_rewrite_sum_if_to_count_if", trueOrFalse},
    {"optimize_skip_merged_partitions", trueOrFalse},
    {"optimize_skip_unused_shards", trueOrFalse},
    {"optimize_skip_unused_shards_rewrite_in", trueOrFalse},
    {"optimize_sorting_by_input_stream_properties", trueOrFalse},
    {"optimize_substitute_columns", trueOrFalse},
    {"optimize_syntax_fuse_functions", trueOrFalse},
    {"optimize_throw_if_noop", trueOrFalse},
    {"optimize_time_filter_with_preimage", trueOrFalse},
    {"optimize_trivial_approximate_count_query", trueOrFalse},
    {"optimize_trivial_count_query", trueOrFalse},
    {"optimize_trivial_insert_select", trueOrFalse},
    {"optimize_uniq_to_count", trueOrFalse},
    {"optimize_use_implicit_projections", trueOrFalse},
    {"optimize_use_projections", trueOrFalse},
    {"optimize_using_constraints", trueOrFalse},
    {"output_format_arrow_compression_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"lz4_frame", "zstd", "none"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_arrow_fixed_string_as_fixed_byte_array", trueOrFalse},
    {"output_format_arrow_low_cardinality_as_dictionary", trueOrFalse},
    {"output_format_arrow_string_as_string", trueOrFalse},
    {"output_format_arrow_use_64_bit_indexes_for_dictionary", trueOrFalse},
    {"output_format_arrow_use_signed_indexes_for_dictionary", trueOrFalse},
    //{"output_format_binary_encode_types_in_binary_format", trueOrFalse},
    {"output_format_binary_write_json_as_string", trueOrFalse},
    {"output_format_bson_string_as_string", trueOrFalse},
    {"output_format_csv_crlf_end_of_line", trueOrFalse},
    {"output_format_csv_serialize_tuple_into_separate_columns", trueOrFalse},
    {"output_format_decimal_trailing_zeros", trueOrFalse},
    {"output_format_enable_streaming", trueOrFalse},
    {"output_format_avro_codec",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"null", "deflate", "snappy", "zstd"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_json_array_of_rows", trueOrFalse},
    {"output_format_json_escape_forward_slashes", trueOrFalse},
    {"output_format_json_named_tuples_as_objects", trueOrFalse},
    {"output_format_json_quote_64bit_floats", trueOrFalse},
    {"output_format_json_quote_64bit_integers", trueOrFalse},
    {"output_format_json_quote_decimals", trueOrFalse},
    {"output_format_json_quote_denormals", trueOrFalse},
    {"output_format_json_skip_null_value_in_named_tuples", trueOrFalse},
    {"output_format_json_validate_utf8", trueOrFalse},
    {"output_format_markdown_escape_special_characters", trueOrFalse},
    //{"output_format_native_encode_types_in_binary_format", trueOrFalse},
    {"output_format_native_write_json_as_string", trueOrFalse},
    {"output_format_orc_compression_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"lz4", "snappy", "zlib", "zstd", "none"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_orc_string_as_string", trueOrFalse},
    {"output_format_parallel_formatting", trueOrFalse},
    {"output_format_parquet_compliant_nested_types", trueOrFalse},
    {"output_format_parquet_compression_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"snappy", "lz4", "brotli", "zstd", "gzip", "none"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_parquet_fixed_string_as_fixed_byte_array", trueOrFalse},
    {"output_format_parquet_parallel_encoding", trueOrFalse},
    {"output_format_parquet_string_as_string", trueOrFalse},
    {"output_format_parquet_use_custom_encoder", trueOrFalse},
    {"output_format_parquet_version",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"1.0", "2.4", "2.6", "2.latest"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_parquet_write_page_index", trueOrFalse},
    {"output_format_pretty_color",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "auto"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"output_format_pretty_grid_charset",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "UTF-8" : "ASCII";
         ret += "'";
     }},
    {"output_format_pretty_highlight_digit_groups", trueOrFalse},
    {"output_format_pretty_row_numbers", trueOrFalse},
    {"output_format_protobuf_nullables_with_google_wrappers", trueOrFalse},
    {"output_format_sql_insert_include_column_names", trueOrFalse},
    {"output_format_sql_insert_quote_names", trueOrFalse},
    {"output_format_sql_insert_use_replace", trueOrFalse},
    {"output_format_tsv_crlf_end_of_line", trueOrFalse},
    {"output_format_values_escape_quote_with_quote", trueOrFalse},
    {"output_format_write_statistics", trueOrFalse},
    {"page_cache_inject_eviction", trueOrFalse},
    {"parallel_replica_offset", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }},
    {"parallel_replicas_allow_in_with_subquery", trueOrFalse},
    {"parallel_replicas_count", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.nextSmallNumber() - 1); }},
    {"parallel_replicas_for_non_replicated_merge_tree", trueOrFalse},
    {"parallel_replicas_index_analysis_only_on_coordinator", trueOrFalse},
    {"parallel_replicas_custom_key_range_lower",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"parallel_replicas_custom_key_range_upper",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"parallel_replicas_local_plan", trueOrFalse},
    {"parallel_replicas_mark_segment_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"parallel_replicas_min_number_of_rows_per_replica",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"parallel_replicas_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"sampling_key", "read_tasks", "custom_key_range", "custom_key_sampling", "auto"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"parallel_replicas_prefer_local_join", trueOrFalse},
    {"parallel_view_processing", trueOrFalse},
    {"parallelize_output_from_storages", trueOrFalse},
    {"partial_merge_join_optimizations", trueOrFalse},
    {"partial_result_on_first_cancel", trueOrFalse},
    {"precise_float_parsing", trueOrFalse},
    {"prefer_external_sort_block_bytes",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "100000000"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"prefer_global_in_and_join", trueOrFalse},
    {"prefer_localhost_replica", trueOrFalse},
    {"print_pretty_type_names", trueOrFalse},
    {"push_external_roles_in_interserver_queries", trueOrFalse},
    {"query_cache_compress_entries", trueOrFalse},
    {"query_cache_share_between_users", trueOrFalse},
    {"query_cache_squash_partial_results", trueOrFalse},
    {"query_plan_aggregation_in_order", trueOrFalse},
    {"query_plan_convert_outer_join_to_inner_join", trueOrFalse},
    {"query_plan_enable_multithreading_after_window_functions", trueOrFalse},
    {"query_plan_enable_optimizations", trueOrFalse},
    {"query_plan_execute_functions_after_sorting", trueOrFalse},
    {"query_plan_filter_push_down", trueOrFalse},
    {"query_plan_join_swap_table",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"false", "true", "auto"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"query_plan_lift_up_array_join", trueOrFalse},
    {"query_plan_lift_up_union", trueOrFalse},
    {"query_plan_merge_expressions", trueOrFalse},
    {"query_plan_merge_filters", trueOrFalse},
    {"query_plan_optimize_prewhere", trueOrFalse},
    {"query_plan_push_down_limit", trueOrFalse},
    {"query_plan_read_in_order", trueOrFalse},
    {"query_plan_remove_redundant_distinct", trueOrFalse},
    {"query_plan_remove_redundant_sorting", trueOrFalse},
    {"query_plan_reuse_storage_ordering_for_window_functions", trueOrFalse},
    {"query_plan_split_filter", trueOrFalse},
    {"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", trueOrFalse},
    {"read_from_page_cache_if_exists_otherwise_bypass_cache", trueOrFalse},
    {"read_in_order_two_level_merge_threshold",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 100)); }},
    {"read_in_order_use_buffering", trueOrFalse},
    {"read_in_order_use_virtual_row", trueOrFalse},
    {"read_through_distributed_cache", trueOrFalse},
    {"regexp_dict_allow_hyperscan", trueOrFalse},
    {"regexp_dict_flag_case_insensitive", trueOrFalse},
    {"regexp_dict_flag_dotall", trueOrFalse},
    {"remote_filesystem_read_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "read" : "threadpool";
         ret += "'";
     }},
    {"reject_expensive_hyperscan_regexps", trueOrFalse},
    {"remote_filesystem_read_prefetch", trueOrFalse},
    {"replace_running_query", trueOrFalse},
    {"rewrite_count_distinct_if_with_count_distinct_implementation", trueOrFalse},
    {"rows_before_aggregation", trueOrFalse},
    {"s3_allow_parallel_part_upload", trueOrFalse},
    {"s3_check_objects_after_upload", trueOrFalse},
    {"s3_create_new_file_on_insert", trueOrFalse},
    {"s3_disable_checksum", trueOrFalse},
    {"s3_ignore_file_doesnt_exist", trueOrFalse},
    {"s3_skip_empty_files", trueOrFalse},
    {"s3_throw_on_zero_files_match", trueOrFalse},
    {"s3_truncate_on_insert", trueOrFalse},
    {"s3_use_adaptive_timeouts", trueOrFalse},
    {"s3_validate_request_settings", trueOrFalse},
    {"s3queue_enable_logging_to_s3queue_log", trueOrFalse},
    {"schema_inference_cache_require_modification_time_for_url", trueOrFalse},
    {"schema_inference_use_cache_for_file", trueOrFalse},
    {"schema_inference_use_cache_for_s3", trueOrFalse},
    {"schema_inference_use_cache_for_url", trueOrFalse},
    {"select_sequential_consistency", trueOrFalse},
    {"send_logs_level",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"debug", "information", "trace", "error", "test", "warning", "fatal", "none"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"send_progress_in_http_headers", trueOrFalse},
    {"show_table_uuid_in_table_create_query_if_not_nil", trueOrFalse},
    {"single_join_prefer_left_table", trueOrFalse},
    {"skip_unavailable_shards", trueOrFalse},
    /*{"set_overflow_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "break" : "throw";
         ret += "'";
     }},*/
    {"split_intersecting_parts_ranges_into_layers_final", trueOrFalse},
    {"split_parts_ranges_into_intersecting_and_non_intersecting_final", trueOrFalse},
    {"splitby_max_substrings_includes_remaining_string", trueOrFalse},
    {"stream_like_engine_allow_direct_select", trueOrFalse},
    {"system_events_show_zero_values", trueOrFalse},
    {"throw_on_error_from_cache_on_write_operations", trueOrFalse},
    {"totals_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices
             = {"before_having", "after_having_exclusive", "after_having_inclusive", "after_having_auto"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"trace_profile_events", trueOrFalse},
    {"transform_null_in", trueOrFalse},
    {"traverse_shadow_remote_data_paths", trueOrFalse},
    {"type_json_skip_duplicated_paths", trueOrFalse},
    {"update_insert_deduplication_token_in_dependent_materialized_views", trueOrFalse},
    {"use_async_executor_for_materialized_views", trueOrFalse},
    {"use_cache_for_count_from_files", trueOrFalse},
    {"use_client_time_zone", trueOrFalse},
    {"use_compact_format_in_distributed_parts_names", trueOrFalse},
    {"use_concurrency_control", trueOrFalse},
    {"use_hedged_requests", trueOrFalse},
    {"use_hive_partitioning", trueOrFalse},
    {"use_index_for_in_with_subqueries", trueOrFalse},
    {"use_local_cache_for_remote_storage", trueOrFalse},
    {"use_page_cache_for_disks_without_file_cache", trueOrFalse},
    {"use_query_cache",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices
             = {"1, set_overflow_mode = 'throw', group_by_overflow_mode = 'throw', join_overflow_mode = 'throw'",
                "0, set_overflow_mode = 'break', group_by_overflow_mode = 'break', join_overflow_mode = 'break'"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"use_skip_indexes", trueOrFalse},
    {"use_skip_indexes_if_final", trueOrFalse},
    {"use_structure_from_insertion_table_in_table_functions", zeroOneTwo},
    {"use_uncompressed_cache", trueOrFalse},
    {"use_variant_as_common_type", trueOrFalse},
    {"use_with_fill_by_sorting_prefix", trueOrFalse},
    {"validate_experimental_and_suspicious_types_inside_nested_types", trueOrFalse},
    {"validate_mutation_query", trueOrFalse},
    {"validate_polygons", trueOrFalse},
    //{"wait_for_async_insert", trueOrFalse},
    {"write_through_distributed_cache", trueOrFalse}};

std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mergeTreeTableSettings
    = {{"adaptive_write_buffer_initial_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }},
       {"allow_experimental_block_number_column", trueOrFalse},
       {"allow_experimental_replacing_merge_with_cleanup", trueOrFalse},
       {"allow_floating_point_partition_key", trueOrFalse},
       {"allow_remote_fs_zero_copy_replication", trueOrFalse},
       {"allow_suspicious_indices", trueOrFalse},
       {"allow_vertical_merges_from_compact_to_wide_parts", trueOrFalse},
       {"always_fetch_merged_part", trueOrFalse},
       {"always_use_copy_instead_of_hardlinks", trueOrFalse},
       {"assign_part_uuids", trueOrFalse},
       {"async_insert", trueOrFalse},
       {"cache_populated_by_fetch", trueOrFalse},
       {"check_sample_column_is_correct", trueOrFalse},
       {"compact_parts_max_bytes_to_buffer",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.RandomInt<uint32_t>(1024, UINT32_C(512) * UINT32_C(1024) * UINT32_C(1024))); }},
       {"compact_parts_max_granules_to_buffer",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.15, 0.15, 1, 256)); }},
       {"compact_parts_merge_max_bytes_to_prefetch_part",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 32 * 1024 * 1024)); }},
       {"compatibility_allow_sampling_expression_not_in_primary_key", trueOrFalse},
       {"compress_marks", trueOrFalse},
       {"compress_primary_key", trueOrFalse},
       {"concurrent_part_removal_threshold",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 100)); }},
       {"deduplicate_merge_projection_mode",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"detach_not_byte_identical_parts", trueOrFalse},
       {"detach_old_local_parts_when_cloning_replica", trueOrFalse},
       {"disable_detach_partition_for_zero_copy_replication", trueOrFalse},
       {"disable_fetch_partition_for_zero_copy_replication", trueOrFalse},
       {"disable_freeze_partition_for_zero_copy_replication", trueOrFalse},
       {"enable_block_number_column", trueOrFalse},
       {"enable_block_offset_column", trueOrFalse},
       {"enable_index_granularity_compression", trueOrFalse},
       {"enable_mixed_granularity_parts", trueOrFalse},
       {"enable_vertical_merge_algorithm", trueOrFalse},
       {"enforce_index_structure_match_on_partition_manipulation", trueOrFalse},
       {"exclude_deleted_rows_for_part_size_in_merge", trueOrFalse},
       {"force_read_through_cache_for_merges", trueOrFalse},
       {"fsync_after_insert", trueOrFalse},
       {"fsync_part_directory", trueOrFalse},
       {"index_granularity",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"index_granularity_bytes",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }},
       {"lightweight_mutation_projection_mode",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"throw", "drop", "rebuild"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"load_existing_rows_count_for_old_parts", trueOrFalse},
       {"marks_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"materialize_ttl_recalculate_only", trueOrFalse},
       {"max_bytes_to_merge_at_max_space_in_pool",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"max_bytes_to_merge_at_min_space_in_pool",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"max_file_name_length",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"max_number_of_mutations_for_replica",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 1, 100)); }},
       {"max_parts_to_merge_at_once",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 1000)); }},
       {"max_replicated_merges_in_queue",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }},
       {"max_replicated_mutations_in_queue",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 1, 100)); }},
       {"merge_max_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"merge_max_block_size_bytes",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"merge_selector_enable_heuristic_to_remove_small_parts_at_right", trueOrFalse},
       {"merge_selector_window_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 8192)); }},
       {"min_age_to_force_merge_on_partition_only", trueOrFalse},
       {"min_bytes_for_full_part_storage",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 512 * 1024 * 1024)); }},
       {"min_bytes_for_wide_part",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); }},
       {"min_compressed_bytes_to_fsync_after_fetch",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"min_compressed_bytes_to_fsync_after_merge",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128 * 1024 * 1024)); }},
       {"min_index_granularity_bytes",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1024, 30 * 1024 * 1024)); }},
       {"min_merge_bytes_to_use_direct_io",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.25, 0.25, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"min_parts_to_merge_at_once",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"min_rows_for_full_part_storage",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"min_rows_to_fsync_after_merge",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"min_rows_for_wide_part",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"non_replicated_deduplication_window",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 1000)); }},
       {"old_parts_lifetime",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 10, 8 * 60)); }},
       {"optimize_row_order", trueOrFalse},
       {"prefer_fetch_merged_part_size_threshold",
        [](RandomGenerator & rg, std::string & ret)
        {
            ret += std::to_string(
                rg.thresholdGenerator<uint32_t>(0.2, 0.5, 1, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024)));
        }},
       {"prewarm_mark_cache", trueOrFalse},
       {"primary_key_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"primary_key_lazy_load", trueOrFalse},
       {"prewarm_primary_key_cache", trueOrFalse},
       {"primary_key_ratio_of_unique_prefix_values_to_skip_suffix_columns",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }},
       {"ratio_of_defaults_for_sparse_serialization",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<double>(0.3, 0.5, 0.0, 1.0)); }},
       {"remote_fs_zero_copy_path_compatible_mode", trueOrFalse},
       {"remove_empty_parts", trueOrFalse},
       {"remove_rolled_back_parts_immediately", trueOrFalse},
       {"replace_long_file_name_to_hash", trueOrFalse},
       {"replicated_can_become_leader", trueOrFalse},
       {"replicated_max_mutations_in_one_entry",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.3, 0, 10000)); }},
       {"shared_merge_tree_disable_merges_and_mutations_assignment", trueOrFalse}, /* ClickHouse cloud */
       {"shared_merge_tree_parts_load_batch_size",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }}, /* ClickHouse cloud */
       {"simultaneous_parts_removal_limit",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, 128)); }},
       {"ttl_only_drop_parts", trueOrFalse},
       {"use_adaptive_write_buffer_for_dynamic_subcolumns", trueOrFalse},
       {"use_async_block_ids_cache", trueOrFalse},
       {"use_compact_variant_discriminators_serialization", trueOrFalse},
       {"use_const_adaptive_granularity", trueOrFalse},
       {"use_minimalistic_part_header_in_zookeeper", trueOrFalse},
       {"use_primary_key_cache", trueOrFalse},
       {"vertical_merge_algorithm_min_bytes_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }},
       {"vertical_merge_algorithm_min_columns_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.8, 1, 16)); }},
       {"vertical_merge_algorithm_min_rows_to_activate",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.thresholdGenerator<uint32_t>(0.4, 0.4, 1, 10000)); }},
       {"vertical_merge_remote_filesystem_prefetch", trueOrFalse}};

std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allTableSettings
    = {{MergeTree, mergeTreeTableSettings},
       {ReplacingMergeTree, mergeTreeTableSettings},
       {SummingMergeTree, mergeTreeTableSettings},
       {AggregatingMergeTree, mergeTreeTableSettings},
       {CollapsingMergeTree, mergeTreeTableSettings},
       {VersionedCollapsingMergeTree, mergeTreeTableSettings},
       {File, fileTableSettings},
       {Null, {}},
       {Set, setTableSettings},
       {Join, joinTableSettings},
       {Memory, memoryTableSettings},
       {StripeLog, {}},
       {Log, {}},
       {TinyLog, {}},
       {EmbeddedRocksDB, embeddedRocksDBTableSettings},
       {Buffer, {}},
       {MySQL, mySQLTableSettings},
       {PostgreSQL, {}},
       {SQLite, {}},
       {MongoDB, {}},
       {Redis, {}},
       {S3, {}},
       {S3Queue, s3QueueTableSettings},
       {Hudi, {}},
       {DeltaLake, {}},
       {IcebergS3, {}},
       {Merge, {}}};

void loadFuzzerSettings(const FuzzConfig & fc)
{
    if (!fc.timezones.empty())
    {
        settings_timezones.insert(settings_timezones.end(), fc.timezones.begin(), fc.timezones.end());
        serverSettings.insert(
            {{"session_timezone",
              [&](RandomGenerator & rg, std::string & ret)
              {
                  ret += "'";
                  ret += rg.pickRandomlyFromVector(settings_timezones);
                  ret += "'";
              }}});
    }
    if (!fc.storage_policies.empty())
    {
        merge_storage_policies.insert(merge_storage_policies.end(), fc.storage_policies.begin(), fc.storage_policies.end());
        mergeTreeTableSettings.insert(
            {{"storage_policy",
              [&](RandomGenerator & rg, std::string & ret)
              {
                  ret += "'";
                  ret += rg.pickRandomlyFromVector(merge_storage_policies);
                  ret += "'";
              }}});
    }
}

}
