#pragma once

#include "FuzzConfig.h"
#include "FuzzTimezones.h"
#include "RandomGenerator.h"
#include "SQLProtoStr.h"

#include <cstdint>
#include <functional>
#include <thread>

namespace BuzzHouse
{

const std::function<void(RandomGenerator &, std::string &)> trueOrFalse
    = [](RandomGenerator & rg, std::string & ret) { ret += rg.nextBool() ? "1" : "0"; };

const std::function<void(RandomGenerator &, std::string &)> zeroOneTwo
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 2)); };

const std::function<void(RandomGenerator &, std::string &)> zeroToThree
    = [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 3)); };

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> serverSettings = {
    {"aggregate_functions_null_for_empty", trueOrFalse},
    {"aggregation_in_order_max_block_bytes",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"allow_aggregate_partitions_independently", trueOrFalse},
    {"allow_experimental_parallel_reading_from_replicas", zeroOneTwo},
    {"allow_experimental_shared_set_join", trueOrFalse},
    {"allow_introspection_functions", trueOrFalse},
    {"allow_prefetched_read_pool_for_remote_filesystem", trueOrFalse},
    {"allow_reorder_prewhere_conditions", trueOrFalse},
    {"alter_sync", zeroOneTwo},
    {"any_join_distinct_right_table_keys", trueOrFalse},
    {"async_insert", trueOrFalse},
    {"async_insert_deduplicate", trueOrFalse},
    {"async_insert_threads",
     [](RandomGenerator & rg, std::string & ret)
     { ret += std::to_string(rg.RandomInt<uint32_t>(0, std::thread::hardware_concurrency())); }},
    {"async_insert_use_adaptive_busy_timeout", trueOrFalse},
    {"cast_keep_nullable", trueOrFalse},
    {"cast_string_to_dynamic_use_inference", trueOrFalse},
    {"check_query_single_value_result", trueOrFalse},
    {"checksum_on_read", trueOrFalse},
    {"compile_aggregate_expressions", trueOrFalse},
    {"compile_expressions", trueOrFalse},
    {"compile_sort_description", trueOrFalse},
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
    //{"deduplicate_blocks_in_dependent_materialized_views", trueOrFalse},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", trueOrFalse},
    {"date_time_output_format",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices
             = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"describe_include_subcolumns", trueOrFalse},
    {"dictionary_use_async_executor", trueOrFalse},
    {"distributed_aggregation_memory_efficient", trueOrFalse},
    {"distributed_background_insert_batch", trueOrFalse},
    {"distributed_background_insert_split_batch_on_failure", trueOrFalse},
    {"distributed_foreground_insert", trueOrFalse},
    {"distributed_group_by_no_merge", zeroOneTwo},
    {"enable_analyzer", trueOrFalse},
    {"enable_early_constant_folding", trueOrFalse},
    {"enable_extended_results_for_datetime_functions", trueOrFalse},
    {"enable_http_compression", trueOrFalse},
    {"enable_memory_bound_merging_of_aggregation_results", trueOrFalse},
    {"enable_multiple_prewhere_read_steps", trueOrFalse},
    {"enable_named_columns_in_function_tuple", trueOrFalse},
    {"enable_optimize_predicate_expression", trueOrFalse},
    {"enable_optimize_predicate_expression_to_final_subquery", trueOrFalse},
    {"enable_parallel_replicas", trueOrFalse},
    {"enable_parsing_to_custom_serialization", trueOrFalse},
    {"enable_reads_from_query_cache", trueOrFalse},
    {"enable_scalar_subquery_optimization", trueOrFalse},
    {"enable_sharing_sets_for_mutations", trueOrFalse},
    {"enable_software_prefetch_in_aggregation", trueOrFalse},
    {"enable_unaligned_array_join", trueOrFalse},
    {"enable_vertical_final", trueOrFalse},
    {"enable_writes_to_query_cache", trueOrFalse},
    {"exact_rows_before_limit", trueOrFalse},
    {"extremes", trueOrFalse},
    {"final", trueOrFalse},
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
    {"flatten_nested", trueOrFalse},
    {"force_aggregate_partitions_independently", trueOrFalse},
    {"force_optimize_projection", trueOrFalse},
    {"format_capn_proto_use_autogenerated_schema", trueOrFalse},
    {"format_csv_allow_single_quotes", trueOrFalse},
    {"format_display_secrets_in_show_and_select", trueOrFalse},
    {"format_protobuf_use_autogenerated_schema", trueOrFalse},
    {"format_regexp_skip_unmatched", trueOrFalse},
    {"fsync_metadata", trueOrFalse},
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
    {"http_response_buffer_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"http_wait_end_of_query", trueOrFalse},
    {"input_format_allow_seeks", trueOrFalse},
    {"input_format_arrow_allow_missing_columns", trueOrFalse},
    {"input_format_arrow_case_insensitive_column_matching", trueOrFalse},
    {"input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalse},
    {"input_format_avro_allow_missing_fields", trueOrFalse},
    {"input_format_avro_null_as_default", trueOrFalse},
    {"input_format_binary_decode_types_in_binary_format", trueOrFalse},
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
    {"input_format_native_decode_types_in_binary_format", trueOrFalse},
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
    {"input_format_with_names_use_header", trueOrFalse},
    {"input_format_with_types_use_header", trueOrFalse},
    {"insert_allow_materialized_columns", trueOrFalse},
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
    {"lightweight_deletes_sync", zeroOneTwo},
    {"local_filesystem_read_method",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"read", "pread", "mmap", "pread_threadpool", "io_uring"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"local_filesystem_read_prefetch", trueOrFalse},
    {"log_queries", trueOrFalse},
    {"log_query_threads", trueOrFalse},
    {"low_cardinality_allow_in_native_format", trueOrFalse},
    {"low_cardinality_max_dictionary_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
    {"low_cardinality_use_single_dictionary_for_part", trueOrFalse},
    {"materialize_skip_indexes_on_insert", trueOrFalse},
    {"materialize_statistics_on_insert", trueOrFalse},
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
    {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(std::ceil(rg.RandomZeroOne() * 100.0) / 100.0); }},
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
    {"move_all_conditions_to_prewhere", trueOrFalse},
    {"move_primary_key_columns_to_end_of_prewhere", trueOrFalse},
    {"mutations_sync", zeroOneTwo},
    {"optimize_aggregation_in_order", trueOrFalse},
    {"optimize_aggregators_of_group_by_keys", trueOrFalse},
    {"optimize_append_index", trueOrFalse},
    {"optimize_arithmetic_operations_in_aggregate_functions", trueOrFalse},
    {"optimize_count_from_files", trueOrFalse},
    {"optimize_distinct_in_order", trueOrFalse},
    {"optimize_group_by_constant_keys", trueOrFalse},
    {"optimize_group_by_function_keys", trueOrFalse},
    {"optimize_functions_to_subcolumns", trueOrFalse},
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
    {"optimize_redundant_functions_in_order_by", trueOrFalse},
    {"optimize_rewrite_aggregate_function_with_if", trueOrFalse},
    {"optimize_rewrite_array_exists_to_has", trueOrFalse},
    {"optimize_rewrite_sum_if_to_count_if", trueOrFalse},
    {"optimize_skip_merged_partitions", trueOrFalse},
    {"optimize_skip_unused_shards", trueOrFalse},
    {"optimize_sorting_by_input_stream_properties", trueOrFalse},
    {"optimize_substitute_columns", trueOrFalse},
    {"optimize_syntax_fuse_functions", trueOrFalse},
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
    {"output_format_binary_encode_types_in_binary_format", trueOrFalse},
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
    {"output_format_native_encode_types_in_binary_format", trueOrFalse},
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
    {"parallel_distributed_insert_select", zeroOneTwo},
    {"parallel_replicas_allow_in_with_subquery", trueOrFalse},
    {"parallel_replicas_for_non_replicated_merge_tree", trueOrFalse},
    {"parallel_replicas_local_plan", trueOrFalse},
    {"parallel_replicas_prefer_local_join", trueOrFalse},
    {"parallel_view_processing", trueOrFalse},
    {"parallelize_output_from_storages", trueOrFalse},
    {"partial_merge_join_optimizations", trueOrFalse},
    {"precise_float_parsing", trueOrFalse},
    {"prefer_external_sort_block_bytes",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"0", "1", "100000000"};
         ret += rg.pickRandomlyFromVector(choices);
     }},
    {"prefer_localhost_replica", trueOrFalse},
    {"query_plan_aggregation_in_order", trueOrFalse},
    {"query_plan_convert_outer_join_to_inner_join", trueOrFalse},
    {"query_plan_enable_multithreading_after_window_functions", trueOrFalse},
    {"query_plan_enable_optimizations", trueOrFalse},
    {"query_plan_execute_functions_after_sorting", trueOrFalse},
    {"query_plan_filter_push_down", trueOrFalse},
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
    {"read_in_order_two_level_merge_threshold",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(0, 100)); }},
    {"read_in_order_use_buffering", trueOrFalse},
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
    {"remote_filesystem_read_prefetch", trueOrFalse},
    {"rows_before_aggregation", trueOrFalse},
    {"select_sequential_consistency", trueOrFalse},
    {"send_logs_level",
     [](RandomGenerator & rg, std::string & ret)
     {
         const std::vector<std::string> & choices = {"debug", "information", "trace", "error", "test", "warning", "fatal", "none"};
         ret += "'";
         ret += rg.pickRandomlyFromVector(choices);
         ret += "'";
     }},
    {"session_timezone",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.pickRandomlyFromVector(timezones);
         ret += "'";
     }},
    /*{"set_overflow_mode",
     [](RandomGenerator & rg, std::string & ret)
     {
         ret += "'";
         ret += rg.nextBool() ? "break" : "throw";
         ret += "'";
     }},*/
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
    {"transform_null_in", trueOrFalse},
    {"type_json_skip_duplicated_paths", trueOrFalse},
    {"update_insert_deduplication_token_in_dependent_materialized_views", trueOrFalse},
    {"use_cache_for_count_from_files", trueOrFalse},
    {"use_concurrency_control", trueOrFalse},
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
    {"validate_experimental_and_suspicious_types_inside_nested_types", trueOrFalse}};

extern std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mergeTreeTableSettings;

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> memoryTableSettings
    = {{"min_bytes_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_bytes_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"min_rows_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_rows_to_keep",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> setTableSettings = {{"persistent", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> joinTableSettings = {{"persistent", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> embeddedRocksDBTableSettings = {
    {"optimize_for_bulk_insert", trueOrFalse},
    {"bulk_insert_block_size",
     [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mySQLTableSettings
    = {{"connection_pool_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 7)); }},
       {"connection_max_tries", [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(rg.RandomInt<uint32_t>(1, 16)); }},
       {"connection_auto_close", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> fileTableSettings
    = {{"engine_file_allow_create_multiple_files", trueOrFalse},
       {"engine_file_empty_if_not_exists", trueOrFalse},
       {"engine_file_skip_empty_files", trueOrFalse},
       {"engine_file_truncate_on_insert", trueOrFalse},
       {"storage_file_read_method",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"read", "pread", "mmap"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> s3TableSettings
    = {{"enable_filesystem_cache", trueOrFalse},
       {"filesystem_cache_name",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"cache_for_s3"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"s3_create_new_file_on_insert", trueOrFalse},
       {"s3_skip_empty_files", trueOrFalse},
       {"s3_truncate_on_insert", trueOrFalse}};

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> s3QueueTableSettings
    = {{"after_processing",
        [](RandomGenerator & rg, std::string & ret)
        {
            const std::vector<std::string> & choices = {"", "keep", "delete"};
            ret += "'";
            ret += rg.pickRandomlyFromVector(choices);
            ret += "'";
        }},
       {"enable_logging_to_s3queue_log", trueOrFalse},
       {"processing_threads_num",
        [](RandomGenerator & rg, std::string & ret)
        { ret += std::to_string(rg.RandomInt<uint32_t>(1, std::thread::hardware_concurrency())); }}};

extern std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allTableSettings;

const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> mergeTreeColumnSettings
    = {{"min_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }},
       {"max_compress_block_size",
        [](RandomGenerator & rg, std::string & ret) { ret += std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 21)); }}};

const std::map<TableEngineValues, std::map<std::string, std::function<void(RandomGenerator &, std::string &)>>> allColumnSettings
    = {{MergeTree, mergeTreeColumnSettings},
       {ReplacingMergeTree, mergeTreeColumnSettings},
       {SummingMergeTree, mergeTreeColumnSettings},
       {AggregatingMergeTree, mergeTreeColumnSettings},
       {CollapsingMergeTree, mergeTreeColumnSettings},
       {VersionedCollapsingMergeTree, mergeTreeColumnSettings},
       {File, {}},
       {Null, {}},
       {Set, {}},
       {Join, {}},
       {Memory, {}},
       {StripeLog, {}},
       {Log, {}},
       {TinyLog, {}},
       {EmbeddedRocksDB, {}},
       {Buffer, {}},
       {MySQL, {}},
       {PostgreSQL, {}},
       {SQLite, {}},
       {MongoDB, {}},
       {Redis, {}},
       {S3, {}},
       {S3Queue, {}},
       {Hudi, {}},
       {DeltaLake, {}},
       {IcebergS3, {}}};

void setRandomSetting(
    RandomGenerator & rg,
    const std::map<std::string, std::function<void(RandomGenerator &, std::string &)>> & settings,
    std::string & ret,
    SetValue * set);
void loadFuzzerSettings(const FuzzConfig & fc);

}
