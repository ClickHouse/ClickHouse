#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace DB
{
namespace ErrorCodes
{
extern const int BUZZHOUSE;
}
}

namespace BuzzHouse
{

static const auto nastyStrings = [](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(rg.nasty_strings) + "'"; };

static const auto setSetting = CHSetting(
    [](RandomGenerator & rg, FuzzConfig &)
    {
        static const DB::Strings & choices = {"''", "'ALL'", "'DISTINCT'"};
        return rg.pickRandomly(choices);
    },
    {},
    false);

std::unordered_map<String, CHSetting> hotSettings;

static String settingCombinations(RandomGenerator & rg, DB::Strings && choices)
{
    String res;

    if (rg.nextBool())
    {
        /// Pick just one
        res = rg.pickRandomly(choices);
    }
    else
    {
        /// Pick a combination of some or none
        const uint32_t nalgo = rg.randomInt<uint32_t>(0, static_cast<uint32_t>(choices.size()));

        std::shuffle(choices.begin(), choices.end(), rg.generator);
        for (uint32_t i = 0; i < nalgo; i++)
        {
            if (i != 0)
            {
                res += ",";
            }
            res += choices[i];
        }
    }
    return "'" + res + "'";
}

std::unordered_map<String, CHSetting> performanceSettings
    = {{"allow_aggregate_partitions_independently", trueOrFalseSetting},
       {"allow_execute_multiif_columnar", trueOrFalseSetting},
       {"allow_experimental_query_deduplication", trueOrFalseSetting},
       {"allow_general_join_planning", trueOrFalseSetting},
       {"allow_hyperscan", trueOrFalseSetting},
       {"allow_prefetched_read_pool_for_local_filesystem", trueOrFalseSetting},
       {"allow_prefetched_read_pool_for_remote_filesystem", trueOrFalseSetting},
       {"allow_push_predicate_ast_for_distributed_subqueries", trueOrFalseSetting},
       {"allow_push_predicate_when_subquery_contains_with", trueOrFalseSetting},
       {"allow_reorder_prewhere_conditions", trueOrFalseSetting},
       {"allow_simdjson", trueOrFalseSetting},
       {"allow_statistics_optimize", trueOrFalseSetting},
       {"cluster_function_process_archive_on_multiple_nodes", trueOrFalseSetting},
       {"compile_aggregate_expressions", trueOrFalseSetting},
       {"compile_expressions", trueOrFalseSetting},
       {"compile_sort_description", trueOrFalseSetting},
       {"correlated_subqueries_substitute_equivalent_expressions", trueOrFalseSetting},
       {"count_distinct_implementation",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings & choices = {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"};
                return rg.pickRandomly(choices);
            },
            {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"},
            false)},
       {"count_distinct_optimization", trueOrFalseSetting},
       {"enable_adaptive_memory_spill_scheduler", trueOrFalseSetting},
       {"enable_add_distinct_to_in_subqueries", trueOrFalseSetting},
       {"enable_analyzer", trueOrFalseSetting},
       {"enable_join_runtime_filters", trueOrFalseSetting},
       {"enable_lazy_columns_replication", trueOrFalseSetting},
       {"enable_optimize_predicate_expression", trueOrFalseSetting},
       {"enable_optimize_predicate_expression_to_final_subquery", trueOrFalseSetting},
       {"enable_parallel_replicas", trueOrFalseSetting},
       {"enable_producing_buckets_out_of_order_in_aggregation", trueOrFalseSetting},
       {"join_algorithm",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                return settingCombinations(
                    rg,
                    {"auto",
                     "default",
                     "direct",
                     "full_sorting_merge",
                     "grace_hash",
                     "hash",
                     "parallel_hash",
                     "partial_merge",
                     "prefer_partial_merge"});
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
             "'prefer_partial_merge'"},
            false)},
       {"join_any_take_last_row", trueOrFalseSetting},
       {"low_cardinality_use_single_dictionary_for_part", trueOrFalseSetting},
       {"max_bytes_ratio_before_external_group_by", probRangeNoZeroSetting},
       {"max_bytes_ratio_before_external_sort", probRangeNoZeroSetting},
       {"max_streams_to_max_threads_ratio", probRangeSetting},
       {"merge_tree_determine_task_size_by_prewhere_columns", trueOrFalseSetting},
       {"min_count_to_compile_aggregate_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"min_count_to_compile_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"min_count_to_compile_sort_description", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"move_all_conditions_to_prewhere", trueOrFalseSetting},
       {"move_primary_key_columns_to_end_of_prewhere", trueOrFalseSetting},
       {"optimize_aggregation_in_order", trueOrFalseSetting},
       {"optimize_aggregators_of_group_by_keys", trueOrFalseSetting},
       {"optimize_append_index", trueOrFalseSetting},
       {"optimize_arithmetic_operations_in_aggregate_functions", trueOrFalseSetting},
       {"optimize_and_compare_chain", trueOrFalseSetting},
       {"optimize_distinct_in_order", trueOrFalseSetting},
       {"optimize_distributed_group_by_sharding_key", trueOrFalseSetting},
       {"optimize_empty_string_comparisons", trueOrFalseSetting},
       {"optimize_functions_to_subcolumns", trueOrFalseSetting},
       {"optimize_group_by_constant_keys", trueOrFalseSetting},
       {"optimize_group_by_function_keys", trueOrFalseSetting},
       {"optimize_if_chain_to_multiif", trueOrFalseSetting},
       {"optimize_if_transform_strings_to_enum", trueOrFalseSetting},
       {"optimize_injective_functions_in_group_by", trueOrFalseSetting},
       {"optimize_injective_functions_inside_uniq", trueOrFalseSetting},
       {"optimize_move_to_prewhere", trueOrFalseSetting},
       {"optimize_move_to_prewhere_if_final", trueOrFalseSetting},
       {"optimize_multiif_to_if", trueOrFalseSetting},
       {"optimize_normalize_count_variants", trueOrFalseSetting},
       {"optimize_qbit_distance_function_reads", trueOrFalseSetting},
       {"optimize_read_in_order", trueOrFalseSetting},
       {"optimize_read_in_window_order", trueOrFalseSetting},
       {"optimize_redundant_functions_in_order_by", trueOrFalseSetting},
       {"optimize_respect_aliases", trueOrFalseSetting},
       {"optimize_rewrite_aggregate_function_with_if", trueOrFalseSetting},
       {"optimize_rewrite_array_exists_to_has", trueOrFalseSetting},
       {"optimize_rewrite_like_perfect_affix", trueOrFalseSetting},
       {"optimize_rewrite_regexp_functions", trueOrFalseSetting},
       {"optimize_rewrite_sum_if_to_count_if", trueOrFalseSetting},
       {"optimize_skip_merged_partitions", trueOrFalseSetting},
       {"optimize_skip_unused_shards", trueOrFalseSetting},
       {"optimize_skip_unused_shards_nesting", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
       {"optimize_skip_unused_shards_rewrite_in", trueOrFalseSetting},
       {"optimize_sorting_by_input_stream_properties", trueOrFalseSetting},
       {"optimize_substitute_columns", trueOrFalseSetting},
       {"optimize_syntax_fuse_functions", trueOrFalseSetting},
       {"optimize_trivial_approximate_count_query", trueOrFalseSetting},
       {"optimize_trivial_count_query", trueOrFalseSetting},
       {"optimize_uniq_to_count", trueOrFalseSetting},
       {"optimize_use_implicit_projections", trueOrFalseSetting},
       {"optimize_use_projections", trueOrFalseSetting},
       {"optimize_use_projection_filtering", trueOrFalseSetting},
       {"optimize_inverse_dictionary_lookup", trueOrFalseSetting},
       {"parallel_replicas_only_with_analyzer", trueOrFalseSetting},
       {"parallel_replicas_prefer_local_join", trueOrFalseSetting},
       {"parallel_view_processing", trueOrFalseSetting},
       {"parallelize_output_from_storages", trueOrFalseSetting},
       {"partial_merge_join_optimizations", trueOrFalseSetting},
       {"prefer_global_in_and_join", trueOrFalseSetting},
       {"prefer_localhost_replica", trueOrFalseSetting},
       {"query_plan_aggregation_in_order", trueOrFalseSetting},
       {"query_plan_convert_join_to_in", trueOrFalseSetting},
       {"query_plan_convert_outer_join_to_inner_join", trueOrFalseSetting},
       {"query_plan_direct_read_from_text_index", trueOrFalseSetting},
       {"query_plan_enable_multithreading_after_window_functions", trueOrFalseSetting},
       {"query_plan_enable_optimizations", trueOrFalseSetting},
       {"query_plan_execute_functions_after_sorting", trueOrFalseSetting},
       {"query_plan_filter_push_down", trueOrFalseSetting},
       {"query_plan_optimize_join_order_limit",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 64)); },
            {"0", "1", "2", "4", "16", "64"},
            false)},
       {"query_plan_join_shard_by_pk_ranges", trueOrFalseSetting},
       {"query_plan_join_swap_table",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings & choices = {"'false'", "'true'", "'auto'"};
                return rg.pickRandomly(choices);
            },
            {"'false'", "'true'", "'auto'"},
            false)},
       {"query_plan_lift_up_array_join", trueOrFalseSetting},
       {"query_plan_lift_up_union", trueOrFalseSetting},
       {"query_plan_merge_expressions", trueOrFalseSetting},
       {"query_plan_merge_filter_into_join_condition", trueOrFalseSetting},
       {"query_plan_merge_filters", trueOrFalseSetting},
       {"query_plan_optimize_join_order_algorithm",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &) { return settingCombinations(rg, {"greedy", "dpsize"}); },
            {"'greedy'", "'dpsize'"},
            false)},
       {"query_plan_optimize_lazy_materialization", trueOrFalseSetting},
       {"query_plan_optimize_prewhere", trueOrFalseSetting},
       {"query_plan_push_down_limit", trueOrFalseSetting},
       {"query_plan_read_in_order", trueOrFalseSetting},
       {"query_plan_remove_redundant_distinct", trueOrFalseSetting},
       {"query_plan_remove_redundant_sorting", trueOrFalseSetting},
       {"query_plan_remove_unused_columns", trueOrFalseSetting},
       {"query_plan_reuse_storage_ordering_for_window_functions", trueOrFalseSetting},
       {"query_plan_split_filter", trueOrFalseSetting},
       {"query_plan_try_use_vector_search", trueOrFalseSetting},
       {"read_in_order_two_level_merge_threshold",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 100)); },
            {"0", "1", "10", "100"},
            false)},
       {"read_in_order_use_buffering", trueOrFalseSetting},
       {"read_in_order_use_virtual_row", trueOrFalseSetting},
       {"remerge_sort_lowered_memory_bytes_ratio",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 4.0)); },
            {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1", "1.5", "2", "2.5"},
            false)},
       {"rewrite_count_distinct_if_with_count_distinct_implementation", trueOrFalseSetting},
       {"rewrite_in_to_join", trueOrFalseSetting},
       {"short_circuit_function_evaluation",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings & choices = {"'enable'", "'force_enable'", "'disable'"};
                return rg.pickRandomly(choices);
            },
            {"'enable'", "'force_enable'", "'disable'"},
            false)},
       {"single_join_prefer_left_table", trueOrFalseSetting},
       {"split_intersecting_parts_ranges_into_layers_final", trueOrFalseSetting},
       {"split_parts_ranges_into_intersecting_and_non_intersecting_final", trueOrFalseSetting},
       {"temporary_files_codec",
        CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings & choices = {"'lz4'", "'none'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"text_index_use_bloom_filter", trueOrFalseSetting},
       {"use_concurrency_control", trueOrFalseSetting},
       {"use_iceberg_partition_pruning", trueOrFalseSetting},
       {"use_index_for_in_with_subqueries", trueOrFalseSetting},
       {"use_index_for_in_with_subqueries_max_values", trueOrFalseSetting},
       {"use_join_disjunctions_push_down", trueOrFalseSetting},
       /// ClickHouse cloud setting
       {"use_page_cache_with_distributed_cache", trueOrFalseSetting},
       {"use_query_condition_cache", trueOrFalseSetting},
       {"use_skip_indexes", trueOrFalseSetting},
       {"use_skip_indexes_for_disjunctions", trueOrFalseSetting},
       {"use_skip_indexes_for_top_k", trueOrFalseSetting},
       {"use_skip_indexes_if_final", trueOrFalseSetting},
       {"use_skip_indexes_on_data_read", trueOrFalseSetting},
       {"use_statistics_cache", trueOrFalseSetting},
       {"use_top_k_dynamic_filtering", trueOrFalseSetting},
       {"use_uncompressed_cache", trueOrFalseSetting}};

std::unordered_map<String, CHSetting> serverSettings = {
    {"add_http_cors_header", trueOrFalseSettingNoOracle},
    {"aggregate_function_input_format",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'state'", "'value'", "'array'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"aggregate_functions_null_for_empty", trueOrFalseSettingNoOracle},
    {"aggregation_memory_efficient_merge_threads", threadSetting},
    {"allow_archive_path_syntax", trueOrFalseSettingNoOracle},
    {"allow_asynchronous_read_from_io_pool_for_merge_tree", trueOrFalseSetting},
    {"allow_changing_replica_until_first_data_packet", trueOrFalseSettingNoOracle},
    {"allow_dynamic_type_in_join_keys", trueOrFalseSettingNoOracle},
    {"allow_experimental_delta_kernel_rs", trueOrFalseSettingNoOracle},
    {"allow_get_client_http_header", trueOrFalseSettingNoOracle},
    {"allow_introspection_functions", trueOrFalseSetting},
    {"allow_special_bool_values_inside_variant", trueOrFalseSettingNoOracle},
    {"allow_special_serialization_kinds_in_output_formats", trueOrFalseSetting},
    {"alter_move_to_space_execute_async", trueOrFalseSettingNoOracle},
    {"alter_partition_verbose_result", trueOrFalseSettingNoOracle},
    {"alter_sync", CHSetting(zeroOneTwo, {}, false)},
    {"alter_update_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'heavy'", "'lightweight'", "'lightweight_force'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"allow_unrestricted_reads_from_keeper", trueOrFalseSettingNoOracle},
    {"analyze_index_with_space_filling_curves", trueOrFalseSetting},
    {"analyzer_compatibility_join_using_top_level_identifier", trueOrFalseSetting},
    {"apply_mutations_on_fly", trueOrFalseSettingNoOracle},
    {"apply_patch_parts", trueOrFalseSetting},
    {"apply_settings_from_server", trueOrFalseSettingNoOracle},
    {"any_join_distinct_right_table_keys", trueOrFalseSetting},
    {"asterisk_include_alias_columns", trueOrFalseSettingNoOracle},
    {"asterisk_include_materialized_columns", trueOrFalseSettingNoOracle},
    {"async_insert", trueOrFalseSettingNoOracle},
    {"async_insert_deduplicate", trueOrFalseSettingNoOracle},
    {"async_insert_threads", threadSetting},
    {"async_insert_use_adaptive_busy_timeout", trueOrFalseSettingNoOracle},
    {"async_query_sending_for_remote", trueOrFalseSetting},
    {"async_socket_for_remote", trueOrFalseSetting},
    {"cache_warmer_threads", threadSetting},
    {"calculate_text_stack_trace", trueOrFalseSettingNoOracle},
    {"cancel_http_readonly_queries_on_client_close", trueOrFalseSettingNoOracle},
    {"cast_ipv4_ipv6_default_on_conversion_error", trueOrFalseSettingNoOracle},
    {"cast_keep_nullable", trueOrFalseSettingNoOracle},
    {"cast_string_to_date_time_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'best_effort'", "'best_effort_us'", "'basic'"};
             return rg.pickRandomly(choices);
         },
         {"'best_effort'", "'best_effort_us'", "'basic'"},
         false)},
    {"cast_string_to_dynamic_use_inference", trueOrFalseSettingNoOracle},
    {"cast_string_to_variant_use_inference", trueOrFalseSettingNoOracle},
    {"check_query_single_value_result", trueOrFalseSetting},
    {"check_referential_table_dependencies", trueOrFalseSettingNoOracle},
    {"check_table_dependencies", trueOrFalseSettingNoOracle},
    {"checksum_on_read", trueOrFalseSettingNoOracle},
    {"cloud_mode", trueOrFalseSettingNoOracle},
    {"cloud_mode_database_engine", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "1" : "2"; }, {}, false)},
    {"cloud_mode_engine",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"cluster_table_function_split_granularity",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'file'", "'bucket'"};
             return rg.pickRandomly(choices);
         },
         {"'file'", "'bucket'"},
         false)},
    {"collect_hash_table_stats_during_aggregation", trueOrFalseSetting},
    {"collect_hash_table_stats_during_joins", trueOrFalseSetting},
    {"compatibility_ignore_auto_increment_in_create_table", trueOrFalseSettingNoOracle},
    {"compatibility_ignore_collation_in_create_table", trueOrFalseSettingNoOracle},
    {"convert_query_to_cnf", trueOrFalseSettingNoOracle},
    {"correlated_subqueries_default_join_kind",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'left'", "'right'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"create_table_empty_primary_key_by_default", trueOrFalseSettingNoOracle},
    {"cross_to_inner_join_rewrite", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
    {"database_atomic_wait_for_drop_and_detach_synchronously", trueOrFalseSettingNoOracle},
    {"database_replicated_allow_explicit_uuid", CHSetting(zeroOneTwo, {}, false)},
    {"database_replicated_allow_heavy_create", trueOrFalseSettingNoOracle},
    {"database_replicated_allow_replicated_engine_arguments",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"database_replicated_always_detach_permanently", trueOrFalseSettingNoOracle},
    {"database_replicated_enforce_synchronous_settings", trueOrFalseSettingNoOracle},
    {"data_type_default_nullable", trueOrFalseSettingNoOracle},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", trueOrFalseSettingNoOracle},
    {"date_time_output_format",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices
                 = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"date_time_overflow_behavior",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'ignore'", "'saturate'", "'throw'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"decimal_check_overflow", trueOrFalseSettingNoOracle},
    {"delta_lake_enable_engine_predicate", trueOrFalseSetting},
    {"delta_lake_enable_expression_visitor_logging", trueOrFalseSettingNoOracle},
    {"delta_lake_log_metadata", trueOrFalseSettingNoOracle},
    {"delta_lake_throw_on_engine_predicate_error", trueOrFalseSettingNoOracle},
    {"describe_include_subcolumns", trueOrFalseSettingNoOracle},
    {"describe_include_virtual_columns", trueOrFalseSettingNoOracle},
    {"dictionary_use_async_executor", trueOrFalseSettingNoOracle},
    {"dictionary_validate_primary_key_type", trueOrFalseSettingNoOracle},
    {"distributed_aggregation_memory_efficient", trueOrFalseSetting},
    {"distributed_background_insert_batch", trueOrFalseSettingNoOracle},
    {"distributed_background_insert_split_batch_on_failure", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_bypass_connection_pool", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_discard_connection_if_unread_data", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_fetch_metrics_only_from_current_az", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_log_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'nothing'", "'on_error'", "'all'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    /// ClickHouse cloud setting
    {"distributed_cache_prefer_bigger_buffer_size", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_read_only_from_current_az", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"distributed_cache_throw_on_error", trueOrFalseSettingNoOracle},
    {"distributed_foreground_insert", trueOrFalseSettingNoOracle},
    {"distributed_group_by_no_merge", CHSetting(zeroOneTwo, {}, false)},
    {"distributed_insert_skip_read_only_replicas", trueOrFalseSettingNoOracle},
    {"distributed_plan_default_reader_bucket_count",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); }, {}, false)},
    {"distributed_plan_default_shuffle_join_bucket_count",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 128)); }, {}, false)},
    {"distributed_plan_execute_locally", trueOrFalseSetting},
    {"distributed_plan_force_exchange_kind",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"''", "'Persisted'", "'Streaming'"};
             return rg.pickRandomly(choices);
         },
         {"''", "'Persisted'", "'Streaming'"},
         false)},
    {"distributed_plan_optimize_exchanges", trueOrFalseSetting},
    {"distributed_plan_force_shuffle_aggregation", trueOrFalseSetting},
    {"distributed_product_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'deny'", "'local'", "'global'", "'allow'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"distributed_push_down_limit", trueOrFalseSetting},
    {"do_not_merge_across_partitions_select_final", trueOrFalseSettingNoOracle},
    {"empty_result_for_aggregation_by_constant_keys_on_empty_set", trueOrFalseSettingNoOracle},
    {"enable_blob_storage_log", trueOrFalseSettingNoOracle},
    {"enable_early_constant_folding", trueOrFalseSettingNoOracle},
    {"enable_extended_results_for_datetime_functions", trueOrFalseSettingNoOracle},
    {"enable_filesystem_cache", trueOrFalseSetting},
    {"enable_filesystem_cache_log", trueOrFalseSettingNoOracle},
    {"enable_filesystem_cache_on_write_operations", trueOrFalseSettingNoOracle},
    {"enable_filesystem_read_prefetches_log", trueOrFalseSetting},
    {"enable_global_with_statement", trueOrFalseSettingNoOracle},
    {"enable_hdfs_pread", trueOrFalseSettingNoOracle},
    {"enable_http_compression", trueOrFalseSettingNoOracle},
    {"enable_job_stack_trace", trueOrFalseSettingNoOracle},
    {"enable_memory_bound_merging_of_aggregation_results", trueOrFalseSetting},
    {"enable_multiple_prewhere_read_steps", trueOrFalseSetting},
    {"enable_named_columns_in_function_tuple", trueOrFalseSettingNoOracle},
    {"enable_parallel_blocks_marshalling", trueOrFalseSetting},
    {"enable_parsing_to_custom_serialization", trueOrFalseSetting},
    {"enable_reads_from_query_cache", trueOrFalseSetting},
    {"enable_s3_requests_logging", trueOrFalseSettingNoOracle},
    {"enable_scalar_subquery_optimization", trueOrFalseSetting},
    {"enable_scopes_for_with_statement", trueOrFalseSettingNoOracle},
    {"enable_shared_storage_snapshot_in_query", trueOrFalseSetting},
    {"enable_sharing_sets_for_mutations", trueOrFalseSetting},
    {"enable_software_prefetch_in_aggregation", trueOrFalseSetting},
    {"enable_unaligned_array_join", trueOrFalseSetting},
    {"enable_url_encoding", trueOrFalseSettingNoOracle},
    {"enable_vertical_final", trueOrFalseSetting},
    {"enable_writes_to_query_cache", trueOrFalseSetting},
    {"engine_file_allow_create_multiple_files", trueOrFalseSettingNoOracle},
    {"engine_file_empty_if_not_exists", trueOrFalseSettingNoOracle},
    {"engine_file_skip_empty_files", trueOrFalseSettingNoOracle},
    {"engine_url_skip_empty_files", trueOrFalseSettingNoOracle},
    /// {"exact_rows_before_limit", trueOrFalseSetting}, cannot use with generateRandom
    {"except_default_mode", setSetting},
    {"exclude_materialize_skip_indexes_on_insert",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return settingCombinations(rg, {"i0", "i1", "i2", "i3"}); }, {}, false)},
    {"extremes", trueOrFalseSettingNoOracle},
    {"fallback_to_stale_replicas_for_distributed_queries", trueOrFalseSetting},
    {"filesystem_cache_allow_background_download", trueOrFalseSettingNoOracle},
    {"filesystem_cache_enable_background_download_during_fetch", trueOrFalseSettingNoOracle},
    {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", trueOrFalseSettingNoOracle},
    {"filesystem_cache_prefer_bigger_buffer_size", trueOrFalseSetting},
    {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", trueOrFalseSettingNoOracle},
    {"filesystem_cache_segments_batch_size",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             const std::vector<uint32_t> choices{0, 3, 10, 50};
             return std::to_string(rg.pickRandomly(choices));
         },
         {},
         false)},
    {"filesystem_prefetch_step_marks", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "0" : "50"; }, {}, false)},
    {"filesystem_prefetches_limit", CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "0" : "10"; }, {}, false)},
    {"final", trueOrFalseSettingNoOracle},
    {"flatten_nested", trueOrFalseSetting},
    {"force_aggregate_partitions_independently", trueOrFalseSetting},
    {"force_grouping_standard_compatibility", trueOrFalseSettingNoOracle},
    {"force_optimize_skip_unused_shards", CHSetting(zeroOneTwo, {}, false)},
    {"force_optimize_skip_unused_shards_nesting", CHSetting(zeroOneTwo, {}, false)},
    {"force_remove_data_recursively_on_drop", trueOrFalseSettingNoOracle},
    {"format_capn_proto_use_autogenerated_schema", trueOrFalseSettingNoOracle},
    {"format_display_secrets_in_show_and_select", trueOrFalseSettingNoOracle},
    {"format_protobuf_use_autogenerated_schema", trueOrFalseSettingNoOracle},
    {"format_regexp_skip_unmatched", trueOrFalseSettingNoOracle},
    {"fsync_metadata", trueOrFalseSetting},
    {"function_date_trunc_return_type_behavior", trueOrFalseSettingNoOracle},
    {"function_json_value_return_type_allow_complex", trueOrFalseSettingNoOracle},
    {"function_json_value_return_type_allow_nullable", trueOrFalseSettingNoOracle},
    {"function_locate_has_mysql_compatible_argument_order", trueOrFalseSettingNoOracle},
    {"function_visible_width_behavior", trueOrFalseSettingNoOracle},
    {"geo_distance_returns_float64_on_float64_arguments", trueOrFalseSettingNoOracle},
    {"grace_hash_join_initial_buckets",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    {"grace_hash_join_max_buckets",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    {"group_by_use_nulls", trueOrFalseSettingNoOracle},
    {"hdfs_create_new_file_on_insert", trueOrFalseSettingNoOracle},
    {"hdfs_ignore_file_doesnt_exist", trueOrFalseSettingNoOracle},
    {"hdfs_skip_empty_files", trueOrFalseSettingNoOracle},
    {"hdfs_throw_on_zero_files_match", trueOrFalseSettingNoOracle},
    {"http_make_head_request", trueOrFalseSettingNoOracle},
    {"http_native_compression_disable_checksumming_on_decompress", trueOrFalseSettingNoOracle},
    {"http_response_buffer_size", CHSetting(highRange, {}, false)},
    {"http_skip_not_found_url_for_globs", trueOrFalseSettingNoOracle},
    {"http_wait_end_of_query", trueOrFalseSettingNoOracle},
    {"http_write_exception_in_output_format", trueOrFalseSettingNoOracle},
    {"iceberg_delete_data_on_drop", trueOrFalseSettingNoOracle},
    {"iceberg_metadata_compression_method",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(compressionMethods) + "'"; }, {}, false)},
    {"iceberg_metadata_log_level",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices
                 = {"'none'",
                    "'metadata'",
                    "'manifest_list_metadata'",
                    "'manifest_list_entry'",
                    "'manifest_file_metadata'",
                    "'manifest_file_entry'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"iceberg_snapshot_id",
     CHSetting([](RandomGenerator &, FuzzConfig & fc) { return fc.getRandomIcebergHistoryValue("\"snapshot_id\""); }, {}, false)},
    {"iceberg_timestamp_ms",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig & fc)
         {
             if (rg.nextBool())
             {
                 return fc.getRandomIcebergHistoryValue("toUnixTimestamp64Milli(\"made_current_at\")");
             }
             else
             {
                 static const std::vector<uint32_t> & values = {1, 2, 3, 5, 10, 15, 20};
                 const auto now = std::chrono::system_clock::now();

                 // Convert to milliseconds since epoch
                 auto ms = duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
                 ms -= (rg.pickRandomly(values) * 1000);
                 return std::to_string(ms);
             }
         },
         {},
         false)},
    /// ClickHouse cloud setting
    {"ignore_cold_parts_seconds",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 60)); }, {}, false)},
    {"ignore_materialized_views_with_dropped_target_table", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_access_entities_queries", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_named_collections_queries", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_udf_queries", trueOrFalseSettingNoOracle},
    {"implicit_select", trueOrFalseSettingNoOracle},
    {"input_format_allow_errors_num", CHSetting(highRange, {}, false)},
    {"input_format_allow_errors_ratio", CHSetting(probRange, {}, false)},
    {"input_format_allow_seeks", trueOrFalseSettingNoOracle},
    {"input_format_arrow_allow_missing_columns", trueOrFalseSettingNoOracle},
    {"input_format_arrow_case_insensitive_column_matching", trueOrFalseSettingNoOracle},
    {"input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_avro_allow_missing_fields", trueOrFalseSettingNoOracle},
    {"input_format_avro_null_as_default", trueOrFalseSettingNoOracle},
    {"input_format_binary_read_json_as_string", trueOrFalseSettingNoOracle},
    {"input_format_bson_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_csv_allow_cr_end_of_line", trueOrFalseSettingNoOracle},
    {"input_format_csv_allow_variable_number_of_columns", trueOrFalseSettingNoOracle},
    {"input_format_csv_allow_whitespace_or_tab_as_delimiter", trueOrFalseSettingNoOracle},
    {"input_format_csv_deserialize_separate_columns_into_tuple", trueOrFalseSettingNoOracle},
    {"input_format_csv_empty_as_default", trueOrFalseSettingNoOracle},
    {"input_format_csv_skip_trailing_empty_lines", trueOrFalseSettingNoOracle},
    {"input_format_csv_trim_whitespaces", trueOrFalseSettingNoOracle},
    {"input_format_csv_try_infer_numbers_from_strings", trueOrFalseSettingNoOracle},
    {"input_format_csv_try_infer_strings_from_quoted_tuples", trueOrFalseSettingNoOracle},
    {"input_format_csv_use_best_effort_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_csv_use_default_on_bad_values", trueOrFalseSettingNoOracle},
    {"input_format_custom_allow_variable_number_of_columns", trueOrFalseSettingNoOracle},
    {"input_format_custom_skip_trailing_empty_lines", trueOrFalseSettingNoOracle},
    {"input_format_defaults_for_omitted_fields", trueOrFalseSettingNoOracle},
    {"input_format_force_null_for_omitted_fields", trueOrFalseSettingNoOracle},
    {"input_format_hive_text_allow_variable_number_of_columns", trueOrFalseSettingNoOracle},
    {"input_format_import_nested_json", trueOrFalseSettingNoOracle},
    {"input_format_ipv4_default_on_conversion_error", trueOrFalseSettingNoOracle},
    {"input_format_ipv6_default_on_conversion_error", trueOrFalseSettingNoOracle},
    {"input_format_json_compact_allow_variable_number_of_columns", trueOrFalseSettingNoOracle},
    {"input_format_json_defaults_for_missing_elements_in_named_tuple", trueOrFalseSettingNoOracle},
    {"input_format_json_ignore_unknown_keys_in_named_tuple", trueOrFalseSettingNoOracle},
    {"input_format_json_ignore_unnecessary_fields", trueOrFalseSettingNoOracle},
    {"input_format_json_infer_incomplete_types_as_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_named_tuples_as_objects", trueOrFalseSettingNoOracle},
    {"input_format_json_read_arrays_as_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_read_bools_as_numbers", trueOrFalseSettingNoOracle},
    {"input_format_json_read_bools_as_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_read_numbers_as_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_read_objects_as_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_throw_on_bad_escape_sequence", trueOrFalseSettingNoOracle},
    {"input_format_json_try_infer_named_tuples_from_objects", trueOrFalseSettingNoOracle},
    {"input_format_json_try_infer_numbers_from_strings", trueOrFalseSettingNoOracle},
    {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", trueOrFalseSettingNoOracle},
    {"input_format_json_validate_types_from_metadata", trueOrFalseSettingNoOracle},
    {"input_format_native_allow_types_conversion", trueOrFalseSettingNoOracle},
    {"input_format_null_as_default", trueOrFalseSettingNoOracle},
    {"input_format_orc_allow_missing_columns", trueOrFalseSettingNoOracle},
    {"input_format_orc_case_insensitive_column_matching", trueOrFalseSettingNoOracle},
    {"input_format_orc_dictionary_as_low_cardinality", trueOrFalseSettingNoOracle},
    {"input_format_orc_filter_push_down", trueOrFalseSetting},
    {"input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_orc_use_fast_decoder", trueOrFalseSettingNoOracle},
    {"input_format_parallel_parsing", trueOrFalseSetting},
    {"input_format_parquet_allow_missing_columns", trueOrFalseSettingNoOracle},
    {"input_format_parquet_bloom_filter_push_down", trueOrFalseSetting},
    {"input_format_parquet_case_insensitive_column_matching", trueOrFalseSettingNoOracle},
    {"input_format_parquet_enable_json_parsing", trueOrFalseSettingNoOracle},
    {"input_format_parquet_enable_row_group_prefetch", trueOrFalseSettingNoOracle},
    {"input_format_parquet_verify_checksums", trueOrFalseSettingNoOracle},
    {"input_format_parquet_local_time_as_utc", trueOrFalseSettingNoOracle},
    {"input_format_parquet_filter_push_down", trueOrFalseSetting},
    {"input_format_parquet_preserve_order", trueOrFalseSettingNoOracle},
    {"input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_parquet_use_native_reader_v3", trueOrFalseSetting},
    {"input_format_parquet_page_filter_push_down", trueOrFalseSetting},
    {"input_format_parquet_use_offset_index", trueOrFalseSetting},
    {"input_format_protobuf_flatten_google_wrappers", trueOrFalseSettingNoOracle},
    {"input_format_protobuf_oneof_presence", trueOrFalseSettingNoOracle},
    {"input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_skip_unknown_fields", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_dates", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_datetimes", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_datetimes_only_datetime64", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_exponent_floats", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_integers", trueOrFalseSettingNoOracle},
    {"input_format_try_infer_variants", trueOrFalseSettingNoOracle},
    {"input_format_tsv_allow_variable_number_of_columns", trueOrFalseSettingNoOracle},
    {"input_format_tsv_empty_as_default", trueOrFalseSettingNoOracle},
    {"input_format_tsv_enum_as_number", trueOrFalseSettingNoOracle},
    {"input_format_tsv_skip_trailing_empty_lines", trueOrFalseSettingNoOracle},
    {"input_format_tsv_use_best_effort_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_values_accurate_types_of_literals", trueOrFalseSettingNoOracle},
    {"input_format_values_deduce_templates_of_expressions", trueOrFalseSettingNoOracle},
    {"insert_allow_materialized_columns", trueOrFalseSettingNoOracle},
    {"insert_deduplicate", trueOrFalseSettingNoOracle},
    {"insert_distributed_one_random_shard", trueOrFalseSettingNoOracle},
    {"insert_null_as_default", trueOrFalseSettingNoOracle},
    {"insert_quorum", CHSetting(zeroOneTwo, {}, false)},
    {"insert_quorum_parallel", trueOrFalseSettingNoOracle},
    {"insert_shard_id",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 2)); }, {}, false)},
    {"intersect_default_mode", setSetting},
    {"interval_output_format",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "'kusto'" : "'numeric'"; }, {}, false)},
    {"join_default_strictness",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'ALL'", "'ANY'", "''"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"join_on_disk_max_files_to_merge",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 32)); }, {}, false)},
    {"join_runtime_bloom_filter_hash_functions",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 20)); },
         {"0", "1", "2", "3", "4", "10"},
         false)},
    {"join_use_nulls", trueOrFalseSettingNoOracle},
    {"joined_block_split_single_row", trueOrFalseSetting},
    {"keeper_map_strict_mode", trueOrFalseSettingNoOracle},
    {"least_greatest_legacy_null_behavior", trueOrFalseSettingNoOracle},
    {"legacy_column_name_of_tuple_literal", trueOrFalseSettingNoOracle},
    {"lightweight_delete_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'alter_update'", "'lightweight_update'", "'lightweight_update_force'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"load_balancing",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {
                 "'round_robin'", "'in_order'", "'hostname_levenshtein_distance'", "'nearest_hostname'", "'first_or_random'", "'random'"};
             return rg.pickRandomly(choices);
         },
         {"'round_robin'", "'in_order'", "'hostname_levenshtein_distance'", "'nearest_hostname'", "'first_or_random'", "'random'"},
         false)},
    {"load_balancing_first_offset",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 8)); }, {}, false)},
    {"load_marks_asynchronously", trueOrFalseSettingNoOracle},
    {"local_filesystem_read_method",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"};
             return rg.pickRandomly(choices);
         },
         {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"},
         false)}};

static std::unordered_map<String, CHSetting> serverSettings2 = {
    {"local_filesystem_read_prefetch", trueOrFalseSetting},
    {"log_formatted_queries", trueOrFalseSettingNoOracle},
    {"log_processors_profiles", trueOrFalseSettingNoOracle},
    {"log_profile_events", trueOrFalseSettingNoOracle},
    {"log_queries", trueOrFalseSettingNoOracle},
    {"log_query_settings", trueOrFalseSettingNoOracle},
    {"log_query_threads", trueOrFalseSetting},
    {"log_query_views", trueOrFalseSetting},
    {"low_cardinality_max_dictionary_size", CHSetting(highRange, {}, false)},
    {"make_distributed_plan", trueOrFalseSetting},
    {"materialize_skip_indexes_on_insert", trueOrFalseSettingNoOracle},
    {"materialize_statistics_on_insert", trueOrFalseSettingNoOracle},
    {"materialize_ttl_after_modify", trueOrFalseSettingNoOracle},
    {"materialized_views_ignore_errors", trueOrFalseSettingNoOracle},
    {"materialized_views_squash_parallel_inserts", trueOrFalseSettingNoOracle},
    {"max_download_threads", threadSetting},
    {"max_final_threads", threadSetting},
    {"max_insert_delayed_streams_for_parallel_write",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 12)); }, {}, false)},
    {"max_insert_threads", threadSetting},
    {"max_parallel_replicas",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.3, 0.2, 0, 5)); }, {}, false)},
    {"max_parsing_threads", threadSetting},
    {"max_parts_to_move",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(4096))); },
         {"0", "1", "100", "1000"},
         false)},
    {"max_threads", threadSetting},
    {"max_threads_for_indexes", threadSetting},
    {"merge_tree_coarse_index_granularity",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(2, 32)); }, {}, false)},
    {"merge_tree_compact_parts_min_granules_to_multibuffer_read",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(1, 128)); }, {}, false)},
    {"merge_tree_use_const_size_tasks_for_remote_reading", trueOrFalseSettingNoOracle},
    {"merge_tree_use_v1_object_and_dynamic_serialization", trueOrFalseSettingNoOracle},
    {"metrics_perf_events_enabled", trueOrFalseSettingNoOracle},
    {"min_hit_rate_to_use_consecutive_keys_optimization", probRangeSetting},
    {"mongodb_throw_on_unsupported_query", trueOrFalseSettingNoOracle},
    {"multiple_joins_try_to_keep_original_names", trueOrFalseSetting},
    {"mutations_execute_nondeterministic_on_initiator", trueOrFalseSetting},
    {"mutations_execute_subqueries_on_initiator", trueOrFalseSetting},
    {"mutations_sync", CHSetting(zeroOneTwo, {}, false)},
    {"mysql_map_fixed_string_to_text_in_show_columns", trueOrFalseSettingNoOracle},
    {"mysql_map_string_to_text_in_show_columns", trueOrFalseSettingNoOracle},
    {"normalize_function_names", trueOrFalseSetting},
    {"opentelemetry_trace_cpu_scheduling", trueOrFalseSettingNoOracle},
    {"optimize_const_name_size",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<int32_t>(-100, 100)); }, {}, false)},
    {"optimize_count_from_files", trueOrFalseSetting},
    {"optimize_extract_common_expressions", trueOrFalseSetting},
    {"optimize_min_equality_disjunction_chain_length",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {"0", "1", "5", "10"}, false)},
    {"optimize_min_inequality_conjunction_chain_length",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 10)); }, {"0", "1", "5", "10"}, false)},
    {"optimize_on_insert", trueOrFalseSetting},
    {"optimize_or_like_chain", trueOrFalseSetting},
    {"optimize_skip_unused_shards_limit",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 30)); }, {"0", "1", "5", "10"}, false)},
    {"optimize_throw_if_noop", trueOrFalseSettingNoOracle},
    {"optimize_time_filter_with_preimage", trueOrFalseSetting},
    {"optimize_trivial_insert_select", trueOrFalseSetting},
    {"output_format_arrow_compression_method",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'lz4_frame'", "'zstd'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_arrow_fixed_string_as_fixed_byte_array", trueOrFalseSettingNoOracle},
    {"output_format_arrow_low_cardinality_as_dictionary", trueOrFalseSettingNoOracle},
    {"output_format_arrow_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_arrow_use_64_bit_indexes_for_dictionary", trueOrFalseSettingNoOracle},
    {"output_format_arrow_use_signed_indexes_for_dictionary", trueOrFalseSettingNoOracle},
    {"output_format_avro_codec",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'null'", "'deflate'", "'snappy'", "'zstd'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_avro_sync_interval",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 32, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); },
         {"32", "1024", "4096", "16384", "'10M'"},
         false)},
    {"output_format_binary_write_json_as_string", trueOrFalseSettingNoOracle},
    {"output_format_bson_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_compression_level",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 1, 22)); }, {}, false)},
    {"output_format_csv_serialize_tuple_into_separate_columns", trueOrFalseSettingNoOracle},
    {"output_format_decimal_trailing_zeros", trueOrFalseSettingNoOracle},
    {"output_format_enable_streaming", trueOrFalseSettingNoOracle},
    {"output_format_json_array_of_rows", trueOrFalseSettingNoOracle},
    {"output_format_json_escape_forward_slashes", trueOrFalseSettingNoOracle},
    {"output_format_json_named_tuples_as_objects", trueOrFalseSettingNoOracle},
    {"output_format_json_pretty_print", trueOrFalseSettingNoOracle},
    {"output_format_json_quote_64bit_floats", trueOrFalseSettingNoOracle},
    {"output_format_json_quote_64bit_integers", trueOrFalseSettingNoOracle},
    {"output_format_json_quote_decimals", trueOrFalseSettingNoOracle},
    {"output_format_json_quote_denormals", trueOrFalseSettingNoOracle},
    {"output_format_json_skip_null_value_in_named_tuples", trueOrFalseSettingNoOracle},
    {"output_format_json_validate_utf8", trueOrFalseSettingNoOracle},
    {"output_format_markdown_escape_special_characters", trueOrFalseSettingNoOracle},
    {"output_format_native_use_flattened_dynamic_and_json_serialization", trueOrFalseSettingNoOracle},
    {"output_format_native_write_json_as_string", trueOrFalseSettingNoOracle},
    {"output_format_orc_compression_method",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'lz4'", "'snappy'", "'zlib'", "'zstd'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_orc_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_parallel_formatting", trueOrFalseSetting},
    {"output_format_parquet_bloom_filter_bits_per_value",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 100.0)); },
         {},
         false)},
    {"output_format_parquet_bloom_filter_flush_threshold_bytes",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"output_format_parquet_compliant_nested_types", trueOrFalseSettingNoOracle},
    {"output_format_parquet_compression_method",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'snappy'", "'lz4'", "'brotli'", "'zstd'", "'gzip'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_enum_as_byte_array", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_datetime_as_uint32", trueOrFalseSettingNoOracle},
    {"output_format_parquet_date_as_uint16", trueOrFalseSettingNoOracle},
    {"output_format_parquet_max_dictionary_size",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.3, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"output_format_parquet_fixed_string_as_fixed_byte_array", trueOrFalseSettingNoOracle},
    {"output_format_parquet_geometadata", trueOrFalseSettingNoOracle},
    {"output_format_parquet_parallel_encoding", trueOrFalseSettingNoOracle},
    {"output_format_parquet_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_parquet_use_custom_encoder", trueOrFalseSettingNoOracle},
    {"output_format_parquet_version",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'1.0'", "'2.4'", "'2.6'", "'2.latest'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_write_bloom_filter", trueOrFalseSettingNoOracle},
    {"output_format_parquet_write_page_index", trueOrFalseSettingNoOracle},
    {"output_format_pretty_color",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'0'", "'1'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_pretty_fallback_to_vertical", trueOrFalseSettingNoOracle},
    {"output_format_pretty_glue_chunks", trueOrFalseSettingNoOracle},
    {"output_format_pretty_grid_charset",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "'UTF-8'" : "'ASCII'"; }, {}, false)},
    {"output_format_pretty_highlight_digit_groups", trueOrFalseSettingNoOracle},
    {"output_format_pretty_multiline_fields", trueOrFalseSettingNoOracle},
    {"output_format_pretty_row_numbers", trueOrFalseSettingNoOracle},
    {"output_format_protobuf_nullables_with_google_wrappers", trueOrFalseSettingNoOracle},
    {"output_format_sql_insert_include_column_names", trueOrFalseSettingNoOracle},
    {"output_format_sql_insert_quote_names", trueOrFalseSettingNoOracle},
    {"output_format_sql_insert_use_replace", trueOrFalseSettingNoOracle},
    {"output_format_values_escape_quote_with_quote", trueOrFalseSettingNoOracle},
    {"output_format_write_statistics", trueOrFalseSettingNoOracle},
    {"page_cache_inject_eviction", trueOrFalseSetting},
    {"parallel_distributed_insert_select", CHSetting(zeroOneTwo, {}, false)},
    {"parallel_replicas_allow_in_with_subquery", trueOrFalseSetting},
    {"parallel_replicas_custom_key_range_lower", CHSetting(highRange, {}, false)},
    {"parallel_replicas_custom_key_range_upper", CHSetting(highRange, {}, false)},
    {"parallel_replicas_for_cluster_engines", trueOrFalseSetting},
    {"parallel_replicas_for_non_replicated_merge_tree", trueOrFalseSetting},
    {"parallel_replicas_index_analysis_only_on_coordinator", trueOrFalseSetting},
    {"parallel_replicas_insert_select_local_pipeline", trueOrFalseSettingNoOracle},
    {"parallel_replicas_local_plan", trueOrFalseSetting},
    {"parallel_replicas_mark_segment_size", CHSetting(highRange, {}, false)},
    {"parallel_replicas_min_number_of_rows_per_replica", CHSetting(highRange, {}, false)},
    {"parallel_replicas_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices
                 = {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"},
         false)},
    {"parallel_replicas_support_projection", trueOrFalseSetting},
    {"partial_result_on_first_cancel", trueOrFalseSettingNoOracle},
    {"parsedatetime_e_requires_space_padding", trueOrFalseSettingNoOracle},
    {"parsedatetime_parse_without_leading_zeros", trueOrFalseSettingNoOracle},
    {"per_part_index_stats", trueOrFalseSetting},
    {"precise_float_parsing", trueOrFalseSettingNoOracle},
    {"print_pretty_type_names", trueOrFalseSettingNoOracle},
    {"push_external_roles_in_interserver_queries", trueOrFalseSettingNoOracle},
    {"query_cache_compress_entries", trueOrFalseSetting},
    {"query_cache_share_between_users", trueOrFalseSettingNoOracle},
    {"query_cache_squash_partial_results", trueOrFalseSetting},
    {"query_condition_cache_store_conditions_as_plaintext", trueOrFalseSettingNoOracle},
    {"query_plan_display_internal_aliases", trueOrFalseSettingNoOracle},
    {"query_plan_max_limit_for_top_k_optimization",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); },
         {"0", "1", "2", "4", "16", "64"},
         false)},
    {"query_plan_max_step_description_length",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.2, 0.2, 0, 1000)); }, {}, false)},
    {"query_plan_text_index_add_hint", trueOrFalseSetting},
    {"query_plan_read_in_order_through_join", trueOrFalseSetting},
    {"query_plan_use_logical_join_step", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"read_from_distributed_cache_if_exists_otherwise_bypass_cache", trueOrFalseSetting},
    {"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", trueOrFalseSetting},
    {"read_from_page_cache_if_exists_otherwise_bypass_cache", trueOrFalseSetting},
    /// ClickHouse cloud setting
    {"read_through_distributed_cache", trueOrFalseSetting},
    {"regexp_dict_allow_hyperscan", trueOrFalseSetting},
    {"regexp_dict_flag_case_insensitive", trueOrFalseSettingNoOracle},
    {"regexp_dict_flag_dotall", trueOrFalseSettingNoOracle},
    {"reject_expensive_hyperscan_regexps", trueOrFalseSetting},
    {"remote_filesystem_read_prefetch", trueOrFalseSetting},
    {"replace_running_query", trueOrFalseSettingNoOracle},
    {"restore_replace_external_dictionary_source_to_null", trueOrFalseSettingNoOracle},
    {"restore_replace_external_engines_to_null", trueOrFalseSettingNoOracle},
    {"restore_replace_external_table_functions_to_null", trueOrFalseSettingNoOracle},
    {"restore_replicated_merge_tree_to_shared_merge_tree", trueOrFalseSettingNoOracle},
    {"rows_before_aggregation", trueOrFalseSetting},
    {"s3_allow_multipart_copy", trueOrFalseSetting},
    {"s3_allow_parallel_part_upload", trueOrFalseSettingNoOracle},
    {"s3_check_objects_after_upload", trueOrFalseSettingNoOracle},
    {"s3_create_new_file_on_insert", trueOrFalseSettingNoOracle},
    {"s3_disable_checksum", trueOrFalseSettingNoOracle},
    {"s3_ignore_file_doesnt_exist", trueOrFalseSettingNoOracle},
    {"s3_skip_empty_files", trueOrFalseSettingNoOracle},
    {"s3_slow_all_threads_after_network_error", trueOrFalseSettingNoOracle},
    {"s3_throw_on_zero_files_match", trueOrFalseSettingNoOracle},
    {"s3_truncate_on_insert", trueOrFalseSettingNoOracle},
    {"s3_use_adaptive_timeouts", trueOrFalseSettingNoOracle},
    {"s3_validate_request_settings", trueOrFalseSettingNoOracle},
    {"s3queue_enable_logging_to_s3queue_log", trueOrFalseSettingNoOracle},
    {"schema_inference_cache_require_modification_time_for_url", trueOrFalseSettingNoOracle},
    {"schema_inference_make_columns_nullable",
     CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"schema_inference_make_json_columns_nullable", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_file", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_s3", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_url", trueOrFalseSettingNoOracle},
    {"secondary_indices_enable_bulk_filtering", trueOrFalseSetting},
    {"select_sequential_consistency", trueOrFalseSetting},
    {"send_logs_level",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices
                 = {"'debug'", "'information'", "'trace'", "'error'", "'test'", "'warning'", "'fatal'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"send_progress_in_http_headers", trueOrFalseSettingNoOracle},
    {"send_profile_events", trueOrFalseSettingNoOracle},
    {"serialize_query_plan", trueOrFalseSetting},
    {"serialize_string_in_memory_with_zero_byte", trueOrFalseSettingNoOracle},
    {"shared_merge_tree_sync_parts_on_partition_operations", trueOrFalseSettingNoOracle},
    {"short_circuit_function_evaluation_for_nulls", trueOrFalseSetting},
    {"short_circuit_function_evaluation_for_nulls_threshold", probRangeSetting},
    {"show_data_lake_catalogs_in_system_tables", trueOrFalseSettingNoOracle},
    {"show_create_query_identifier_quoting_rule",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'when_necessary'", "'always'", "'user_display'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"show_create_query_identifier_quoting_style",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'Backticks'", "'DoubleQuotes'", "'BackticksMySQL'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"show_processlist_include_internal", trueOrFalseSettingNoOracle},
    {"show_table_uuid_in_table_create_query_if_not_nil", trueOrFalseSettingNoOracle},
    {"skip_download_if_exceeds_query_cache", trueOrFalseSetting},
    {"skip_redundant_aliases_in_udf", trueOrFalseSettingNoOracle},
    {"skip_unavailable_shards", trueOrFalseSettingNoOracle},
    {"splitby_max_substrings_includes_remaining_string", trueOrFalseSettingNoOracle},
    {"storage_file_read_method",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'read'", "'pread'", "'mmap'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"stream_like_engine_allow_direct_select", trueOrFalseSetting},
    {"system_events_show_zero_values", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"table_engine_read_through_distributed_cache", trueOrFalseSetting},
    {"table_function_remote_max_addresses",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.3, 0.2, 1, 100)); }, {}, false)},
    {"text_index_hint_max_selectivity", probRangeSetting},
    {"throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert", trueOrFalseSettingNoOracle},
    {"throw_if_no_data_to_insert", trueOrFalseSettingNoOracle},
    {"throw_on_error_from_cache_on_write_operations", trueOrFalseSettingNoOracle},
    {"throw_on_max_partitions_per_insert_block", trueOrFalseSettingNoOracle},
    {"throw_on_unsupported_query_inside_transaction", trueOrFalseSettingNoOracle},
    {"totals_auto_threshold", CHSetting(probRange, {}, false)},
    {"totals_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices
                 = {"'before_having'", "'after_having_exclusive'", "'after_having_inclusive'", "'after_having_auto'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"trace_profile_events", trueOrFalseSettingNoOracle},
    {"transform_null_in", trueOrFalseSettingNoOracle},
    {"traverse_shadow_remote_data_paths", trueOrFalseSettingNoOracle},
    {"type_json_skip_duplicated_paths", trueOrFalseSettingNoOracle},
    {"type_json_skip_invalid_typed_paths", trueOrFalseSettingNoOracle},
    {"union_default_mode", setSetting},
    {"update_insert_deduplication_token_in_dependent_materialized_views", trueOrFalseSettingNoOracle},
    {"update_parallel_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'sync'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"update_sequential_consistency", trueOrFalseSetting},
    {"use_async_executor_for_materialized_views", trueOrFalseSetting},
    {"use_cache_for_count_from_files", trueOrFalseSetting},
    {"use_client_time_zone", trueOrFalseSettingNoOracle},
    {"use_compact_format_in_distributed_parts_names", trueOrFalseSettingNoOracle},
    {"use_hedged_requests", trueOrFalseSetting},
    {"use_hive_partitioning", trueOrFalseSettingNoOracle},
    {"use_iceberg_metadata_files_cache", trueOrFalseSetting},
    {"use_legacy_to_time", trueOrFalseSettingNoOracle},
    {"use_page_cache_for_disks_without_file_cache", trueOrFalseSetting},
    {"use_query_cache", trueOrFalseSetting},
    {"use_roaring_bitmap_iceberg_positional_deletes", trueOrFalseSetting},
    {"use_skip_indexes_if_final_exact_mode", CHSetting(trueOrFalse, {"0", "1"}, true)},
    {"use_structure_from_insertion_table_in_table_functions", CHSetting(zeroOneTwo, {}, false)},
    {"use_text_index_dictionary_cache", trueOrFalseSetting},
    {"use_text_index_header_cache", trueOrFalseSetting},
    {"use_text_index_postings_cache", trueOrFalseSetting},
    {"use_variant_as_common_type", CHSetting(trueOrFalse, {"0", "1"}, true)},
    {"use_with_fill_by_sorting_prefix", trueOrFalseSetting},
    {"validate_enum_literals_in_operators", trueOrFalseSettingNoOracle},
    {"validate_experimental_and_suspicious_types_inside_nested_types", trueOrFalseSettingNoOracle},
    {"validate_mutation_query", trueOrFalseSettingNoOracle},
    {"validate_polygons", trueOrFalseSettingNoOracle},
    {"vector_search_filter_strategy",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'auto'", "'postfilter'", "'prefilter'"};
             return rg.pickRandomly(choices);
         },
         {"'auto'", "'postfilter'", "'prefilter'"},
         false)},
    {"vector_search_index_fetch_multiplier",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 4.0)); },
         {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1", "1.5", "2", "2.5"},
         false)},
    {"vector_search_with_rescoring", trueOrFalseSettingNoOracle},
    {"wait_changes_become_visible_after_commit_mode",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &)
         {
             static const DB::Strings & choices = {"'async'", "'wait'", "'wait_unknown'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"write_full_path_in_iceberg_metadata", trueOrFalseSettingNoOracle},
    /// ClickHouse cloud setting
    {"write_through_distributed_cache", trueOrFalseSettingNoOracle},
    {"zstd_window_log_max",
     CHSetting(
         [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.3, 0.2, -100, 100)); },
         {},
         false)}};

std::unordered_map<String, CHSetting> queryOracleSettings;

std::unordered_map<String, CHSetting> formatSettings;

void loadFuzzerServerSettings(const FuzzConfig & fc)
{
    if (!fc.clusters.empty())
    {
        serverSettings.insert(
            {{"cluster_for_parallel_replicas",
              CHSetting([&](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(fc.clusters) + "'"; }, {}, false)}});
    }
    if (!fc.caches.empty())
    {
        serverSettings.insert(
            {{"filesystem_cache_name",
              CHSetting([&](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(fc.caches) + "'"; }, {}, false)}});
    }
    for (const auto & setting : performanceSettings)
    {
        serverSettings.insert(setting);
    }
    for (auto & setting : serverSettings2)
    {
        serverSettings.emplace(std::move(setting));
    }
    if (fc.allow_transactions)
    {
        serverSettings.insert({{"implicit_transaction", trueOrFalseSettingNoOracle}});
    }

    DB::Strings max_bytes_values
        = {"aggregation_in_order_max_block_bytes",
           "async_insert_max_data_size",
           "azure_max_single_part_upload_size",
           "cross_join_min_bytes_to_compress",
           "default_max_bytes_in_join",
           "delta_lake_insert_max_bytes_in_data_file",
           /// ClickHouse cloud setting
           "distributed_cache_alignment",
           /// ClickHouse cloud setting
           "distributed_cache_min_bytes_for_seek",
           /// ClickHouse cloud setting
           "distributed_cache_read_alignment",
           "external_storage_max_read_bytes",
           "filesystem_cache_boundary_alignment",
           "filesystem_cache_max_download_size",
           "filesystem_prefetch_max_memory_usage",
           "filesystem_prefetch_min_bytes_for_single_read_task",
           "filesystem_prefetch_step_bytes",
           "group_by_two_level_threshold_bytes",
           "input_format_max_block_size_bytes",
           "input_format_parquet_local_file_min_bytes_for_seek",
           "input_format_parquet_prefer_block_bytes",
           "input_format_parquet_memory_low_watermark",
           "input_format_parquet_memory_high_watermark",
           "join_runtime_bloom_filter_bytes",
           "max_bytes_before_external_group_by",
           "max_bytes_before_external_sort",
           "max_bytes_before_remerge_sort",
           "max_download_buffer_size",
           "iceberg_insert_max_bytes_in_data_file",
           "max_joined_block_size_bytes",
           "max_read_buffer_size",
           "max_read_buffer_size_local_fs",
           "max_read_buffer_size_remote_fs",
           "max_table_size_to_drop",
           "max_temporary_data_on_disk_size_for_query",
           "max_temporary_data_on_disk_size_for_user",
           "max_untracked_memory",
           "max_reverse_dictionary_lookup_cache_size_bytes",
           "merge_tree_max_bytes_to_use_cache",
           "merge_tree_min_bytes_for_concurrent_read",
           "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem",
           "merge_tree_min_bytes_for_seek",
           "merge_tree_min_bytes_per_task_for_remote_reading",
           "min_bytes_to_use_direct_io",
           "min_bytes_to_use_mmap_io",
           "min_chunk_bytes_for_parallel_parsing",
           "min_external_table_block_size_bytes",
           "min_insert_block_size_bytes",
           "min_insert_block_size_bytes_for_materialized_views",
           "min_joined_block_size_bytes",
           "output_format_parquet_row_group_size_bytes",
           "page_cache_block_size",
           "partial_merge_join_left_table_buffer_bytes",
           "prefer_external_sort_block_bytes",
           "preferred_block_size_bytes",
           "preferred_max_column_in_block_size_bytes",
           "prefetch_buffer_size",
           "query_cache_max_size_in_bytes",
           "remote_read_min_bytes_for_seek",
           "temporary_files_buffer_size",
           /// ClickHouse cloud setting
           "write_through_distributed_cache_buffer_size"};
    DB::Strings max_rows_values
        = {"cluster_table_function_buckets_batch_size",
           "cross_join_min_rows_to_compress",
           "delta_lake_insert_max_rows_in_data_file",
           "distributed_plan_max_rows_to_broadcast",
           "external_storage_max_read_rows",
           "function_range_max_elements_in_block",
           "group_by_two_level_threshold",
           "hnsw_candidate_list_size_for_search",
           "join_output_by_rowlist_perkey_rows_threshold",
           "join_runtime_filter_exact_values_limit",
           "join_to_sort_maximum_table_rows",
           "join_to_sort_minimum_perkey_rows",
           "iceberg_insert_max_partitions",
           "iceberg_insert_max_rows_in_data_file",
           "max_joined_block_size_rows",
           "max_limit_for_vector_search_queries",
           "max_number_of_partitions_for_independent_aggregation",
           "max_projection_rows_to_use_projection_index",
           "max_rows_to_transfer",
           "merge_tree_max_rows_to_use_cache",
           "merge_tree_min_read_task_size",
           "merge_tree_min_rows_for_concurrent_read",
           "merge_tree_min_rows_for_concurrent_read_for_remote_filesystem",
           "merge_tree_min_rows_for_seek",
           "min_external_table_block_size_rows",
           "min_insert_block_size_rows",
           "min_insert_block_size_rows_for_materialized_views",
           "min_joined_block_size_rows",
           "min_outstreams_per_resize_after_split",
           "min_table_rows_to_use_projection_index",
           "output_format_parquet_batch_size",
           "output_format_parquet_data_page_size",
           "output_format_parquet_row_group_size",
           "output_format_pretty_max_rows",
           "page_cache_lookahead_blocks",
           "parallel_hash_join_threshold",
           "partial_merge_join_rows_in_right_blocks",
           "query_plan_max_limit_for_lazy_materialization"};
    DB::Strings max_block_sizes = {"input_format_parquet_max_block_size",
            "max_block_size",
            "max_compress_block_size",
            "max_insert_block_size",
            "min_compress_block_size"/*,
            "output_format_orc_compression_block_size" can give std::exception */};
    DB::Strings max_columns_values;

    if (!fc.allow_query_oracles)
    {
        serverSettings.insert(
            {{"apply_deleted_mask", trueOrFalseSettingNoOracle}, /// gives issue with dump table oracle
             {"deduplicate_blocks_in_dependent_materialized_views", trueOrFalseSettingNoOracle},
             {"describe_compact_output", trueOrFalseSettingNoOracle},
             {"empty_result_for_aggregation_by_empty_set", trueOrFalseSettingNoOracle}, /// the oracle doesn't get output
             {"external_table_functions_use_nulls", trueOrFalseSettingNoOracle},
             {"external_table_strict_query", trueOrFalseSettingNoOracle},
             {"ignore_data_skipping_indices", trueOrFalseSettingNoOracle},
             {"lightweight_deletes_sync", CHSetting(zeroOneTwo, {}, false)},
             {"optimize_using_constraints", trueOrFalseSettingNoOracle},
             {"parallel_replica_offset",
              CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.nextSmallNumber() - 1); }, {}, false)},
             {"parallel_replicas_count",
              CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.nextSmallNumber() - 1); }, {}, false)},
             {"remote_filesystem_read_method",
              CHSetting([](RandomGenerator & rg, FuzzConfig &) { return rg.nextBool() ? "'read'" : "'threadpool'"; }, {}, false)},
             {"wait_for_async_insert", trueOrFalseSettingNoOracle}});
        max_bytes_values.insert(
            max_bytes_values.end(),
            {"max_bytes_in_distinct",
             "max_bytes_in_join",
             "max_bytes_in_set",
             "max_bytes_to_read",
             "max_bytes_to_read_leaf",
             "max_bytes_to_sort",
             "max_bytes_to_transfer",
             "max_result_bytes"});
        max_rows_values.insert(
            max_rows_values.end(),
            {"limit",
             "max_result_rows",
             "max_rows_in_distinct",
             "max_rows_in_join",
             "max_rows_in_set",
             "max_rows_to_group_by",
             "max_rows_to_read",
             "max_rows_to_read_leaf",
             "max_rows_to_sort"});
        /*max_columns_values.insert( too many errors on queries
            max_columns_values.end(), {"max_columns_to_read", "max_temporary_columns", "max_temporary_non_const_columns"});*/
    }

    /// When measuring performance use bigger block sizes
    for (const auto & entry : max_bytes_values)
    {
        performanceSettings.insert({{entry, CHSetting(bytesRange, {"32768", "65536", "1048576", "4194304", "33554432", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(bytesRange, {"0", "4", "8", "32", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    for (const auto & entry : max_rows_values)
    {
        performanceSettings.insert({{entry, CHSetting(rowsRange, {"0", "512", "1024", "2048", "4096", "16384", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(rowsRange, {"0", "4", "8", "32", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    for (const auto & entry : max_block_sizes)
    {
        performanceSettings.insert({{entry, CHSetting(highRange, {"1024", "2048", "4096", "8192", "16384", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(highRange, {"4", "8", "32", "64", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    for (const auto & entry : max_columns_values)
    {
        chassert(!fc.allow_query_oracles);
        serverSettings.insert({{entry, CHSetting(columnsRange, {}, false)}});
    }

    if (!fc.timezones.empty())
    {
        const auto timeZones = [&](RandomGenerator & rg, FuzzConfig &) { return "'" + rg.pickRandomly(fc.timezones) + "'"; };

        serverSettings.insert({{"session_timezone", CHSetting(timeZones, {}, false)}});
        formatSettings.insert(
            {{"input_format_orc_reader_time_zone_name", CHSetting(timeZones, {}, false)},
             {"output_format_orc_writer_time_zone_name", CHSetting(timeZones, {}, false)}});
    }
    if (fc.enable_fault_injection_settings)
    {
        serverSettings.insert(
            {{"backup_restore_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
             {"create_replicated_merge_tree_fault_injection_probability", CHSetting(probRange, {}, false)},
             {"enforce_strict_identifier_format", trueOrFalseSettingNoOracle},
             {"input_format_values_interpret_expressions", trueOrFalseSettingNoOracle},
             {"insert_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
             {"memory_tracker_fault_probability", CHSetting(probRange, {}, false)},
             {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability", CHSetting(probRange, {}, false)},
             {"min_free_disk_bytes_to_perform_insert", CHSetting(bytesRange, {}, false)},
             {"min_free_disk_ratio_to_perform_insert", CHSetting(probRange, {}, false)},
             {"min_free_disk_space_for_temporary_data", CHSetting(bytesRange, {}, false)},
             {"postgresql_fault_injection_probability", CHSetting(probRange, {}, false)},
             {"s3queue_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
             {"unknown_packet_in_send_data",
              CHSetting(
                  [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<uint64_t>(0.1, 0.1, 0, 16384)); },
                  {},
                  false)}});
    }
    if (fc.enable_force_settings)
    {
        serverSettings.insert(
            {{"force_aggregation_in_order", trueOrFalseSettingNoOracle},
             {"force_data_skipping_indices", trueOrFalseSettingNoOracle},
             {"force_index_by_date", trueOrFalseSettingNoOracle},
             {"force_optimize_projection", trueOrFalseSettingNoOracle},
             {"force_primary_key", trueOrFalseSettingNoOracle}});
    }
    if (fc.enable_overflow_settings)
    {
        static const auto & overflowSetting = CHSetting(
            [](RandomGenerator & rg, FuzzConfig &)
            {
                static const DB::Strings & choices = {"'throw'", "'break'", "'any'"};
                return rg.pickRandomly(choices);
            },
            {},
            false);

        serverSettings.insert(
            {{"distinct_overflow_mode", overflowSetting},
             {"group_by_overflow_mode", overflowSetting},
             {"join_overflow_mode", overflowSetting},
             {"read_overflow_mode", overflowSetting},
             {"read_overflow_mode_leaf", overflowSetting},
             {"result_overflow_mode", overflowSetting},
             {"set_overflow_mode", overflowSetting},
             {"sort_overflow_mode", overflowSetting},
             {"timeout_overflow_mode", overflowSetting},
             {"timeout_overflow_mode_leaf", overflowSetting},
             {"transfer_overflow_mode", overflowSetting}});
    }

    /// Set hot settings
    for (const auto & entry : fc.hot_settings)
    {
        if (!serverSettings.contains(entry))
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Unknown server setting: {}", entry);
        }
        const auto & next = serverSettings.at(entry);
        if (next.oracle_values.empty())
        {
            throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Server setting {} can't be set as hot", entry);
        }
        hotSettings.insert({entry, next});
    }
    for (const auto & [key, value] : serverSettings)
    {
        if (!value.oracle_values.empty())
        {
            queryOracleSettings.insert({{key, value}});
        }
    }

    /// Format settings are to be used by the dump oracle when not looking for correctness
    for (const auto & setting : serverSettings)
    {
        formatSettings.insert(setting);
    }
    formatSettings.insert(
        {{"bool_false_representation", CHSetting(nastyStrings, {}, false)},
         {"bool_true_representation", CHSetting(nastyStrings, {}, false)},
         {"format_binary_max_array_size", CHSetting(rowsRange, {}, false)},
         {"format_binary_max_string_size", CHSetting(rowsRange, {}, false)},
         {"format_capn_proto_enum_comparising_mode",
          CHSetting(
              [](RandomGenerator & rg, FuzzConfig &)
              {
                  static const DB::Strings & choices = {"'by_names'", "'by_names_case_insensitive'", "'by_values'"};
                  return rg.pickRandomly(choices);
              },
              {},
              false)},
         {"format_csv_allow_double_quotes", trueOrFalseSettingNoOracle},
         {"format_csv_allow_single_quotes", trueOrFalseSettingNoOracle},
         {"format_csv_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_csv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"format_custom_escaping_rule",
          CHSetting(
              [](RandomGenerator & rg, FuzzConfig &)
              {
                  static const DB::Strings & choices = {"'None'", "'Escaped'", "'Quoted'", "'CSV'", "'JSON'", "'XML'", "'Raw'"};
                  return rg.pickRandomly(choices);
              },
              {},
              false)},
         {"format_custom_field_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_custom_row_before_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_custom_row_after_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_custom_row_between_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_custom_result_before_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_custom_result_after_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_tsv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"input_format_binary_decode_types_in_binary_format", trueOrFalseSettingNoOracle},
         {"input_format_csv_arrays_as_nested_csv", trueOrFalseSettingNoOracle},
         {"input_format_csv_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_csv_enum_as_number", trueOrFalseSettingNoOracle},
         {"input_format_custom_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_json_empty_as_default", trueOrFalseSettingNoOracle},
         {"input_format_json_infer_array_of_dynamic_from_array_of_different_types", trueOrFalseSettingNoOracle},
         {"input_format_json_map_as_array_of_tuples", trueOrFalseSettingNoOracle},
         {"input_format_json_max_depth", CHSetting(rowsRange, {}, false)},
         {"input_format_max_rows_to_read_for_schema_inference", CHSetting(rowsRange, {}, false)},
         {"input_format_max_bytes_to_read_for_schema_inference", CHSetting(bytesRange, {}, false)},
         {"input_format_msgpack_number_of_columns",
          CHSetting([](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.randomInt<uint32_t>(0, 20)); }, {}, false)},
         /// {"input_format_native_decode_types_in_binary_format", trueOrFalseSettingNoOracle}, may block the client
         {"input_format_orc_row_batch_size", CHSetting(rowsRange, {}, false)},
         {"input_format_tsv_crlf_end_of_line", trueOrFalseSettingNoOracle},
         {"input_format_tsv_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_with_names_use_header", trueOrFalseSettingNoOracle},
         {"input_format_with_types_use_header", trueOrFalseSettingNoOracle},
         {"low_cardinality_allow_in_native_format", trueOrFalseSettingNoOracle},
         {"output_format_avro_rows_in_file", CHSetting(rowsRange, {}, false)},
         {"output_format_orc_dictionary_key_size_threshold",
          CHSetting(
              [](RandomGenerator & rg, FuzzConfig &) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 1.0)); },
              {},
              false)},
         {"output_format_binary_encode_types_in_binary_format", trueOrFalseSettingNoOracle},
         {"output_format_csv_crlf_end_of_line", trueOrFalseSettingNoOracle},
         {"output_format_json_map_as_array_of_tuples", trueOrFalseSettingNoOracle},
         {"output_format_msgpack_uuid_representation",
          CHSetting(
              [](RandomGenerator & rg, FuzzConfig &)
              {
                  static const DB::Strings & choices = {"'ext'", "'str'", "'bin'"};
                  return rg.pickRandomly(choices);
              },
              {},
              false)},
         /// {"output_format_native_encode_types_in_binary_format", trueOrFalseSettingNoOracle}, may block the client
         {"output_format_orc_row_index_stride", CHSetting(rowsRange, {}, false)},
         {"output_format_parquet_write_checksums", trueOrFalseSettingNoOracle},
         {"output_format_tsv_crlf_end_of_line", trueOrFalseSettingNoOracle}});

    /// Remove disallowed settings
    for (const auto & entry : fc.disallowed_settings)
    {
        hotSettings.erase(entry);
        serverSettings.erase(entry);
        performanceSettings.erase(entry);
        queryOracleSettings.erase(entry);
        formatSettings.erase(entry);
    }
    if (serverSettings.empty() || performanceSettings.empty() || queryOracleSettings.empty() || formatSettings.empty())
    {
        throw DB::Exception(DB::ErrorCodes::BUZZHOUSE, "Settings map can't be empty");
    }
}

std::unique_ptr<SQLType> size_tp, null_tp, string_tp;

std::vector<SystemTable> systemTables;

void loadSystemTables(FuzzConfig & fc)
{
    size_tp = std::make_unique<IntType>(64, true);
    null_tp = std::make_unique<BoolType>();
    string_tp = std::make_unique<StringType>(1);

    fc.loadSystemTables(systemTables);
}

}
