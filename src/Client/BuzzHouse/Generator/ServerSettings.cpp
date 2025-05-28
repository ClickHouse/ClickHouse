#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

const auto nastyStrings = [](RandomGenerator & rg) { return "'" + rg.pickRandomly(rg.nasty_strings) + "'"; };

std::unordered_map<String, CHSetting> performanceSettings
    = {{"allow_aggregate_partitions_independently", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_experimental_query_deduplication", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_hyperscan", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_prefetched_read_pool_for_local_filesystem", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_prefetched_read_pool_for_remote_filesystem", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_push_predicate_when_subquery_contains_with", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_reorder_prewhere_conditions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_simdjson", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"allow_statistics_optimize", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"compile_aggregate_expressions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"compile_expressions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"compile_sort_description", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"count_distinct_implementation",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"};
                return rg.pickRandomly(choices);
            },
            {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"},
            false)},
       {"count_distinct_optimization", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"enable_adaptive_memory_spill_scheduler", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"enable_optimize_predicate_expression", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"enable_optimize_predicate_expression_to_final_subquery", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"join_any_take_last_row", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"join_algorithm",
        CHSetting(
            [](RandomGenerator & rg)
            {
                String res;
                DB::Strings choices
                    = {"auto",
                       "default",
                       "direct",
                       "full_sorting_merge",
                       "grace_hash",
                       "hash",
                       "parallel_hash",
                       "partial_merge",
                       "prefer_partial_merge"};

                if (rg.nextBool())
                {
                    res = rg.pickRandomly(choices);
                }
                else
                {
                    const uint32_t nalgo = (rg.nextMediumNumber() % static_cast<uint32_t>(choices.size())) + 1;

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
       {"low_cardinality_use_single_dictionary_for_part", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"max_bytes_ratio_before_external_group_by", CHSetting(probRange, {"0", "0.1", "0.5", "0.99"}, false)},
       {"max_bytes_ratio_before_external_sort", CHSetting(probRange, {"0", "0.1", "0.5", "0.99"}, false)},
       {"max_streams_to_max_threads_ratio", CHSetting(probRange, {"0", "0.1", "0.5", "0.99"}, false)},
       {"merge_tree_determine_task_size_by_prewhere_columns", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"min_count_to_compile_aggregate_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"min_count_to_compile_expression", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"min_count_to_compile_sort_description", CHSetting(zeroToThree, {"0", "1", "2", "3"}, false)},
       {"move_all_conditions_to_prewhere", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"move_primary_key_columns_to_end_of_prewhere", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_aggregation_in_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_aggregators_of_group_by_keys", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_append_index", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_arithmetic_operations_in_aggregate_functions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_distinct_in_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_distributed_group_by_sharding_key", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_functions_to_subcolumns", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_group_by_constant_keys", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_group_by_function_keys", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_if_chain_to_multiif", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_if_transform_strings_to_enum", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_injective_functions_in_group_by", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_injective_functions_inside_uniq", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_move_to_prewhere", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_move_to_prewhere_if_final", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_multiif_to_if", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_normalize_count_variants", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_read_in_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_read_in_window_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_redundant_functions_in_order_by", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_respect_aliases", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_rewrite_aggregate_function_with_if", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_rewrite_array_exists_to_has", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_rewrite_sum_if_to_count_if", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_skip_merged_partitions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_skip_unused_shards", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_skip_unused_shards_nesting", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
       {"optimize_skip_unused_shards_rewrite_in", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_sorting_by_input_stream_properties", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_substitute_columns", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_syntax_fuse_functions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_trivial_approximate_count_query", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_trivial_count_query", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_uniq_to_count", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_use_implicit_projections", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"optimize_use_projections", CHSetting(trueOrFalse, {"0", "1"}, false)},
       /// {"optimize_using_constraints", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"os_thread_priority",
        CHSetting(
            [](RandomGenerator & rg) { return std::to_string(rg.randomInt<int32_t>(-20, 19)); }, {"-20", "-10", "0", "10", "19"}, false)},
       {"parallel_replicas_only_with_analyzer", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"parallel_replicas_prefer_local_join", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"parallel_view_processing", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"parallelize_output_from_storages", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"partial_merge_join_optimizations", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"prefer_global_in_and_join", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"prefer_localhost_replica", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_aggregation_in_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_convert_join_to_in", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_convert_outer_join_to_inner_join", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_enable_multithreading_after_window_functions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_enable_optimizations", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_execute_functions_after_sorting", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_filter_push_down", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_join_shard_by_pk_ranges", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_lift_up_array_join", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_lift_up_union", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_merge_expressions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_merge_filters", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_optimize_lazy_materialization", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_optimize_prewhere", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_push_down_limit", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_read_in_order", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_remove_redundant_distinct", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_remove_redundant_sorting", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_reuse_storage_ordering_for_window_functions", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_split_filter", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_join_swap_table",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'false'", "'true'", "'auto'"};
                return rg.pickRandomly(choices);
            },
            {"'false'", "'true'", "'auto'"},
            false)},
       {"read_in_order_two_level_merge_threshold",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 100)); }, {"0", "1", "10", "100"}, false)},
       {"read_in_order_use_buffering", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"read_in_order_use_virtual_row", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"remerge_sort_lowered_memory_bytes_ratio",
        CHSetting(
            [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.7, 0.0, 4.0)); },
            {"0", "0.1", "0.5", "1", "1.5", "2", "2.5"},
            false)},
       {"rewrite_count_distinct_if_with_count_distinct_implementation", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"short_circuit_function_evaluation",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'enable'", "'force_enable'", "'disable'"};
                return rg.pickRandomly(choices);
            },
            {"'enable'", "'force_enable'", "'disable'"},
            false)},
       {"single_join_prefer_left_table", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"split_intersecting_parts_ranges_into_layers_final", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"split_parts_ranges_into_intersecting_and_non_intersecting_final", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"temporary_files_codec",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'lz4'", "'none'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"transform_null_in", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_concurrency_control", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_index_for_in_with_subqueries", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_page_cache_with_distributed_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_query_condition_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_skip_indexes", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_skip_indexes_if_final", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_uncompressed_cache", CHSetting(trueOrFalse, {"0", "1"}, false)}};

std::unordered_map<String, CHSetting> serverSettings = {
    {"aggregate_functions_null_for_empty", CHSetting(trueOrFalse, {}, false)},
    {"aggregation_memory_efficient_merge_threads", threadSetting},
    {"allow_asynchronous_read_from_io_pool_for_merge_tree", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"allow_changing_replica_until_first_data_packet", CHSetting(trueOrFalse, {}, false)},
    {"allow_introspection_functions", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"alter_move_to_space_execute_async", CHSetting(trueOrFalse, {}, false)},
    {"alter_partition_verbose_result", CHSetting(trueOrFalse, {}, false)},
    {"alter_sync", CHSetting(zeroOneTwo, {}, false)},
    {"analyze_index_with_space_filling_curves", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"analyzer_compatibility_join_using_top_level_identifier", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"apply_deleted_mask", CHSetting(trueOrFalse, {}, false)},
    {"apply_mutations_on_fly", CHSetting(trueOrFalse, {}, false)},
    {"any_join_distinct_right_table_keys", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"asterisk_include_alias_columns", CHSetting(trueOrFalse, {}, false)},
    {"async_insert", CHSetting(trueOrFalse, {}, false)},
    {"async_insert_deduplicate", CHSetting(trueOrFalse, {}, false)},
    {"async_insert_threads", threadSetting},
    {"async_insert_use_adaptive_busy_timeout", CHSetting(trueOrFalse, {}, false)},
    {"async_query_sending_for_remote", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"async_socket_for_remote", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"backup_restore_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"cache_warmer_threads", threadSetting},
    {"calculate_text_stack_trace", CHSetting(trueOrFalse, {}, false)},
    {"cancel_http_readonly_queries_on_client_close", CHSetting(trueOrFalse, {}, false)},
    {"cast_ipv4_ipv6_default_on_conversion_error", CHSetting(trueOrFalse, {}, false)},
    {"cast_keep_nullable", CHSetting(trueOrFalse, {}, false)},
    {"cast_string_to_dynamic_use_inference", CHSetting(trueOrFalse, {}, false)},
    {"check_query_single_value_result", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"check_referential_table_dependencies", CHSetting(trueOrFalse, {}, false)},
    {"check_table_dependencies", CHSetting(trueOrFalse, {}, false)},
    {"checksum_on_read", CHSetting(trueOrFalse, {}, false)},
    {"cloud_mode", CHSetting(zeroOneTwo, {}, false)},
    {"cloud_mode_database_engine", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "1" : "2"; }, {}, false)},
    {"cloud_mode_engine", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"collect_hash_table_stats_during_aggregation", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"collect_hash_table_stats_during_joins", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"compatibility_ignore_auto_increment_in_create_table", CHSetting(trueOrFalse, {}, false)},
    {"compatibility_ignore_collation_in_create_table", CHSetting(trueOrFalse, {}, false)},
    {"convert_query_to_cnf", CHSetting(trueOrFalse, {}, false)},
    {"create_replicated_merge_tree_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"create_table_empty_primary_key_by_default", CHSetting(trueOrFalse, {}, false)},
    {"cross_to_inner_join_rewrite", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
    {"database_atomic_wait_for_drop_and_detach_synchronously", CHSetting(trueOrFalse, {}, false)},
    {"database_replicated_allow_explicit_uuid", CHSetting(zeroOneTwo, {}, false)},
    {"database_replicated_allow_heavy_create", CHSetting(trueOrFalse, {}, false)},
    {"database_replicated_allow_replicated_engine_arguments",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"database_replicated_always_detach_permanently", CHSetting(trueOrFalse, {}, false)},
    {"database_replicated_enforce_synchronous_settings", CHSetting(trueOrFalse, {}, false)},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", CHSetting(trueOrFalse, {}, false)},
    {"date_time_output_format",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"decimal_check_overflow", CHSetting(trueOrFalse, {}, false)},
    /// {"deduplicate_blocks_in_dependent_materialized_views", CHSetting(trueOrFalse, {}, false)},
    /// {"describe_compact_output", CHSetting(trueOrFalse, {}, false)},
    {"describe_extend_object_types", CHSetting(trueOrFalse, {}, false)},
    {"describe_include_subcolumns", CHSetting(trueOrFalse, {}, false)},
    {"describe_include_virtual_columns", CHSetting(trueOrFalse, {}, false)},
    {"dictionary_use_async_executor", CHSetting(trueOrFalse, {}, false)},
    {"dictionary_validate_primary_key_type", CHSetting(trueOrFalse, {}, false)},
    {"distributed_aggregation_memory_efficient", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"distributed_background_insert_batch", CHSetting(trueOrFalse, {}, false)},
    {"distributed_background_insert_split_batch_on_failure", CHSetting(trueOrFalse, {}, false)},
    {"distributed_cache_bypass_connection_pool", CHSetting(trueOrFalse, {}, false)},
    {"distributed_cache_discard_connection_if_unread_data", CHSetting(trueOrFalse, {}, false)},
    {"distributed_cache_fetch_metrics_only_from_current_az", CHSetting(trueOrFalse, {}, false)},
    {"distributed_cache_throw_on_error", CHSetting(trueOrFalse, {}, false)},
    {"distributed_foreground_insert", CHSetting(trueOrFalse, {}, false)},
    {"distributed_group_by_no_merge", CHSetting(zeroOneTwo, {}, false)},
    {"distributed_insert_skip_read_only_replicas", CHSetting(trueOrFalse, {}, false)},
    {"distributed_product_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'deny'", "'local'", "'global'", "'allow'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"distributed_push_down_limit", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"do_not_merge_across_partitions_select_final", CHSetting(trueOrFalse, {}, false)},
    {"empty_result_for_aggregation_by_constant_keys_on_empty_set", CHSetting(trueOrFalse, {}, false)},
    {"enable_analyzer", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_blob_storage_log", CHSetting(trueOrFalse, {}, false)},
    {"enable_early_constant_folding", CHSetting(trueOrFalse, {}, false)},
    {"enable_extended_results_for_datetime_functions", CHSetting(trueOrFalse, {}, false)},
    {"enable_filesystem_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_filesystem_cache_log", CHSetting(trueOrFalse, {}, false)},
    {"enable_filesystem_cache_on_write_operations", CHSetting(trueOrFalse, {}, false)},
    {"enable_filesystem_read_prefetches_log", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_global_with_statement", CHSetting(trueOrFalse, {}, false)},
    {"enable_http_compression", CHSetting(trueOrFalse, {}, false)},
    {"enable_job_stack_trace", CHSetting(trueOrFalse, {}, false)},
    {"enable_memory_bound_merging_of_aggregation_results", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_multiple_prewhere_read_steps", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_named_columns_in_function_tuple", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_parsing_to_custom_serialization", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_reads_from_query_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_s3_requests_logging", CHSetting(trueOrFalse, {}, false)},
    {"enable_scalar_subquery_optimization", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_sharing_sets_for_mutations", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_software_prefetch_in_aggregation", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_unaligned_array_join", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_url_encoding", CHSetting(trueOrFalse, {}, false)},
    {"enable_vertical_final", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"enable_writes_to_query_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"engine_file_allow_create_multiple_files", CHSetting(trueOrFalse, {}, false)},
    {"engine_file_empty_if_not_exists", CHSetting(trueOrFalse, {}, false)},
    {"engine_file_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
    {"engine_url_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
    {"exact_rows_before_limit", CHSetting(trueOrFalse, {"0", "1"}, false)},
    /// {"external_table_functions_use_nulls", CHSetting(trueOrFalse, {}, false)},
    /// {"external_table_strict_query", CHSetting(trueOrFalse, {}, true)},
    {"extremes", CHSetting(trueOrFalse, {}, false)},
    {"fallback_to_stale_replicas_for_distributed_queries", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"filesystem_cache_enable_background_download_during_fetch", CHSetting(trueOrFalse, {}, false)},
    {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", CHSetting(trueOrFalse, {}, false)},
    {"filesystem_cache_name",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'cache_for_s3'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"filesystem_cache_prefer_bigger_buffer_size", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", CHSetting(trueOrFalse, {}, false)},
    {"filesystem_cache_segments_batch_size",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const std::vector<uint32_t> choices{0, 3, 10, 50};
             return std::to_string(rg.pickRandomly(choices));
         },
         {},
         false)},
    {"filesystem_prefetch_step_marks", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "0" : "50"; }, {}, false)},
    {"filesystem_prefetches_limit", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "0" : "10"; }, {}, false)},
    {"final", CHSetting(trueOrFalse, {}, false)},
    {"flatten_nested", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"force_aggregate_partitions_independently", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"force_grouping_standard_compatibility", CHSetting(trueOrFalse, {}, false)},
    {"force_optimize_skip_unused_shards", CHSetting(zeroOneTwo, {}, false)},
    {"force_optimize_skip_unused_shards_nesting", CHSetting(zeroOneTwo, {}, false)},
    /// {"force_index_by_date", CHSetting(trueOrFalse, {}, false)},
    /// {"force_optimize_projection", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"force_remove_data_recursively_on_drop", CHSetting(trueOrFalse, {}, false)},
    {"format_capn_proto_use_autogenerated_schema", CHSetting(trueOrFalse, {}, false)},
    {"format_display_secrets_in_show_and_select", CHSetting(trueOrFalse, {}, false)},
    {"format_protobuf_use_autogenerated_schema", CHSetting(trueOrFalse, {}, false)},
    {"format_regexp_skip_unmatched", CHSetting(trueOrFalse, {}, false)},
    {"fsync_metadata", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"function_json_value_return_type_allow_complex", CHSetting(trueOrFalse, {}, false)},
    {"function_json_value_return_type_allow_nullable", CHSetting(trueOrFalse, {}, false)},
    {"function_locate_has_mysql_compatible_argument_order", CHSetting(trueOrFalse, {}, false)},
    {"geo_distance_returns_float64_on_float64_arguments", CHSetting(trueOrFalse, {}, false)},
    {"grace_hash_join_initial_buckets",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    {"grace_hash_join_max_buckets",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    /// {"group_by_overflow_mode", CHSetting([](RandomGenerator & rg) { const DB::Strings & choices = {"'throw'", "'break'", "'any'"}; return rg.pickRandomly(choices); }, {}, false)},
    {"group_by_use_nulls", CHSetting(trueOrFalse, {}, false)},
    {"hdfs_create_new_file_on_insert", CHSetting(trueOrFalse, {}, false)},
    {"hdfs_ignore_file_doesnt_exist", CHSetting(trueOrFalse, {}, false)},
    {"hdfs_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
    {"hdfs_throw_on_zero_files_match", CHSetting(trueOrFalse, {}, false)},
    {"http_make_head_request", CHSetting(trueOrFalse, {}, false)},
    {"http_native_compression_disable_checksumming_on_decompress", CHSetting(trueOrFalse, {}, false)},
    {"http_response_buffer_size", CHSetting(highRange, {}, false)},
    {"http_skip_not_found_url_for_globs", CHSetting(trueOrFalse, {}, false)},
    {"http_wait_end_of_query", CHSetting(trueOrFalse, {}, false)},
    {"http_write_exception_in_output_format", CHSetting(trueOrFalse, {}, false)},
    {"ignore_materialized_views_with_dropped_target_table", CHSetting(trueOrFalse, {}, false)},
    {"ignore_on_cluster_for_replicated_access_entities_queries", CHSetting(trueOrFalse, {}, false)},
    {"ignore_on_cluster_for_replicated_named_collections_queries", CHSetting(trueOrFalse, {}, false)},
    {"ignore_on_cluster_for_replicated_udf_queries", CHSetting(trueOrFalse, {}, false)},
    {"implicit_transaction", CHSetting(trueOrFalse, {}, false)},
    {"input_format_allow_errors_num", CHSetting(highRange, {}, false)},
    {"input_format_allow_errors_ratio", CHSetting(probRange, {}, false)},
    {"input_format_allow_seeks", CHSetting(trueOrFalse, {}, false)},
    {"input_format_arrow_allow_missing_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_arrow_case_insensitive_column_matching", CHSetting(trueOrFalse, {}, false)},
    {"input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_avro_allow_missing_fields", CHSetting(trueOrFalse, {}, false)},
    {"input_format_avro_null_as_default", CHSetting(trueOrFalse, {}, false)},
    {"input_format_binary_read_json_as_string", CHSetting(trueOrFalse, {}, false)},
    {"input_format_bson_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_allow_cr_end_of_line", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_allow_variable_number_of_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_allow_whitespace_or_tab_as_delimiter", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_deserialize_separate_columns_into_tuple", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_empty_as_default", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_enum_as_number", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_skip_trailing_empty_lines", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_trim_whitespaces", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_try_infer_numbers_from_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_try_infer_strings_from_quoted_tuples", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_use_best_effort_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_csv_use_default_on_bad_values", CHSetting(trueOrFalse, {}, false)},
    {"input_format_custom_allow_variable_number_of_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_custom_skip_trailing_empty_lines", CHSetting(trueOrFalse, {}, false)},
    {"input_format_defaults_for_omitted_fields", CHSetting(trueOrFalse, {}, false)},
    {"input_format_force_null_for_omitted_fields", CHSetting(trueOrFalse, {}, false)},
    {"input_format_hive_text_allow_variable_number_of_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_import_nested_json", CHSetting(trueOrFalse, {}, false)},
    {"input_format_ipv4_default_on_conversion_error", CHSetting(trueOrFalse, {}, false)},
    {"input_format_ipv6_default_on_conversion_error", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_compact_allow_variable_number_of_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_defaults_for_missing_elements_in_named_tuple", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_ignore_unknown_keys_in_named_tuple", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_ignore_unnecessary_fields", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_infer_incomplete_types_as_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_named_tuples_as_objects", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_read_arrays_as_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_read_bools_as_numbers", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_read_bools_as_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_read_numbers_as_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_read_objects_as_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_throw_on_bad_escape_sequence", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_try_infer_named_tuples_from_objects", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_try_infer_numbers_from_strings", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects", CHSetting(trueOrFalse, {}, false)},
    {"input_format_json_validate_types_from_metadata", CHSetting(trueOrFalse, {}, false)},
    {"input_format_mysql_dump_map_column_names", CHSetting(trueOrFalse, {}, false)},
    {"input_format_native_allow_types_conversion", CHSetting(trueOrFalse, {}, false)},
    {"input_format_null_as_default", CHSetting(trueOrFalse, {}, false)},
    {"input_format_orc_allow_missing_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_orc_case_insensitive_column_matching", CHSetting(trueOrFalse, {}, false)},
    {"input_format_orc_dictionary_as_low_cardinality", CHSetting(trueOrFalse, {}, false)},
    {"input_format_orc_filter_push_down", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"input_format_orc_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_orc_use_fast_decoder", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parallel_parsing", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"input_format_parquet_allow_missing_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parquet_bloom_filter_push_down", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"input_format_parquet_case_insensitive_column_matching", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parquet_enable_row_group_prefetch", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parquet_filter_push_down", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"input_format_parquet_preserve_order", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_parquet_use_native_reader", CHSetting(trueOrFalse, {}, false)},
    {"input_format_protobuf_flatten_google_wrappers", CHSetting(trueOrFalse, {}, false)},
    {"input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_skip_unknown_fields", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_dates", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_datetimes", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_datetimes_only_datetime64", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_exponent_floats", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_integers", CHSetting(trueOrFalse, {}, false)},
    {"input_format_try_infer_variants", CHSetting(trueOrFalse, {}, false)},
    {"input_format_tsv_allow_variable_number_of_columns", CHSetting(trueOrFalse, {}, false)},
    {"input_format_tsv_empty_as_default", CHSetting(trueOrFalse, {}, false)},
    {"input_format_tsv_enum_as_number", CHSetting(trueOrFalse, {}, false)},
    {"input_format_tsv_skip_trailing_empty_lines", CHSetting(trueOrFalse, {}, false)},
    {"input_format_tsv_use_best_effort_in_schema_inference", CHSetting(trueOrFalse, {}, false)},
    {"input_format_values_accurate_types_of_literals", CHSetting(trueOrFalse, {}, false)},
    {"input_format_values_deduce_templates_of_expressions", CHSetting(trueOrFalse, {}, false)},
    {"input_format_values_interpret_expressions", CHSetting(trueOrFalse, {}, false)},
    {"insert_deduplicate", CHSetting(trueOrFalse, {}, false)},
    {"insert_distributed_one_random_shard", CHSetting(trueOrFalse, {}, false)},
    {"insert_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"insert_null_as_default", CHSetting(trueOrFalse, {}, false)},
    {"insert_quorum", CHSetting(zeroOneTwo, {}, false)},
    {"insert_quorum_parallel", CHSetting(trueOrFalse, {}, false)},
    {"interval_output_format", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'kusto'" : "'numeric'"; }, {}, false)},
    /// {"join_overflow_mode", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'throw'" : "'break'"; }, {}, false)},
    {"join_use_nulls", CHSetting(trueOrFalse, {}, false)},
    {"keeper_map_strict_mode", CHSetting(trueOrFalse, {}, false)},
    {"legacy_column_name_of_tuple_literal", CHSetting(trueOrFalse, {}, false)},
    /// {"lightweight_deletes_sync", CHSetting(zeroOneTwo, {}, false)}, FINAL queries don't cover these
    {"load_balancing",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {
                 "'round_robin'", "'in_order'", "'hostname_levenshtein_distance'", "'nearest_hostname'", "'first_or_random'", "'random'"};
             return rg.pickRandomly(choices);
         },
         {"'round_robin'", "'in_order'", "'hostname_levenshtein_distance'", "'nearest_hostname'", "'first_or_random'", "'random'"},
         false)},
    {"load_marks_asynchronously", CHSetting(trueOrFalse, {}, false)},
    {"local_filesystem_read_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"};
             return rg.pickRandomly(choices);
         },
         {"'read'", "'pread'", "'mmap'", "'pread_threadpool'", "'io_uring'"},
         false)}};

static std::unordered_map<String, CHSetting> serverSettings2 = {
    {"local_filesystem_read_prefetch", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"log_formatted_queries", CHSetting(trueOrFalse, {}, false)},
    {"log_processors_profiles", CHSetting(trueOrFalse, {}, false)},
    {"log_profile_events", CHSetting(trueOrFalse, {}, false)},
    {"log_queries", CHSetting(trueOrFalse, {}, false)},
    {"log_query_settings", CHSetting(trueOrFalse, {}, false)},
    {"log_query_threads", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"log_query_views", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"low_cardinality_max_dictionary_size", CHSetting(highRange, {}, false)},
    {"materialize_skip_indexes_on_insert", CHSetting(trueOrFalse, {}, false)},
    {"materialize_statistics_on_insert", CHSetting(trueOrFalse, {}, false)},
    {"materialize_ttl_after_modify", CHSetting(trueOrFalse, {}, false)},
    {"materialized_views_ignore_errors", CHSetting(trueOrFalse, {}, false)},
    /// {"max_bytes_in_distinct", CHSetting(bytesRange, {}, false)},
    /// {"max_bytes_in_join", CHSetting(bytesRange, {"0", "1", "1000", "1000000"}, false)},
    /// {"max_bytes_in_set", CHSetting(bytesRange, {}, false)},
    /// {"max_bytes_to_read", CHSetting(bytesRange, {}, false)},
    /// {"max_bytes_to_read_leaf", CHSetting(bytesRange, {}, false)},
    /// {"max_bytes_to_sort", CHSetting(bytesRange, {}, false)},
    /// {"max_bytes_to_transfer", CHSetting(bytesRange, {}, false)},
    /// {"max_columns_to_read", CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {}, false)},
    {"max_download_threads", threadSetting},
    {"max_final_threads", threadSetting},
    {"max_insert_delayed_streams_for_parallel_write",
     CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 12)); }, {}, false)},
    {"max_insert_threads", threadSetting},
    /// {"max_memory_usage", CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }, {}, false)},
    /// {"max_memory_usage_for_user", CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << ((rg.nextLargeNumber() % 8) + 15)); }, {}, false)},
    {"max_parallel_replicas",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.7, 0, 5)); }, {}, false)},
    {"max_parsing_threads", threadSetting},
    {"max_parts_to_move",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.5, 0, UINT32_C(4096))); },
         {"0", "1", "100", "1000"},
         false)},
    /// {"max_result_bytes", CHSetting(bytesRange, {}, false)},
    /// {"max_result_rows", CHSetting(highRange, {}, false)},
    /// {"max_rows_in_distinct", CHSetting(highRange, {}, false)},
    /// {"max_rows_in_join", CHSetting(highRange, {"0", "8", "32", "64", "1024", "10000"}, false)},
    /// {"max_rows_in_set", CHSetting(highRange, {}, false)},
    /// {"max_rows_to_group_by", CHSetting(highRange, {}, false)},
    /// {"max_rows_to_read", CHSetting(highRange, {}, false)},
    /// {"max_rows_to_read_leaf", CHSetting(highRange, {}, false)},
    /// {"max_rows_to_sort", CHSetting(highRange, {}, false)},
    /// {"max_temporary_columns", CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {}, false)},
    /// {"max_temporary_non_const_columns", CHSetting([](RandomGenerator & rg) { return std::to_string(UINT32_C(1) << (rg.nextLargeNumber() % 6)); }, {}, false)},
    {"max_threads", threadSetting},
    {"max_threads_for_indexes", threadSetting},
    {"memory_tracker_fault_probability", CHSetting(probRange, {}, false)},
    {"merge_tree_coarse_index_granularity",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(2, 32)); }, {}, false)},
    {"merge_tree_compact_parts_min_granules_to_multibuffer_read",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(1, 128)); }, {}, false)},
    {"merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability", CHSetting(probRange, {}, false)},
    {"merge_tree_use_const_size_tasks_for_remote_reading", CHSetting(trueOrFalse, {}, false)},
    {"merge_tree_use_v1_object_and_dynamic_serialization", CHSetting(trueOrFalse, {}, false)},
    {"metrics_perf_events_enabled", CHSetting(trueOrFalse, {}, false)},
    {"min_free_disk_ratio_to_perform_insert", CHSetting(probRange, {}, false)},
    {"min_hit_rate_to_use_consecutive_keys_optimization", CHSetting(probRange, {"0", "0.1", "0.5", "0.9", "1.0"}, false)},
    {"mongodb_throw_on_unsupported_query", CHSetting(trueOrFalse, {}, false)},
    {"multiple_joins_try_to_keep_original_names", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"mutations_execute_nondeterministic_on_initiator", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"mutations_execute_subqueries_on_initiator", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"mutations_sync", CHSetting(zeroOneTwo, {}, false)},
    {"mysql_map_fixed_string_to_text_in_show_columns", CHSetting(trueOrFalse, {}, false)},
    {"mysql_map_string_to_text_in_show_columns", CHSetting(trueOrFalse, {}, false)},
    {"optimize_count_from_files", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"optimize_extract_common_expressions", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"optimize_on_insert", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"optimize_or_like_chain", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"optimize_throw_if_noop", CHSetting(trueOrFalse, {}, false)},
    {"optimize_time_filter_with_preimage", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"optimize_trivial_insert_select", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"output_format_arrow_compression_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'lz4_frame'", "'zstd'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_arrow_fixed_string_as_fixed_byte_array", CHSetting(trueOrFalse, {}, false)},
    {"output_format_arrow_low_cardinality_as_dictionary", CHSetting(trueOrFalse, {}, false)},
    {"output_format_arrow_string_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_arrow_use_64_bit_indexes_for_dictionary", CHSetting(trueOrFalse, {}, false)},
    {"output_format_arrow_use_signed_indexes_for_dictionary", CHSetting(trueOrFalse, {}, false)},
    {"output_format_binary_write_json_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_bson_string_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_csv_serialize_tuple_into_separate_columns", CHSetting(trueOrFalse, {}, false)},
    {"output_format_decimal_trailing_zeros", CHSetting(trueOrFalse, {}, false)},
    {"output_format_enable_streaming", CHSetting(trueOrFalse, {}, false)},
    {"output_format_avro_codec",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'null'", "'deflate'", "'snappy'", "'zstd'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_json_array_of_rows", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_escape_forward_slashes", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_named_tuples_as_objects", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_pretty_print", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_quote_64bit_floats", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_quote_64bit_integers", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_quote_decimals", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_quote_denormals", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_skip_null_value_in_named_tuples", CHSetting(trueOrFalse, {}, false)},
    {"output_format_json_validate_utf8", CHSetting(trueOrFalse, {}, false)},
    {"output_format_markdown_escape_special_characters", CHSetting(trueOrFalse, {}, false)},
    {"output_format_native_write_json_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_orc_compression_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'lz4'", "'snappy'", "'zlib'", "'zstd'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_orc_string_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parallel_formatting", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"output_format_parquet_bloom_filter_bits_per_value",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.3, 0.7, 0.0, 100.0)); }, {}, false)},
    {"output_format_parquet_bloom_filter_flush_threshold_bytes",
     CHSetting(
         [](RandomGenerator & rg)
         { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.5, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"output_format_parquet_compliant_nested_types", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_compression_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'snappy'", "'lz4'", "'brotli'", "'zstd'", "'gzip'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_datetime_as_uint32", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_fixed_string_as_fixed_byte_array", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_parallel_encoding", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_string_as_string", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_use_custom_encoder", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_version",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'1.0'", "'2.4'", "'2.6'", "'2.latest'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_write_bloom_filter", CHSetting(trueOrFalse, {}, false)},
    {"output_format_parquet_write_page_index", CHSetting(trueOrFalse, {}, false)},
    {"output_format_pretty_color",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'0'", "'1'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_pretty_fallback_to_vertical", CHSetting(trueOrFalse, {}, false)},
    {"output_format_pretty_grid_charset", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'UTF-8'" : "'ASCII'"; }, {}, false)},
    {"output_format_pretty_highlight_digit_groups", CHSetting(trueOrFalse, {}, false)},
    {"output_format_pretty_multiline_fields", CHSetting(trueOrFalse, {}, false)},
    {"output_format_pretty_row_numbers", CHSetting(trueOrFalse, {}, false)},
    {"output_format_protobuf_nullables_with_google_wrappers", CHSetting(trueOrFalse, {}, false)},
    {"output_format_sql_insert_include_column_names", CHSetting(trueOrFalse, {}, false)},
    {"output_format_sql_insert_quote_names", CHSetting(trueOrFalse, {}, false)},
    {"output_format_sql_insert_use_replace", CHSetting(trueOrFalse, {}, false)},
    {"output_format_values_escape_quote_with_quote", CHSetting(trueOrFalse, {}, false)},
    {"output_format_write_statistics", CHSetting(trueOrFalse, {}, false)},
    {"page_cache_inject_eviction", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"parallel_distributed_insert_select", CHSetting(zeroOneTwo, {}, false)},
    /// {"parallel_replica_offset", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
    {"parallel_replicas_allow_in_with_subquery", CHSetting(trueOrFalse, {"0", "1"}, false)},
    /// {"parallel_replicas_count", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
    {"parallel_replicas_for_cluster_engines", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"parallel_replicas_for_non_replicated_merge_tree", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"parallel_replicas_index_analysis_only_on_coordinator", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"parallel_replicas_custom_key_range_lower", CHSetting(highRange, {}, false)},
    {"parallel_replicas_custom_key_range_upper", CHSetting(highRange, {}, false)},
    {"parallel_replicas_local_plan", CHSetting(trueOrFalse, {"0", "1"}, false)},
    {"parallel_replicas_mark_segment_size", CHSetting(highRange, {}, false)},
    {"parallel_replicas_min_number_of_rows_per_replica", CHSetting(highRange, {}, false)},
    {"parallel_replicas_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"},
         false)}};

/// We need to split the serverSettings because in order to initialize the values for the map it
/// needs to be able to fit into the stack. Note we may have to split it even more in the future.
static std::unordered_map<String, CHSetting> serverSettings3
    = {{"partial_result_on_first_cancel", CHSetting(trueOrFalse, {}, false)},
       {"postgresql_fault_injection_probability", CHSetting(probRange, {}, false)},
       {"precise_float_parsing", CHSetting(trueOrFalse, {}, false)},
       {"print_pretty_type_names", CHSetting(trueOrFalse, {}, false)},
       {"push_external_roles_in_interserver_queries", CHSetting(trueOrFalse, {}, false)},
       {"query_cache_compress_entries", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_cache_share_between_users", CHSetting(trueOrFalse, {}, false)},
       {"query_cache_squash_partial_results", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"query_plan_use_new_logical_join_step", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"read_from_page_cache_if_exists_otherwise_bypass_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"read_through_distributed_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"regexp_dict_allow_hyperscan", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"regexp_dict_flag_case_insensitive", CHSetting(trueOrFalse, {}, false)},
       {"regexp_dict_flag_dotall", CHSetting(trueOrFalse, {}, false)},
       {"remote_filesystem_read_method",
        CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'read'" : "'threadpool'"; }, {"'read'", "'threadpool'"}, false)},
       {"reject_expensive_hyperscan_regexps", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"remote_filesystem_read_prefetch", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"replace_running_query", CHSetting(trueOrFalse, {}, false)},
       {"rows_before_aggregation", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"s3_allow_parallel_part_upload", CHSetting(trueOrFalse, {}, false)},
       {"s3_check_objects_after_upload", CHSetting(trueOrFalse, {}, false)},
       {"s3_create_new_file_on_insert", CHSetting(trueOrFalse, {}, false)},
       {"s3_disable_checksum", CHSetting(trueOrFalse, {}, false)},
       {"s3_ignore_file_doesnt_exist", CHSetting(trueOrFalse, {}, false)},
       {"s3_skip_empty_files", CHSetting(trueOrFalse, {}, false)},
       {"s3_throw_on_zero_files_match", CHSetting(trueOrFalse, {}, false)},
       {"s3_truncate_on_insert", CHSetting(trueOrFalse, {}, false)},
       {"s3_use_adaptive_timeouts", CHSetting(trueOrFalse, {}, false)},
       {"s3_validate_request_settings", CHSetting(trueOrFalse, {}, false)},
       {"s3queue_enable_logging_to_s3queue_log", CHSetting(trueOrFalse, {}, false)},
       {"schema_inference_cache_require_modification_time_for_url", CHSetting(trueOrFalse, {}, false)},
       {"schema_inference_make_json_columns_nullable", CHSetting(trueOrFalse, {}, false)},
       {"schema_inference_use_cache_for_file", CHSetting(trueOrFalse, {}, false)},
       {"schema_inference_use_cache_for_s3", CHSetting(trueOrFalse, {}, false)},
       {"schema_inference_use_cache_for_url", CHSetting(trueOrFalse, {}, false)},
       {"select_sequential_consistency", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"send_logs_level",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices
                    = {"'debug'", "'information'", "'trace'", "'error'", "'test'", "'warning'", "'fatal'", "'none'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"send_progress_in_http_headers", CHSetting(trueOrFalse, {}, false)},
       {"show_table_uuid_in_table_create_query_if_not_nil", CHSetting(trueOrFalse, {}, false)},
       {"skip_unavailable_shards", CHSetting(trueOrFalse, {}, false)},
       /// {"set_overflow_mode", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'break'" : "'throw'"; }, {}, false)},
       {"splitby_max_substrings_includes_remaining_string", CHSetting(trueOrFalse, {}, false)},
       {"storage_file_read_method",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'read'", "'pread'", "'mmap'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"stream_like_engine_allow_direct_select", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"system_events_show_zero_values", CHSetting(trueOrFalse, {}, false)},
       {"throw_on_error_from_cache_on_write_operations", CHSetting(trueOrFalse, {}, false)},
       {"throw_on_unsupported_query_inside_transaction", CHSetting(trueOrFalse, {}, false)},
       {"totals_auto_threshold", CHSetting(probRange, {}, false)},
       {"totals_mode",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices
                    = {"'before_having'", "'after_having_exclusive'", "'after_having_inclusive'", "'after_having_auto'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"trace_profile_events", CHSetting(trueOrFalse, {}, false)},
       {"traverse_shadow_remote_data_paths", CHSetting(trueOrFalse, {}, false)},
       {"type_json_skip_duplicated_paths", CHSetting(trueOrFalse, {}, false)},
       {"update_insert_deduplication_token_in_dependent_materialized_views", CHSetting(trueOrFalse, {}, false)},
       {"use_async_executor_for_materialized_views", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_cache_for_count_from_files", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_client_time_zone", CHSetting(trueOrFalse, {}, false)},
       {"use_compact_format_in_distributed_parts_names", CHSetting(trueOrFalse, {}, false)},
       {"use_hedged_requests", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"use_hive_partitioning", CHSetting(trueOrFalse, {}, false)},
       {"use_page_cache_for_disks_without_file_cache", CHSetting(trueOrFalse, {"0", "1"}, false)},
       /*{"use_query_cache",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices
                    = {"1, set_overflow_mode = 'throw', group_by_overflow_mode = 'throw', join_overflow_mode = 'throw'",
                       "0, set_overflow_mode = 'break', group_by_overflow_mode = 'break', join_overflow_mode = 'break'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},*/
       {"use_structure_from_insertion_table_in_table_functions", CHSetting(zeroOneTwo, {}, false)},
       {"use_variant_as_common_type", CHSetting(trueOrFalse, {"0", "1"}, true)},
       {"use_with_fill_by_sorting_prefix", CHSetting(trueOrFalse, {"0", "1"}, false)},
       {"validate_experimental_and_suspicious_types_inside_nested_types", CHSetting(trueOrFalse, {}, false)},
       {"validate_mutation_query", CHSetting(trueOrFalse, {}, false)},
       {"validate_polygons", CHSetting(trueOrFalse, {}, false)},
       {"wait_changes_become_visible_after_commit_mode",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'async'", "'wait'", "'wait_unknown'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       /// {"wait_for_async_insert", CHSetting(trueOrFalse, {}, false)},
       {"write_through_distributed_cache", CHSetting(trueOrFalse, {}, false)}};

std::unordered_map<String, CHSetting> queryOracleSettings;

std::unordered_map<String, CHSetting> formatSettings;

void loadFuzzerServerSettings(const FuzzConfig & fc)
{
    if (!fc.clusters.empty())
    {
        performanceSettings.insert({{"enable_parallel_replicas", CHSetting(trueOrFalse, {"0", "1"}, false)}});
    }
    for (auto & setting : performanceSettings)
    {
        serverSettings.insert(setting);
    }
    for (auto & setting : serverSettings2)
    {
        serverSettings.emplace(std::move(setting));
    }
    for (auto & setting : serverSettings3)
    {
        serverSettings.emplace(std::move(setting));
    }

    /// When measuring performance use bigger block sizes
    /// Number of rows values
    for (const auto & entry :
         {"cross_join_min_rows_to_compress",
          "group_by_two_level_threshold",
          "hnsw_candidate_list_size_for_search",
          "join_to_sort_maximum_table_rows",
          "join_to_sort_minimum_perkey_rows",
          "max_joined_block_size_rows",
          "max_number_of_partitions_for_independent_aggregation",
          "max_rows_to_transfer",
          "min_insert_block_size_rows",
          "min_insert_block_size_rows_for_materialized_views",
          "output_format_parquet_batch_size",
          "output_format_parquet_data_page_size",
          "output_format_parquet_row_group_size",
          "partial_merge_join_rows_in_right_blocks",
          "query_plan_max_limit_for_lazy_materialization"})
    {
        performanceSettings.insert({{entry, CHSetting(rowsRange, {"0", "512", "1024", "2048", "4096", "16384", "65536"}, false)}});
        serverSettings.insert({{entry, CHSetting(rowsRange, {"0", "4", "8", "32", "1024", "4096", "10000", "50000000"}, false)}});
    }
    /// Number of bytes values
    for (const auto & entry :
         {"aggregation_in_order_max_block_bytes",
          "cross_join_min_bytes_to_compress",
          "filesystem_prefetch_max_memory_usage",
          "filesystem_prefetch_min_bytes_for_single_read_task",
          "filesystem_prefetch_step_bytes",
          "group_by_two_level_threshold_bytes",
          "input_format_parquet_local_file_min_bytes_for_seek",
          "input_format_parquet_prefer_block_bytes",
          "max_bytes_before_external_group_by",
          "max_bytes_before_external_sort",
          "max_bytes_before_remerge_sort",
          "max_read_buffer_size",
          "max_read_buffer_size_local_fs",
          "max_read_buffer_size_remote_fs",
          "merge_tree_min_bytes_for_concurrent_read",
          "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem",
          "merge_tree_min_bytes_for_seek",
          "merge_tree_min_bytes_per_task_for_remote_reading",
          "min_bytes_to_use_direct_io",
          "min_bytes_to_use_mmap_io",
          "min_chunk_bytes_for_parallel_parsing",
          "min_external_sort_block_bytes",
          "min_external_table_block_size_bytes",
          "min_insert_block_size_bytes",
          "min_insert_block_size_bytes_for_materialized_views",
          "output_format_avro_sync_interval",
          "output_format_parquet_row_group_size_bytes",
          "partial_merge_join_left_table_buffer_bytes",
          "prefer_external_sort_block_bytes",
          "preferred_block_size_bytes",
          "preferred_max_column_in_block_size_bytes",
          "prefetch_buffer_size"})
    {
        performanceSettings.insert(
            {{entry, CHSetting(bytesRange, {"32768", "65536", "1048576", "4194304", "33554432", "50000000"}, false)}});
        serverSettings.insert({{entry, CHSetting(bytesRange, {"0", "4", "8", "32", "1024", "4096", "10000", "50000000"}, false)}});
    }
    /// Block size settings
    for (const auto & entry :
         {"input_format_parquet_max_block_size", "max_block_size", "max_compress_block_size", "min_compress_block_size"})
    {
        performanceSettings.insert({{entry, CHSetting(highRange, {"1024", "2048", "4096", "8192", "16384", "131072"}, false)}});
        serverSettings.insert({{entry, CHSetting(highRange, {"4", "8", "32", "64", "1024", "4096", "1000000"}, false)}});
    }
    if (!fc.timezones.empty())
    {
        serverSettings.insert(
            {{"session_timezone", CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(fc.timezones) + "'"; }, {}, false)}});
    }
    if (!fc.clusters.empty())
    {
        serverSettings.insert(
            {{"allow_experimental_parallel_reading_from_replicas", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
             {"cluster_for_parallel_replicas",
              CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(fc.clusters) + "'"; }, {}, false)}});
    }

    for (auto & [key, value] : serverSettings)
    {
        if (!value.oracle_values.empty())
        {
            queryOracleSettings.insert({{key, value}});
        }
    }

    /// Format settings are to be used by the dump oracle when not looking for correctness
    for (auto & [key, value] : serverSettings)
    {
        formatSettings.insert({{key, value}});
    }
    formatSettings.insert(
        {{"format_csv_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_csv_allow_single_quotes", CHSetting(trueOrFalse, {}, false)},
         {"format_csv_allow_double_quotes", CHSetting(trueOrFalse, {}, false)},
         {"format_csv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"format_tsv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"input_format_binary_decode_types_in_binary_format", CHSetting(trueOrFalse, {}, false)},
         {"input_format_csv_arrays_as_nested_csv", CHSetting(trueOrFalse, {}, false)},
         {"input_format_csv_detect_header", CHSetting(trueOrFalse, {}, false)},
         {"input_format_custom_detect_header", CHSetting(trueOrFalse, {}, false)},
         {"input_format_json_empty_as_default", CHSetting(trueOrFalse, {}, false)},
         {"input_format_msgpack_number_of_columns",
          CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<int32_t>(0, 20)); }, {}, false)},
         /// {"input_format_native_decode_types_in_binary_format", CHSetting(trueOrFalse, {}, false)}, may block the client
         {"input_format_tsv_crlf_end_of_line", CHSetting(trueOrFalse, {}, false)},
         {"input_format_tsv_detect_header", CHSetting(trueOrFalse, {}, false)},
         {"input_format_with_names_use_header", CHSetting(trueOrFalse, {}, false)},
         {"input_format_with_types_use_header", CHSetting(trueOrFalse, {}, false)},
         {"low_cardinality_allow_in_native_format", CHSetting(trueOrFalse, {}, false)},
         {"output_format_binary_encode_types_in_binary_format", CHSetting(trueOrFalse, {}, false)},
         {"output_format_csv_crlf_end_of_line", CHSetting(trueOrFalse, {}, false)},
         {"output_format_msgpack_uuid_representation", CHSetting(nastyStrings, {}, false)},
         /// {"output_format_native_encode_types_in_binary_format", CHSetting(trueOrFalse, {}, false)}, may block the client
         {"output_format_tsv_crlf_end_of_line", CHSetting(trueOrFalse, {}, false)}});
}

std::unique_ptr<SQLType> size_tp, null_tp;

std::unordered_map<String, DB::Strings> systemTables;

void loadSystemTables(const FuzzConfig & fc)
{
    size_tp = std::make_unique<IntType>(64, true);
    null_tp = std::make_unique<BoolType>();

    fc.loadSystemTables(systemTables);
}

}
