#include <Client/BuzzHouse/Generator/RandomSettings.h>

namespace BuzzHouse
{

static const auto nastyStrings = [](RandomGenerator & rg) { return "'" + rg.pickRandomly(rg.nasty_strings) + "'"; };

static const auto setSetting = CHSetting(
    [](RandomGenerator & rg)
    {
        const DB::Strings & choices = {"''", "'ALL'", "'DISTINCT'"};
        return rg.pickRandomly(choices);
    },
    {},
    false);

std::unordered_map<String, CHSetting> hotSettings
    = {{"enable_analyzer", trueOrFalseSetting},
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
       {"query_plan_optimize_lazy_materialization", trueOrFalseSetting}};

std::unordered_map<String, CHSetting> performanceSettings
    = {{"allow_aggregate_partitions_independently", trueOrFalseSetting},
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
       {"compile_aggregate_expressions", trueOrFalseSetting},
       {"compile_expressions", trueOrFalseSetting},
       {"compile_sort_description", trueOrFalseSetting},
       {"count_distinct_implementation",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"};
                return rg.pickRandomly(choices);
            },
            {"'uniq'", "'uniqCombined'", "'uniqCombined64'", "'uniqHLL12'", "'uniqExact'"},
            false)},
       {"count_distinct_optimization", trueOrFalseSetting},
       {"enable_adaptive_memory_spill_scheduler", trueOrFalseSetting},
       {"enable_optimize_predicate_expression", trueOrFalseSetting},
       {"enable_optimize_predicate_expression_to_final_subquery", trueOrFalseSetting},
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
       {"optimize_distinct_in_order", trueOrFalseSetting},
       {"optimize_distributed_group_by_sharding_key", trueOrFalseSetting},
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
       {"optimize_read_in_order", trueOrFalseSetting},
       {"optimize_read_in_window_order", trueOrFalseSetting},
       {"optimize_redundant_functions_in_order_by", trueOrFalseSetting},
       {"optimize_respect_aliases", trueOrFalseSetting},
       {"optimize_rewrite_aggregate_function_with_if", trueOrFalseSetting},
       {"optimize_rewrite_array_exists_to_has", trueOrFalseSetting},
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
       /// {"optimize_using_constraints", trueOrFalseSetting},
       {"os_thread_priority",
        CHSetting(
            [](RandomGenerator & rg) { return std::to_string(rg.randomInt<int32_t>(-20, 19)); }, {"-20", "-10", "0", "10", "19"}, false)},
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
       {"query_plan_enable_multithreading_after_window_functions", trueOrFalseSetting},
       {"query_plan_enable_optimizations", trueOrFalseSetting},
       {"query_plan_execute_functions_after_sorting", trueOrFalseSetting},
       {"query_plan_filter_push_down", trueOrFalseSetting},
       {"query_plan_join_shard_by_pk_ranges", trueOrFalseSetting},
       {"query_plan_join_swap_table",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'false'", "'true'", "'auto'"};
                return rg.pickRandomly(choices);
            },
            {"'false'", "'true'", "'auto'"},
            false)},
       {"query_plan_lift_up_array_join", trueOrFalseSetting},
       {"query_plan_lift_up_union", trueOrFalseSetting},
       {"query_plan_merge_expressions", trueOrFalseSetting},
       {"query_plan_merge_filter_into_join_condition", trueOrFalseSetting},
       {"query_plan_merge_filters", trueOrFalseSetting},
       {"query_plan_optimize_prewhere", trueOrFalseSetting},
       {"query_plan_push_down_limit", trueOrFalseSetting},
       {"query_plan_read_in_order", trueOrFalseSetting},
       {"query_plan_remove_redundant_distinct", trueOrFalseSetting},
       {"query_plan_remove_redundant_sorting", trueOrFalseSetting},
       {"query_plan_reuse_storage_ordering_for_window_functions", trueOrFalseSetting},
       {"query_plan_split_filter", trueOrFalseSetting},
       {"query_plan_try_use_vector_search", trueOrFalseSetting},
       {"read_in_order_two_level_merge_threshold",
        CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 100)); }, {"0", "1", "10", "100"}, false)},
       {"read_in_order_use_buffering", trueOrFalseSetting},
       {"read_in_order_use_virtual_row", trueOrFalseSetting},
       {"remerge_sort_lowered_memory_bytes_ratio",
        CHSetting(
            [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 4.0)); },
            {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1", "1.5", "2", "2.5"},
            false)},
       {"rewrite_count_distinct_if_with_count_distinct_implementation", trueOrFalseSetting},
       {"short_circuit_function_evaluation",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'enable'", "'force_enable'", "'disable'"};
                return rg.pickRandomly(choices);
            },
            {"'enable'", "'force_enable'", "'disable'"},
            false)},
       {"single_join_prefer_left_table", trueOrFalseSetting},
       {"split_intersecting_parts_ranges_into_layers_final", trueOrFalseSetting},
       {"split_parts_ranges_into_intersecting_and_non_intersecting_final", trueOrFalseSetting},
       {"temporary_files_codec",
        CHSetting(
            [](RandomGenerator & rg)
            {
                const DB::Strings & choices = {"'lz4'", "'none'"};
                return rg.pickRandomly(choices);
            },
            {},
            false)},
       {"transform_null_in", trueOrFalseSetting},
       {"use_concurrency_control", trueOrFalseSetting},
       {"use_iceberg_partition_pruning", trueOrFalseSetting},
       {"use_index_for_in_with_subqueries", trueOrFalseSetting},
       {"use_index_for_in_with_subqueries_max_values", trueOrFalseSetting},
       {"use_page_cache_with_distributed_cache", trueOrFalseSetting},
       {"use_query_condition_cache", trueOrFalseSetting},
       {"use_skip_indexes", trueOrFalseSetting},
       {"use_skip_indexes_if_final", trueOrFalseSetting},
       {"use_uncompressed_cache", trueOrFalseSetting}};

std::unordered_map<String, CHSetting> serverSettings = {
    {"aggregate_functions_null_for_empty", trueOrFalseSettingNoOracle},
    {"aggregation_memory_efficient_merge_threads", threadSetting},
    {"allow_asynchronous_read_from_io_pool_for_merge_tree", trueOrFalseSetting},
    {"allow_changing_replica_until_first_data_packet", trueOrFalseSettingNoOracle},
    {"allow_introspection_functions", trueOrFalseSetting},
    {"allow_special_bool_values_inside_variant", trueOrFalseSettingNoOracle},
    {"alter_move_to_space_execute_async", trueOrFalseSettingNoOracle},
    {"alter_partition_verbose_result", trueOrFalseSettingNoOracle},
    {"alter_sync", CHSetting(zeroOneTwo, {}, false)},
    {"analyze_index_with_space_filling_curves", trueOrFalseSetting},
    {"analyzer_compatibility_join_using_top_level_identifier", trueOrFalseSetting},
    /// {"apply_deleted_mask", trueOrFalseSettingNoOracle}, gives issue with dump table oracle
    {"apply_mutations_on_fly", trueOrFalseSettingNoOracle},
    {"apply_patch_parts", trueOrFalseSetting},
    {"any_join_distinct_right_table_keys", trueOrFalseSetting},
    {"asterisk_include_alias_columns", trueOrFalseSettingNoOracle},
    {"async_insert", trueOrFalseSettingNoOracle},
    {"async_insert_deduplicate", trueOrFalseSettingNoOracle},
    {"async_insert_threads", threadSetting},
    {"async_insert_use_adaptive_busy_timeout", trueOrFalseSettingNoOracle},
    {"async_query_sending_for_remote", trueOrFalseSetting},
    {"async_socket_for_remote", trueOrFalseSetting},
    {"backup_restore_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"cache_warmer_threads", threadSetting},
    {"calculate_text_stack_trace", trueOrFalseSettingNoOracle},
    {"cancel_http_readonly_queries_on_client_close", trueOrFalseSettingNoOracle},
    {"cast_ipv4_ipv6_default_on_conversion_error", trueOrFalseSettingNoOracle},
    {"cast_keep_nullable", trueOrFalseSettingNoOracle},
    {"cast_string_to_date_time_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'best_effort'", "'best_effort_us'", "'basic'"};
             return rg.pickRandomly(choices);
         },
         {"'best_effort'", "'best_effort_us'", "'basic'"},
         false)},
    {"cast_string_to_dynamic_use_inference", trueOrFalseSettingNoOracle},
    {"check_query_single_value_result", trueOrFalseSetting},
    {"check_referential_table_dependencies", trueOrFalseSettingNoOracle},
    {"check_table_dependencies", trueOrFalseSettingNoOracle},
    {"checksum_on_read", trueOrFalseSettingNoOracle},
    {"cloud_mode", trueOrFalseSettingNoOracle},
    {"cloud_mode_database_engine", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "1" : "2"; }, {}, false)},
    {"cloud_mode_engine", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"collect_hash_table_stats_during_aggregation", trueOrFalseSetting},
    {"collect_hash_table_stats_during_joins", trueOrFalseSetting},
    {"compatibility_ignore_auto_increment_in_create_table", trueOrFalseSettingNoOracle},
    {"compatibility_ignore_collation_in_create_table", trueOrFalseSettingNoOracle},
    {"convert_query_to_cnf", trueOrFalseSettingNoOracle},
    {"create_replicated_merge_tree_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"create_table_empty_primary_key_by_default", trueOrFalseSettingNoOracle},
    {"cross_to_inner_join_rewrite", CHSetting(zeroOneTwo, {"0", "1", "2"}, false)},
    {"database_atomic_wait_for_drop_and_detach_synchronously", trueOrFalseSettingNoOracle},
    {"database_replicated_allow_explicit_uuid", CHSetting(zeroOneTwo, {}, false)},
    {"database_replicated_allow_heavy_create", trueOrFalseSettingNoOracle},
    {"database_replicated_allow_replicated_engine_arguments",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<uint32_t>(0, 3)); }, {}, false)},
    {"database_replicated_always_detach_permanently", trueOrFalseSettingNoOracle},
    {"database_replicated_enforce_synchronous_settings", trueOrFalseSettingNoOracle},
    {"date_time_64_output_format_cut_trailing_zeros_align_to_groups_of_thousands", trueOrFalseSettingNoOracle},
    {"date_time_output_format",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'simple', date_time_input_format = 'basic'", "'iso', date_time_input_format = 'best_effort'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"decimal_check_overflow", trueOrFalseSettingNoOracle},
    /// {"deduplicate_blocks_in_dependent_materialized_views", trueOrFalseSettingNoOracle},
    /// {"describe_compact_output", trueOrFalseSettingNoOracle},
    {"distributed_plan_default_reader_bucket_count",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
    {"distributed_plan_default_shuffle_join_bucket_count",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 128)); }, {}, false)},
    {"describe_extend_object_types", trueOrFalseSettingNoOracle},
    {"describe_include_subcolumns", trueOrFalseSettingNoOracle},
    {"describe_include_virtual_columns", trueOrFalseSettingNoOracle},
    {"dictionary_use_async_executor", trueOrFalseSettingNoOracle},
    {"dictionary_validate_primary_key_type", trueOrFalseSettingNoOracle},
    {"distributed_aggregation_memory_efficient", trueOrFalseSetting},
    {"distributed_background_insert_batch", trueOrFalseSettingNoOracle},
    {"distributed_background_insert_split_batch_on_failure", trueOrFalseSettingNoOracle},
    {"distributed_cache_bypass_connection_pool", trueOrFalseSettingNoOracle},
    {"distributed_cache_discard_connection_if_unread_data", trueOrFalseSettingNoOracle},
    {"distributed_cache_fetch_metrics_only_from_current_az", trueOrFalseSettingNoOracle},
    {"distributed_cache_read_only_from_current_az", trueOrFalseSettingNoOracle},
    {"distributed_cache_throw_on_error", trueOrFalseSettingNoOracle},
    {"distributed_foreground_insert", trueOrFalseSettingNoOracle},
    {"distributed_group_by_no_merge", CHSetting(zeroOneTwo, {}, false)},
    {"distributed_insert_skip_read_only_replicas", trueOrFalseSettingNoOracle},
    {"distributed_product_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'deny'", "'local'", "'global'", "'allow'"};
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
    {"exact_rows_before_limit", trueOrFalseSetting},
    {"except_default_mode", setSetting},
    {"distributed_plan_execute_locally", trueOrFalseSettingNoOracle},
    /// {"external_table_functions_use_nulls", trueOrFalseSettingNoOracle},
    /// {"external_table_strict_query", CHSetting(trueOrFalse, {}, true)},
    {"extremes", trueOrFalseSettingNoOracle},
    {"fallback_to_stale_replicas_for_distributed_queries", trueOrFalseSetting},
    {"filesystem_cache_enable_background_download_during_fetch", trueOrFalseSettingNoOracle},
    {"filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage", trueOrFalseSettingNoOracle},
    {"filesystem_cache_name",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'cache_for_s3'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"filesystem_cache_prefer_bigger_buffer_size", trueOrFalseSetting},
    {"filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit", trueOrFalseSettingNoOracle},
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
    {"final", trueOrFalseSettingNoOracle},
    {"flatten_nested", trueOrFalseSetting},
    {"force_aggregate_partitions_independently", trueOrFalseSetting},
    {"distributed_plan_force_exchange_kind",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"''", "'Persisted'", "'Streaming'"};
             return rg.pickRandomly(choices);
         },
         {"''", "'Persisted'", "'Streaming'"},
         false)},
    {"force_grouping_standard_compatibility", trueOrFalseSettingNoOracle},
    {"force_optimize_skip_unused_shards", CHSetting(zeroOneTwo, {}, false)},
    {"force_optimize_skip_unused_shards_nesting", CHSetting(zeroOneTwo, {}, false)},
    /// {"force_index_by_date", trueOrFalseSettingNoOracle},
    /// {"force_optimize_projection", trueOrFalseSetting},
    {"force_remove_data_recursively_on_drop", trueOrFalseSettingNoOracle},
    {"format_capn_proto_use_autogenerated_schema", trueOrFalseSettingNoOracle},
    {"format_display_secrets_in_show_and_select", trueOrFalseSettingNoOracle},
    {"format_protobuf_use_autogenerated_schema", trueOrFalseSettingNoOracle},
    {"format_regexp_skip_unmatched", trueOrFalseSettingNoOracle},
    {"fsync_metadata", trueOrFalseSetting},
    {"function_json_value_return_type_allow_complex", trueOrFalseSettingNoOracle},
    {"function_json_value_return_type_allow_nullable", trueOrFalseSettingNoOracle},
    {"function_locate_has_mysql_compatible_argument_order", trueOrFalseSettingNoOracle},
    {"geo_distance_returns_float64_on_float64_arguments", trueOrFalseSettingNoOracle},
    {"grace_hash_join_initial_buckets",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    {"grace_hash_join_max_buckets",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 1, 1024)); }, {}, false)},
    /// {"group_by_overflow_mode", CHSetting([](RandomGenerator & rg) { const DB::Strings & choices = {"'throw'", "'break'", "'any'"}; return rg.pickRandomly(choices); }, {}, false)},
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
    {"ignore_materialized_views_with_dropped_target_table", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_access_entities_queries", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_named_collections_queries", trueOrFalseSettingNoOracle},
    {"ignore_on_cluster_for_replicated_udf_queries", trueOrFalseSettingNoOracle},
    {"implicit_transaction", trueOrFalseSettingNoOracle},
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
    {"input_format_mysql_dump_map_column_names", trueOrFalseSettingNoOracle},
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
    {"input_format_parquet_filter_push_down", trueOrFalseSetting},
    {"input_format_parquet_preserve_order", trueOrFalseSettingNoOracle},
    {"input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference", trueOrFalseSettingNoOracle},
    {"input_format_parquet_use_native_reader", trueOrFalseSettingNoOracle},
    {"input_format_protobuf_flatten_google_wrappers", trueOrFalseSettingNoOracle},
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
    {"input_format_values_interpret_expressions", trueOrFalseSettingNoOracle},
    {"insert_deduplicate", trueOrFalseSettingNoOracle},
    {"insert_distributed_one_random_shard", trueOrFalseSettingNoOracle},
    {"insert_keeper_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"insert_null_as_default", trueOrFalseSettingNoOracle},
    {"insert_quorum", CHSetting(zeroOneTwo, {}, false)},
    {"insert_quorum_parallel", trueOrFalseSettingNoOracle},
    {"intersect_default_mode", setSetting},
    {"interval_output_format", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'kusto'" : "'numeric'"; }, {}, false)},
    /// {"join_overflow_mode", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'throw'" : "'break'"; }, {}, false)},
    {"join_use_nulls", trueOrFalseSettingNoOracle},
    {"keeper_map_strict_mode", trueOrFalseSettingNoOracle},
    {"least_greatest_legacy_null_behavior", trueOrFalseSettingNoOracle},
    {"legacy_column_name_of_tuple_literal", trueOrFalseSettingNoOracle},
    {"lightweight_delete_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'alter_update'", "'lightweight_update'", "'lightweight_update_force'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    /// {"lightweight_deletes_sync", CHSetting(zeroOneTwo, {}, false)}, FINAL queries don't cover these
    {"limit", CHSetting(rowsRange, {}, false)},
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
    {"load_balancing_first_offset",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 8)); }, {}, false)},
    {"load_marks_asynchronously", trueOrFalseSettingNoOracle},
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
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.3, 0.2, 0, 5)); }, {}, false)},
    {"max_parsing_threads", threadSetting},
    {"max_parts_to_move",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, UINT32_C(4096))); },
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
    {"merge_tree_use_const_size_tasks_for_remote_reading", trueOrFalseSettingNoOracle},
    {"merge_tree_use_v1_object_and_dynamic_serialization", trueOrFalseSettingNoOracle},
    {"metrics_perf_events_enabled", trueOrFalseSettingNoOracle},
    {"min_free_disk_ratio_to_perform_insert", CHSetting(probRange, {}, false)},
    {"min_hit_rate_to_use_consecutive_keys_optimization", probRangeSetting},
    {"mongodb_throw_on_unsupported_query", trueOrFalseSettingNoOracle},
    {"multiple_joins_try_to_keep_original_names", trueOrFalseSetting},
    {"mutations_execute_nondeterministic_on_initiator", trueOrFalseSetting},
    {"mutations_execute_subqueries_on_initiator", trueOrFalseSetting},
    {"mutations_sync", CHSetting(zeroOneTwo, {}, false)},
    {"mysql_map_fixed_string_to_text_in_show_columns", trueOrFalseSettingNoOracle},
    {"mysql_map_string_to_text_in_show_columns", trueOrFalseSettingNoOracle},
    {"optimize_count_from_files", trueOrFalseSetting},
    {"distributed_plan_optimize_exchanges", trueOrFalseSettingNoOracle},
    {"optimize_extract_common_expressions", trueOrFalseSetting},
    {"optimize_on_insert", trueOrFalseSetting},
    {"optimize_or_like_chain", trueOrFalseSetting},
    {"optimize_throw_if_noop", trueOrFalseSettingNoOracle},
    {"optimize_time_filter_with_preimage", trueOrFalseSetting},
    {"optimize_trivial_insert_select", trueOrFalseSetting},
    {"output_format_arrow_compression_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'lz4_frame'", "'zstd'", "'none'"};
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
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'null'", "'deflate'", "'snappy'", "'zstd'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_avro_sync_interval",
     CHSetting(
         [](RandomGenerator & rg)
         { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 32, UINT32_C(10) * UINT32_C(1024) * UINT32_C(1024))); },
         {"32", "1024", "4096", "16384", "'10M'"},
         false)},
    {"output_format_binary_write_json_as_string", trueOrFalseSettingNoOracle},
    {"output_format_bson_string_as_string", trueOrFalseSettingNoOracle},
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
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'lz4'", "'snappy'", "'zlib'", "'zstd'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_orc_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_parallel_formatting", trueOrFalseSetting},
    {"output_format_parquet_bloom_filter_bits_per_value",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 100.0)); }, {}, false)},
    {"output_format_parquet_bloom_filter_flush_threshold_bytes",
     CHSetting(
         [](RandomGenerator & rg)
         { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, UINT32_C(1024) * UINT32_C(1024) * UINT32_C(1024))); },
         {},
         false)},
    {"output_format_parquet_compliant_nested_types", trueOrFalseSettingNoOracle},
    {"output_format_parquet_compression_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'snappy'", "'lz4'", "'brotli'", "'zstd'", "'gzip'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_datetime_as_uint32", trueOrFalseSettingNoOracle},
    {"output_format_parquet_fixed_string_as_fixed_byte_array", trueOrFalseSettingNoOracle},
    {"output_format_parquet_parallel_encoding", trueOrFalseSettingNoOracle},
    {"output_format_parquet_string_as_string", trueOrFalseSettingNoOracle},
    {"output_format_parquet_use_custom_encoder", trueOrFalseSettingNoOracle},
    {"output_format_parquet_version",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'1.0'", "'2.4'", "'2.6'", "'2.latest'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_parquet_write_bloom_filter", trueOrFalseSettingNoOracle},
    {"output_format_parquet_write_page_index", trueOrFalseSettingNoOracle},
    {"output_format_pretty_color",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'0'", "'1'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"output_format_pretty_fallback_to_vertical", trueOrFalseSettingNoOracle},
    {"output_format_pretty_glue_chunks", trueOrFalseSettingNoOracle},
    {"output_format_pretty_grid_charset", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'UTF-8'" : "'ASCII'"; }, {}, false)},
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
    /// {"parallel_replica_offset", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
    {"parallel_replicas_allow_in_with_subquery", trueOrFalseSetting},
    /// {"parallel_replicas_count", CHSetting([](RandomGenerator & rg) { return std::to_string(rg.nextSmallNumber() - 1); }, {"0", "1", "2", "3", "4"})},
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
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"};
             return rg.pickRandomly(choices);
         },
         {"'sampling_key'", "'read_tasks'", "'custom_key_range'", "'custom_key_sampling'", "'auto'"},
         false)},
    {"partial_result_on_first_cancel", trueOrFalseSettingNoOracle},
    {"parsedatetime_e_requires_space_padding", trueOrFalseSettingNoOracle},
    {"parsedatetime_parse_without_leading_zeros", trueOrFalseSettingNoOracle},
    {"postgresql_fault_injection_probability", CHSetting(probRange, {}, false)},
    {"precise_float_parsing", trueOrFalseSettingNoOracle},
    {"print_pretty_type_names", trueOrFalseSettingNoOracle},
    {"push_external_roles_in_interserver_queries", trueOrFalseSettingNoOracle},
    {"query_cache_compress_entries", trueOrFalseSetting},
    {"query_cache_share_between_users", trueOrFalseSettingNoOracle},
    {"query_cache_squash_partial_results", trueOrFalseSetting},
    {"query_condition_cache_store_conditions_as_plaintext", trueOrFalseSettingNoOracle},
    {"query_plan_use_new_logical_join_step", trueOrFalseSetting},
    {"read_from_filesystem_cache_if_exists_otherwise_bypass_cache", trueOrFalseSetting},
    {"read_from_page_cache_if_exists_otherwise_bypass_cache", trueOrFalseSetting},
    {"read_through_distributed_cache", trueOrFalseSetting},
    {"regexp_dict_allow_hyperscan", trueOrFalseSetting},
    {"regexp_dict_flag_case_insensitive", trueOrFalseSettingNoOracle},
    {"regexp_dict_flag_dotall", trueOrFalseSettingNoOracle},
    /*{"remote_filesystem_read_method", Gives issues on cloud
     CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'read'" : "'threadpool'"; }, {"'read'", "'threadpool'"}, false)},*/
    {"reject_expensive_hyperscan_regexps", trueOrFalseSetting},
    {"remote_filesystem_read_prefetch", trueOrFalseSetting},
    {"replace_running_query", trueOrFalseSettingNoOracle},
    {"restore_replace_external_dictionary_source_to_null", trueOrFalseSettingNoOracle},
    {"restore_replace_external_engines_to_null", trueOrFalseSettingNoOracle},
    {"restore_replace_external_table_functions_to_null", trueOrFalseSettingNoOracle},
    {"restore_replicated_merge_tree_to_shared_merge_tree", trueOrFalseSettingNoOracle},
    {"rows_before_aggregation", trueOrFalseSetting},
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
    {"schema_inference_make_json_columns_nullable", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_file", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_s3", trueOrFalseSettingNoOracle},
    {"schema_inference_use_cache_for_url", trueOrFalseSettingNoOracle},
    {"secondary_indices_enable_bulk_filtering", trueOrFalseSetting},
    {"select_sequential_consistency", trueOrFalseSetting},
    {"send_logs_level",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'debug'", "'information'", "'trace'", "'error'", "'test'", "'warning'", "'fatal'", "'none'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"send_progress_in_http_headers", trueOrFalseSettingNoOracle},
    {"serialize_query_plan", trueOrFalseSetting},
    {"shared_merge_tree_sync_parts_on_partition_operations", trueOrFalseSettingNoOracle},
    {"short_circuit_function_evaluation_for_nulls", trueOrFalseSetting},
    {"short_circuit_function_evaluation_for_nulls_threshold", probRangeSetting},
    {"show_table_uuid_in_table_create_query_if_not_nil", trueOrFalseSettingNoOracle},
    {"skip_download_if_exceeds_query_cache", trueOrFalseSetting},
    {"skip_redundant_aliases_in_udf", trueOrFalseSettingNoOracle},
    {"skip_unavailable_shards", trueOrFalseSettingNoOracle},
    /// {"set_overflow_mode", CHSetting([](RandomGenerator & rg) { return rg.nextBool() ? "'break'" : "'throw'"; }, {}, false)},
    {"splitby_max_substrings_includes_remaining_string", trueOrFalseSettingNoOracle},
    {"storage_file_read_method",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'read'", "'pread'", "'mmap'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    {"stream_like_engine_allow_direct_select", trueOrFalseSetting},
    {"system_events_show_zero_values", trueOrFalseSettingNoOracle},
    {"throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert", trueOrFalseSettingNoOracle},
    {"throw_if_no_data_to_insert", trueOrFalseSettingNoOracle},
    {"throw_on_error_from_cache_on_write_operations", trueOrFalseSettingNoOracle},
    {"throw_on_max_partitions_per_insert_block", trueOrFalseSettingNoOracle},
    {"throw_on_unsupported_query_inside_transaction", trueOrFalseSettingNoOracle},
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
    {"trace_profile_events", trueOrFalseSettingNoOracle},
    {"traverse_shadow_remote_data_paths", trueOrFalseSettingNoOracle},
    {"type_json_skip_duplicated_paths", trueOrFalseSettingNoOracle},
    {"union_default_mode", setSetting},
    {"unknown_packet_in_send_data",
     CHSetting([](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<uint32_t>(0.2, 0.2, 0, 1024)); }, {}, false)},
    {"update_insert_deduplication_token_in_dependent_materialized_views", trueOrFalseSettingNoOracle},
    {"update_parallel_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'sync'", "'auto'"};
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
    {"use_legacy_to_time", trueOrFalseSettingNoOracle},
    {"use_page_cache_for_disks_without_file_cache", trueOrFalseSetting},
    {"use_query_cache", trueOrFalseSetting},
    {"use_skip_indexes_if_final_exact_mode", CHSetting(trueOrFalse, {"0", "1"}, true)},
    {"use_structure_from_insertion_table_in_table_functions", CHSetting(zeroOneTwo, {}, false)},
    {"use_variant_as_common_type", CHSetting(trueOrFalse, {"0", "1"}, true)},
    {"use_with_fill_by_sorting_prefix", trueOrFalseSetting},
    {"validate_enum_literals_in_operators", trueOrFalseSettingNoOracle},
    {"validate_experimental_and_suspicious_types_inside_nested_types", trueOrFalseSettingNoOracle},
    {"validate_mutation_query", trueOrFalseSettingNoOracle},
    {"validate_polygons", trueOrFalseSettingNoOracle},
    {"vector_search_filter_strategy",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'auto'", "'postfilter'", "'prefilter'"};
             return rg.pickRandomly(choices);
         },
         {"'auto'", "'postfilter'", "'prefilter'"},
         false)},
    {"vector_search_postfilter_multiplier",
     CHSetting(
         [](RandomGenerator & rg) { return std::to_string(rg.thresholdGenerator<double>(0.2, 0.2, 0.0, 4.0)); },
         {"0", "0.001", "0.01", "0.1", "0.5", "0.9", "0.99", "0.999", "1", "1.5", "2", "2.5"},
         false)},
    {"wait_changes_become_visible_after_commit_mode",
     CHSetting(
         [](RandomGenerator & rg)
         {
             const DB::Strings & choices = {"'async'", "'wait'", "'wait_unknown'"};
             return rg.pickRandomly(choices);
         },
         {},
         false)},
    /// {"wait_for_async_insert", trueOrFalseSettingNoOracle},
    {"write_through_distributed_cache", trueOrFalseSettingNoOracle}};

std::unordered_map<String, CHSetting> queryOracleSettings;

std::unordered_map<String, CHSetting> formatSettings;

void loadFuzzerServerSettings(const FuzzConfig & fc)
{
    if (!fc.clusters.empty())
    {
        hotSettings.insert({{"enable_parallel_replicas", trueOrFalseSetting}});
        serverSettings.insert(
            {{"cluster_for_parallel_replicas",
              CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(fc.clusters) + "'"; }, {}, false)}});
    }
    for (const auto & [key, value] : hotSettings)
    {
        chassert(!value.oracle_values.empty());
        performanceSettings.insert({{key, value}});
    }
    for (const auto & setting : performanceSettings)
    {
        serverSettings.insert(setting);
    }
    for (auto & setting : serverSettings2)
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
          "max_limit_for_vector_search_queries",
          "max_number_of_partitions_for_independent_aggregation",
          "max_rows_to_transfer",
          "min_insert_block_size_rows",
          "min_insert_block_size_rows_for_materialized_views",
          "min_outstreams_per_resize_after_split",
          "output_format_parquet_batch_size",
          "output_format_parquet_data_page_size",
          "output_format_parquet_row_group_size",
          "page_cache_lookahead_blocks",
          "parallel_hash_join_threshold",
          "partial_merge_join_rows_in_right_blocks",
          "query_plan_max_limit_for_lazy_materialization"})
    {
        performanceSettings.insert({{entry, CHSetting(rowsRange, {"0", "512", "1024", "2048", "4096", "16384", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(rowsRange, {"0", "4", "8", "32", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    performanceSettings.insert({{"output_format_parquet_row_group_size", CHSetting(rowsRange, {}, false)}});
    serverSettings.insert({{"output_format_parquet_row_group_size", CHSetting(rowsRange, {}, false)}});

    /// Number of bytes values
    for (const auto & entry :
         {"aggregation_in_order_max_block_bytes",
          "cross_join_min_bytes_to_compress",
          "filesystem_cache_max_download_size",
          "filesystem_prefetch_max_memory_usage",
          "filesystem_prefetch_min_bytes_for_single_read_task",
          "filesystem_prefetch_step_bytes",
          "group_by_two_level_threshold_bytes",
          "input_format_max_block_size_bytes",
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
          "min_external_table_block_size_bytes",
          "min_insert_block_size_bytes",
          "min_insert_block_size_bytes_for_materialized_views",
          "output_format_parquet_row_group_size_bytes",
          "page_cache_block_size",
          "partial_merge_join_left_table_buffer_bytes",
          "prefer_external_sort_block_bytes",
          "preferred_block_size_bytes",
          "preferred_max_column_in_block_size_bytes",
          "prefetch_buffer_size",
          "query_cache_max_size_in_bytes",
          "remote_read_min_bytes_for_seek"})
    {
        performanceSettings.insert({{entry, CHSetting(bytesRange, {"32768", "65536", "1048576", "4194304", "33554432", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(bytesRange, {"0", "4", "8", "32", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    /// Block size settings
    for (const auto & entry :
         {"input_format_parquet_max_block_size", "max_block_size", "max_compress_block_size", "min_compress_block_size"})
    {
        performanceSettings.insert({{entry, CHSetting(highRange, {"1024", "2048", "4096", "8192", "16384", "'10M'"}, false)}});
        serverSettings.insert({{entry, CHSetting(highRange, {"4", "8", "32", "64", "1024", "4096", "16384", "'10M'"}, false)}});
    }
    if (!fc.timezones.empty())
    {
        serverSettings.insert(
            {{"session_timezone", CHSetting([&](RandomGenerator & rg) { return "'" + rg.pickRandomly(fc.timezones) + "'"; }, {}, false)}});
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
        {{"format_csv_delimiter", CHSetting(nastyStrings, {}, false)},
         {"format_csv_allow_single_quotes", trueOrFalseSettingNoOracle},
         {"format_csv_allow_double_quotes", trueOrFalseSettingNoOracle},
         {"format_csv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"format_tsv_null_representation", CHSetting(nastyStrings, {}, false)},
         {"input_format_binary_decode_types_in_binary_format", trueOrFalseSettingNoOracle},
         {"input_format_csv_arrays_as_nested_csv", trueOrFalseSettingNoOracle},
         {"input_format_csv_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_csv_enum_as_number", trueOrFalseSettingNoOracle},
         {"input_format_custom_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_json_empty_as_default", trueOrFalseSettingNoOracle},
         {"input_format_msgpack_number_of_columns",
          CHSetting([](RandomGenerator & rg) { return std::to_string(rg.randomInt<int32_t>(0, 20)); }, {}, false)},
         /// {"input_format_native_decode_types_in_binary_format", trueOrFalseSettingNoOracle}, may block the client
         {"input_format_tsv_crlf_end_of_line", trueOrFalseSettingNoOracle},
         {"input_format_tsv_detect_header", trueOrFalseSettingNoOracle},
         {"input_format_with_names_use_header", trueOrFalseSettingNoOracle},
         {"input_format_with_types_use_header", trueOrFalseSettingNoOracle},
         {"low_cardinality_allow_in_native_format", trueOrFalseSettingNoOracle},
         {"output_format_binary_encode_types_in_binary_format", trueOrFalseSettingNoOracle},
         {"output_format_csv_crlf_end_of_line", trueOrFalseSettingNoOracle},
         {"output_format_msgpack_uuid_representation",
          CHSetting(
              [](RandomGenerator & rg)
              {
                  const DB::Strings & choices = {"'ext'", "'str'", "'bin'"};
                  return rg.pickRandomly(choices);
              },
              {},
              false)},
         /// {"output_format_native_encode_types_in_binary_format", trueOrFalseSettingNoOracle}, may block the client
         {"output_format_tsv_crlf_end_of_line", trueOrFalseSettingNoOracle}});
}

std::unique_ptr<SQLType> size_tp, null_tp;

std::unordered_map<String, DB::Strings> systemTables;

void loadSystemTables(FuzzConfig & fc)
{
    size_tp = std::make_unique<IntType>(64, true);
    null_tp = std::make_unique<BoolType>();

    fc.loadSystemTables(systemTables);
}

}
