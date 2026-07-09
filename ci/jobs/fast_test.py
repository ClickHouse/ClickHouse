import argparse
import os
import platform
import time
import sys
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent
repo_path_normalized = str(repo_path)
sys.path.append(str(repo_path / "ci"))

from ci.defs.defs import ToolSet, chcache_secret
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import ContextManager, MetaClasses, Shell, Utils

current_directory = Utils.cwd()
build_dir = f"{current_directory}/ci/tmp/fast_build"
temp_dir = f"{current_directory}/ci/tmp/"
build_dir_normalized = str(repo_path / "ci" / "tmp" / "fast_build")


def clone_submodules():
    submodules_to_update = [
        "contrib/sysroot",
        "contrib/magic_enum",
        "contrib/abseil-cpp",
        "contrib/boost",
        "contrib/zlib-ng",
        "contrib/libxml2",
        "contrib/fmtlib",
        "contrib/base64",
        "contrib/cctz",
        "contrib/libcpuid",
        "contrib/libdivide",
        "contrib/double-conversion",
        "contrib/llvm-project",
        "contrib/lz4",
        "contrib/zstd",
        "contrib/fastops",
        "contrib/rapidjson",
        "contrib/re2",
        "contrib/sparsehash-c11",
        "contrib/croaring",
        "contrib/miniselect",
        "contrib/xz",
        "contrib/zmij",
        "contrib/fast_float",
        "contrib/NuRaft",
        "contrib/jemalloc",
        "contrib/replxx",
        "contrib/wyhash",
        "contrib/c-ares",
        "contrib/morton-nd",
        "contrib/xxHash",
        "contrib/simdjson",
        "contrib/simdcomp",
        "contrib/liburing",
        "contrib/libfiu",
        "contrib/yaml-cpp",
        "contrib/corrosion",
        "contrib/StringZilla",
        "contrib/rust_vendor",
        "contrib/clickstack",
    ]

    res = Shell.check("git submodule sync", verbose=True, strict=True)
    res = res and Shell.check(
        # Init only the needed submodules, not all 129
        command="git submodule init -- " + " ".join(submodules_to_update),
        verbose=True,
        strict=True,
    )

    if os.path.isdir(".git/modules/contrib") and os.listdir(".git/modules/contrib"):
        # Submodule cache was restored by runner.py — just populate working trees
        print("Submodule cache detected, populating working trees from cache")
        res = res and Shell.check(
            command="git submodule update --depth 1 --single-branch -- " + " ".join(submodules_to_update),
            timeout=300,
            retries=3,
            verbose=True,
        )
    else:
        res = res and Shell.check(
            command=f"xargs --max-procs={min([Utils.cpu_count(), 20])} --null --no-run-if-empty --max-args=1 git submodule update --depth 1 --single-branch",
            stdin_str="\0".join(submodules_to_update) + "\0",
            timeout=300,
            retries=3,
            verbose=True,
        )
    # NOTE: the three "git submodule foreach" cleanup commands (reset --hard,
    # checkout @ -f, clean -xfd) that used to run here were removed because
    # "git submodule update" already checks out the correct commit into a
    # fresh clone.  The foreach commands added ~7s of sequential overhead
    # iterating over every submodule for no benefit in the fast-test context.
    return res


def update_path_ch_config(config_file_path=""):
    print("Updating path in clickhouse config")
    config_file_path = (
        config_file_path or f"{temp_dir}/etc/clickhouse-server/config.xml"
    )
    ssl_config_file_path = f"{temp_dir}/etc/clickhouse-server/config.d/ssl_certs.xml"
    try:
        with open(config_file_path, "r", encoding="utf-8") as file:
            content = file.read()

        with open(ssl_config_file_path, "r", encoding="utf-8") as file:
            ssl_config_content = file.read()
        content = content.replace(">/var/", f">{temp_dir}/var/")
        content = content.replace(">/etc/", f">{temp_dir}/etc/")
        ssl_config_content = ssl_config_content.replace(">/etc/", f">{temp_dir}/etc/")
        with open(config_file_path, "w", encoding="utf-8") as file:
            file.write(content)
        with open(ssl_config_file_path, "w", encoding="utf-8") as file:
            file.write(ssl_config_content)
    except Exception as e:
        print(f"ERROR: failed to update config, exception: {e}")
        return False
    return True


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    BUILD = "build"
    CONFIG = "config"
    TEST = "test"


# Tests that are skipped on Darwin (macOS) because they rely on Linux-specific
# features (e.g. distributed/ZooKeeper setup, procfs, HTTP server, etc.).
_DARWIN_SKIP_TESTS = (
    "00004_shard_format_ast_and_remote_table",
    "00005_shard_format_ast_and_remote_table_lambda",
    "00019_shard_quantiles_totals_distributed",
    "00026_shard_something_distributed",
    "00028_shard_big_agg_aj_distributed",
    "00059_shard_global_in",
    "00059_shard_global_in_mergetree",
    "00065_shard_float_literals_formatting",
    "00075_shard_formatting_negate_of_negative_literal",
    "00098_shard_i_union_all",
    "00108_shard_totals_after_having",
    "00112_shard_totals_after_having",
    "00113_shard_group_array",
    "00115_shard_in_incomplete_result",
    "00123_shard_unmerged_result_when_max_distributed_connections_is_one",
    "00124_shard_distributed_with_many_replicas",
    "00154_shard_distributed_with_distinct",
    "00162_shard_global_join",
    "00163_shard_join_with_empty_table",
    "00171_shard_array_of_tuple_remote",
    "00177_inserts_through_http_parts",
    "00184_shard_distributed_group_by_no_merge",
    "00195_shard_union_all_and_global_in",
    "00200_shard_distinct_order_by_limit_distributed",
    "00211_shard_query_formatting_aliases",
    "00217_shard_global_subquery_columns_with_same_name",
    "00220_shard_with_totals_in_subquery_remote_and_limit",
    "00223_shard_distributed_aggregation_memory_efficient",
    "00224_shard_distributed_aggregation_memory_efficient_and_overflows",
    "00228_shard_quantiles_deterministic_merge_overflow",
    "00252_shard_global_in_aggregate_function",
    "00257_shard_no_aggregates_and_constant_keys",
    "00266_shard_global_subquery_and_aliases",
    "00274_shard_group_array",
    "00275_shard_quantiles_weighted",
    "00290_shard_aggregation_memory_efficient",
    "00293_shard_max_subquery_depth",
    "00294_shard_enums",
    "00295_global_in_one_shard_rows_before_limit",
    "00305_http_and_readonly",
    "00337_shard_any_heavy",
    "00368_format_option_collision",
    "00372_cors_header",
    "00379_system_processes_port",
    "00409_shard_limit_by",
    "00424_shard_aggregate_functions_of_nullable",
    "00491_shard_distributed_and_aliases_in_where_having",
    "00494_shard_alias_substitution_bug",
    "00506_shard_global_in_union",
    "00515_shard_desc_table_functions_and_subqueries",
    "00534_functions_bad_arguments11",
    "00534_functions_bad_arguments2",
    "00546_shard_tuple_element_formatting",
    "00550_join_insert_select",
    "00558_parse_floats",
    "00563_insert_into_remote_and_zookeeper_long",
    "00563_shard_insert_into_remote",
    "00573_shard_aggregation_by_empty_set",
    "00588_shard_distributed_prewhere",
    "00604_shard_remote_and_columns_with_defaults",
    "00612_shard_count",
    "00613_shard_distributed_max_execution_time",
    "00614_shard_same_header_for_local_and_remote_node_in_distributed_query",
    "00625_query_in_form_data",
    "00634_logging_shard",
    "00635_shard_distinct_order_by",
    "00636_partition_key_parts_pruning",
    "00673_subquery_prepared_set_performance",
    "00675_shard_remote_with_table_function",
    "00677_shard_any_heavy_merge",
    "00678_shard_funnel_window",
    "00697_in_subquery_shard",
    "00700_decimal_math",
    "00704_drop_truncate_memory_table",
    "00725_quantiles_shard",
    "00850_global_join_dups",
    "00857_global_joinsavel_table_alias",
    "00898_parsing_bad_diagnostic_message",
    "00900_entropy_shard",
    "00909_kill_not_initialized_query",
    "00942_mutate_index",
    "00943_materialize_index",
    "00952_insert_into_distributed_with_materialized_column",
    "00961_checksums_in_system_parts_columns_table",
    "00975_sample_prewhere_distributed",
    "00976_shard_low_cardinality_achimbab",
    "00980_shard_aggregation_state_deserialization",
    "00995_exception_while_insert",
    "01018_Distributed__shard_num",
    "01030_limit_by_with_ties_error",
    "01034_prewhere_max_parallel_replicas_distributed",
    "01034_sample_final_distributed",
    "01037_polygon_dicts_correctness_all",
    "01037_polygon_dicts_correctness_fast",
    "01040_distributed_background_insert_batch_inserts",
    "01043_geo_distance",
    "01046_trivial_count_query_distributed",
    "01054_random_printable_ascii_ubsan",
    "01056_predicate_optimizer_bugs",
    "01062_max_parser_depth",
    "01071_force_optimize_skip_unused_shards",
    "01072_optimize_skip_unused_shards_const_expr_eval",
    "01077_mutations_index_consistency",
    "01081_PartialSortingTransform_full_column",
    "01085_max_distributed_connections",
    "01098_sum",
    "01099_parallel_distributed_insert_select",
    "01104_distributed_numbers_test",
    "01104_distributed_one_test",
    "01121_remote_scalar_subquery",
    "01133_max_result_rows",
    "01160_table_dependencies",
    "01177_group_array_moving",
    "01182_materialized_view_different_structure",
    "01211_optimize_skip_unused_shards_type_mismatch",
    "01223_dist_on_dist",
    "01227_distributed_global_in_issue_2610",
    "01227_distributed_merge_global_in_primary_key",
    "01244_optimize_distributed_group_by_sharding_key",
    "01245_distributed_group_by_no_merge_with-extremes_and_totals",
    "01245_limit_infinite_sources",
    "01247_optimize_distributed_group_by_sharding_key_dist_on_dist",
    "01253_subquery_in_aggregate_function_JustStranger",
    "01259_combinator_distinct_distributed",
    "01263_type_conversion_nvartolomei",
    "01268_procfs_metrics",
    "01268_shard_avgweighted",
    "01270_optimize_skip_unused_shards_low_cardinality",
    "01288_shard_max_network_bandwidth",
    "01290_max_execution_speed_distributed",
    "01291_distributed_low_cardinality_memory_efficient",
    "01295_aggregation_bug_11413",
    "01302_polygons_distance",
    "01317_no_password_in_command_line",
    "01319_optimize_skip_unused_shards_nesting",
    "01356_state_resample",
    "01412_group_array_moving_shard",
    "01412_row_from_totals",
    "01417_query_time_in_system_events",
    "01418_query_scope_constants_and_remote",
    "01430_moving_sum_empty_state",
    "01451_dist_logs",
    "01455_opentelemetry_distributed",
    "01455_shard_leaf_max_rows_bytes_to_read",
    "01473_event_time_microseconds",
    "01505_distributed_local_type_conversion_enum",
    "01509_dictionary_preallocate",
    "01509_format_raw_blob",
    "01514_distributed_cancel_query_on_error",
    "01517_select_final_distributed",
    "01521_distributed_query_hang",
    "01528_allow_nondeterministic_optimize_skip_unused_shards",
    "01529_bad_memory_tracking",
    "01548_parallel_parsing_max_memory",
    "01548_query_log_query_execution_ms",
    "01532_having_with_totals",
    "01533_quantile_deterministic_assert",
    "01533_sum_if_nullable_bug",
    "01557_max_parallel_replicas_no_sample",
    "01560_merge_distributed_join",
    "01560_ttl_remove_empty_parts",
    "01568_window_functions_distributed",
    "01569_query_profiler_big_query_id",
    "01583_parallel_parsing_exception_with_offset",
    "01584_distributed_buffer_cannot_find_column",
    "01597_columns_list_ignored",
    "01600_quota_by_forwarded_ip",
    "01602_insert_into_table_function_cluster",
    "01602_max_distributed_connections",
    "01621_clickhouse_compressor",
    "01636_nullable_fuzz2",
    "01639_distributed_sync_insert_zero_rows",
    "01640_distributed_async_insert_compression",
    "01642_if_nullable_regression",
    "01644_distributed_async_insert_fsync_smoke",
    "01646_rewrite_sum_if_bug",
    "01651_bugs_from_15889",
    "01655_agg_if_nullable",
    "01660_sum_ubsan",
    "01683_dist_INSERT_block_structure_mismatch",
    "01684_insert_specify_shard_id",
    "01685_ssd_cache_dictionary_complex_key",
    "01710_aggregate_projection_with_normalized_states",
    "01710_minmax_count_projection_distributed_query",
    "01710_projections_in_distributed_query",
    "01710_projections_optimize_aggregation_in_order",
    "01710_projections_partial_optimize_aggregation_in_order",
    "01720_country_intersection",
    "01750_parsing_exception",
    "01752_distributed_query_sigsegv",
    "01753_optimize_aggregation_in_order",
    "01754_cluster_all_replicas_shard_num",
    "01755_shard_pruning_with_literal",
    "01756_optimize_skip_unused_shards_rewrite_in",
    "01757_optimize_skip_unused_shards_limit",
    "01758_optimize_skip_unused_shards_once",
    "01780_column_sparse_full",
    "01785_parallel_formatting_memory",
    "01787_map_remote",
    "01790_dist_INSERT_block_structure_mismatch_types_and_names",
    "01791_dist_INSERT_block_structure_mismatch",
    "01801_distinct_group_by_shard",
    "01814_distributed_push_down_limit",
    "01822_async_read_from_socket_crash",
    "01834_alias_columns_laziness_filimonov",
    "01851_hedged_connections_external_tables",
    "01854_HTTP_dict_decompression",
    "01860_Distributed__shard_num_GROUP_BY",
    "01872_initial_query_start_time",
    "01882_total_rows_approx",
    "01883_subcolumns_distributed",
    "01890_materialized_distributed_join",
    "01892_setting_limit_offset_distributed",
    "01901_in_literal_shard_prune",
    "01915_for_each_crakjie",
    "01922_sum_null_for_remote",
    "01924_argmax_bitmap_state",
    "01927_query_views_log_matview_exceptions",
    "01930_optimize_skip_unused_shards_rewrite_in",
    "01932_null_valid_identifier",
    "01932_remote_sharding_key_column",
    "01940_custom_tld_sharding_key",
    "01943_query_id_check",
    "01948_group_bitmap_and_or_xor_fix",
    "01956_skip_unavailable_shards_excessive_attempts",
    "01961_roaring_memory_tracking",
    "02001_dist_on_dist_WithMergeableStateAfterAggregation",
    "02001_hostname_test",
    "02001_shard_num_shard_count",
    "02002_row_level_filter_bug",
    "02003_WithMergeableStateAfterAggregationAndLimit_LIMIT_BY_LIMIT_OFFSET",
    "02003_memory_limit_in_client",
    "02021_exponential_sum_shard",
    "02022_storage_filelog_one_file",
    "02023_storage_filelog",
    "02023_transform_or_to_in",
    "02025_storage_filelog_virtual_col",
    "02030_client_unknown_database",
    "02035_isNull_isNotNull_format",
    "02040_clickhouse_benchmark_query_id_pass_through",
    "02047_log_family_data_file_sizes",
    "02050_client_profile_events",
    "02095_function_get_os_kernel_version",
    "02096_totals_global_in_bug",
    "02110_clickhouse_local_custom_tld",
    "02116_clickhouse_stderr",
    "02121_pager",
    "02122_join_group_by_timeout",
    "02124_json_each_row_with_progress",
    "02125_fix_storage_filelog",
    "02126_fix_filelog",
    "02133_distributed_queries_formatting",
    "02141_clickhouse_local_interactive_table",
    "02152_bool_type_parsing",
    "02163_shard_num",
    "02165_replicated_grouping_sets",
    "02175_distributed_join_current_database",
    "02176_optimize_aggregation_in_order_empty",
    "02177_merge_optimize_aggregation_in_order",
    "02179_degrees_radians",
    "02183_array_tuple_literals_remote",
    "02183_combinator_if",
    "02203_shebang",
    "02205_HTTP_user_agent",
    "02206_format_override",
    "02221_parallel_replicas_bug",
    "02224_parallel_distributed_insert_select_cluster",
    "02225_parallel_distributed_insert_select_view",
    "02228_unquoted_dates_in_csv_schema_inference",
    "02233_HTTP_ranged",
    "02250_ON_CLUSTER_grant",
    "02253_empty_part_checksums",
    "02263_format_insert_settings",
    "02281_limit_by_distributed",
    "02293_grouping_function",
    "02294_anova_cmp",
    "02310_profile_events_insert",
    "02332_dist_insert_send_logs_level",
    "02341_global_join_cte",
    "02343_group_by_use_nulls_distributed",
    "02344_distinct_limit_distiributed",
    "02346_additional_filters",
    "02346_additional_filters_index",
    "02359_send_logs_source_regexp",
    "02366_union_decimal_conversion",
    "02377_optimize_sorting_by_input_stream_properties_2",
    "02381_client_prints_server_side_time",
    "02383_join_and_filtering_set",
    "02420_final_setting_analyzer",
    "02420_stracktrace_debug_symbols",
    "02423_ddl_for_opentelemetry",
    "02454_create_table_with_custom_disk",
    "02457_insert_select_progress_http",
    "02466_distributed_query_profiler",
    "02482_insert_into_dist_race",
    "02483_password_reset",
    "02490_benchmark_max_consecutive_errors",
    "02494_optimize_group_by_function_keys_and_alias_columns",
    "02494_query_cache_http_introspection",
    "02500_remove_redundant_distinct",
    "02501_limits_on_result_for_view",
    "02511_complex_literals_as_aggregate_function_parameters",
    "02521_grouping_sets_plus_memory_efficient_aggr",
    "02525_different_engines_in_temporary_tables",
    "02531_two_level_aggregation_bug",
    "02532_send_logs_level_test",
    "02536_distributed_detach_table",
    "02539_settings_alias",
    "02540_input_format_json_ignore_unknown_keys_in_named_tuple",
    "02550_client_connections_credentials",
    "02560_tuple_format",
    "02566_analyzer_limit_settings_distributed",
    "02571_local_desc_abort_on_twitter_json",
    "02596_build_set_and_remote",
    "02695_logical_optimizer_alias_bug",
    "02703_max_local_write_bandwidth",
    "02704_max_backup_bandwidth",
    "02705_projection_and_ast_optimizations_bug",
    "02761_ddl_initial_query_id",
    "02765_queries_with_subqueries_profile_events",
    "02768_cse_nested_distributed",
    "02768_into_outfile_extensions_format",
    "02771_system_user_processes",
    "02785_global_join_too_many_columns",
    "02790_async_queries_in_query_log",
    "02790_optimize_skip_unused_shards_join",
    "02803_remote_cannot_clone_block",
    "02804_clusterAllReplicas_insert",
    "02813_float_parsing",
    "02815_range_dict_no_direct_join",
    "02818_memory_profiler_sample_min_max_allocation_size",
    "02835_fuzz_remove_redundant_sorting",
    "02835_parallel_replicas_over_distributed",
    "02836_file_diagnostics_while_reading_header",
    "02841_check_table_progress",
    "02844_distributed_virtual_columns",
    "02859_replicated_db_name_zookeeper",
    "02874_array_random_sample",
    "02875_parallel_replicas_cluster_all_replicas",
    "02875_parallel_replicas_remote",
    "02884_async_insert_native_protocol_4",
    "02887_tuple_element_distributed",
    "02889_file_log_save_errors",
    "02889_parts_columns_filenames",
    "02894_ast_depth_check",
    "02895_peak_memory_usage_http_headers_regression",
    "02900_clickhouse_local_drop_current_database",
    "02901_parallel_replicas_rollup",
    "02907_http_exception_json_bug",
    "02915_analyzer_fuzz_5",
    "02916_local_insert_into_function",
    "02922_analyzer_aggregate_nothing_type",
    "02933_replicated_database_forbid_create_as_select",
    "02935_http_content_type_with_http_headers_progress",
    "02950_dictionary_ssd_cache_short_circuit",
    "02954_analyzer_fuzz_i57086",
    "02961_drop_tables",
    "02962_analyzer_resolve_group_by_on_shards",
    "02967_analyzer_fuzz",
    "02968_file_log_multiple_read",
    "02971_analyzer_remote_id",
    "02971_limit_by_distributed",
    "02984_topk_empty_merge",
    "02985_shard_query_start_time",
    "02992_analyzer_group_by_const",
    "02998_http_redirects",
    "02999_scalar_subqueries_bug_1",
    "03001_insert_threads_deduplication",
    "03008_deduplication_remote_insert_select",
    "03010_file_log_large_poll_batch_size",
    "03010_virtual_memory_mappings_asynchronous_metrics",
    "03012_prewhere_merge_distributed",
    "03013_forbid_attach_table_if_active_replica_already_exists",
    "03018_analyzer_distributed_query_with_positional_arguments",
    "03020_long_values_pretty_are_not_cut_if_single",
    "03021_get_client_http_header",
    "03023_group_by_use_nulls_analyzer_crashes",
    "03025_clickhouse_host_env",
    "03031_filter_float64_logical_error",
    "03035_max_insert_threads_support",
    "03095_group_by_server_constants_bug",
    "03096_text_log_format_string_args_not_empty",
    "03113_analyzer_not_found_column_in_block_2",
    "03114_analyzer_cte_with_join",
    "03124_analyzer_nested_CTE_dist_in",
    "03133_help_message_verbosity",
    "03143_cte_scope",
    "03143_group_by_constant_secondary",
    "03147_rows_before_limit_fix",
    "03154_recursive_cte_distributed",
    "03150_trace_log_add_build_id",
    "03155_analyzer_interpolate",
    "03156_analyzer_array_join_distributed",
    "03160_pretty_format_tty",
    "03164_analyzer_global_in_alias",
    "03164_selects_with_pk_usage_profile_event",
    "03165_storage_merge_view_prewhere",
    "03172_system_detached_tables",
    "03174_exact_rows_before_aggregation",
    "03196_local_memory_limit",
    "03204_distributed_with_scalar_subquery",
    "03208_buffer_over_distributed_type_mismatch",
    "03213_distributed_analyzer",
    "03214_parsing_archive_name_file",
    "03215_analyzer_materialized_constants_bug",
    "03217_datetime64_constant_to_ast",
    "03221_merge_profile_events",
    "03228_clickhouse_local_copy_argument",
    "03229_query_condition_cache_folded_constants",
    "03240_insert_select_named_tuple",
    "03243_cluster_not_found_column",
    "03248_with_insert_with",
    "03254_limit_by_with_offset_parallel_replicas",
    "03258_nonexistent_db",
    "03259_grouping_sets_aliases",
    "03269_bf16",
    "03271_benchmark_metrics",
    "03271_parse_sparse_columns_defaults",
    "03275_basic_auth_interactive",
    "03277_analyzer_array_join_fix",
    "03277_dead_letter_queue_unsupported",
    "03287_dynamic_and_json_squashing_fix",
    "03302_analyzer_distributed_filter_push_down",
    "03305_mergine_aggregated_filter_push_down",
    "03305_parallel_with_query_log",
    "03303_distributed_explain",
    "03306_expose_headers",
    "03316_analyzer_unique_table_aliases_dist",
    "03321_forwarded_for",
    "03322_initial_query_start_time_check",
    "03328_syntax_error_exception",
    "03344_clickhouse_extract_from_config_try",
    "03360_bool_remote",
    "03362_basic_auth_interactive_not_with_authorization_never",
    "03362_merge_tree_with_background_refresh",
    "03364_estimate_compression_ratio",
    "03369_predicate_pushdown_enforce_literal_type",
    "03369_variant_escape_filename_merge_tree",
    "03371_analyzer_filter_pushdown_distributed",
    "03371_constant_alias_columns",
    "03371_dynamic_values_parsing_templates",
    "03381_clickhouse_local_empty_default_database",
    "03381_file_log_merge_empty",
    "03381_remote_constants",
    "03381_udf_asterisk",
    "03394_pr_insert_select",
    "03397_disallow_empty_session_id",
    "03400_distributed_final",
    "03400_explain_distributed_bug",
    "03403_parallel_blocks_marshalling_for_distributed",
    "03404_lazy_materialization_distributed",
    "03405_ssd_cache_incorrect_min_max_lifetimes_and_block_size",
    "03408_limit_by_rows_before_limit_dist",
    "03444_distributed_hedged_requests_async_socket",
    "03448_analyzer_array_join_alias_in_join_using_bug",
    "03448_window_functions_distinct_distributed",
    "03459_socket_asynchronous_metrics",
    "03454_global_join_index_subqueries",
    "03457_numeric_indexed_vector_build",
    "03513_fix_shard_num_column_to_function_pass_with_nulls",
    "03519_analyzer_tuple_cast",
    "03519_cte_allow_push_predicate_ast_for_distributed_subqueries_bug",
    "03520_analyzer_distributed_in_cte_bug",
    "03526_columns_substreams_in_wide_parts",
    "03536_ch_as_client_and_local",
    "03527_format_insert_partition",
    "03533_column_array_insert_broken_offsets",
    "03533_lexer_c_library",
    "03537_clickhouse_local_drop_view_sync_temp_mode",
    "03545_progress_header_goes_first",
    "03546_json_input_output_map_as_array",
    "03554_connection_crash",
    "03562_parallel_replicas_remote_with_cluster",
    "03567_join_using_projection_distributed",
    "03572_pr_remote_in_subquery",
    "03577_server_constant_folding",
    "03593_prewhere_bytes_read_stat_bug",
    "03593_remote_map_in",
    "03595_parallel_replicas_join_remote",
    "03595_pread_threadpool_direct_io",
    "03610_remote_queries_with_describe_compact_output",
    "03620_analyzer_distributed_global_in",
    "03620_distributed_index_analysis",
    "03620_mergeTreeAnalyzeIndexesUUID",
    "03622_explain_indexes_distributed_index_analysis",
    "03636_benchmark_error_messages",
    "03653_benchmark_proto_caps_option",
    "03658_negative_limit_offset_distributed",
    "03681_distributed_fractional_limit_offset",
    "03701_distributed_index_analysis_async_modes",
    "03702_geometry_functions",
    "03703_prelimit_explain_message",
    "03710_array_join_in_map_bug",
    "03713_replicated_columns_in_external_data_bug",
    "03726_distributed_alias_column_order",
    "03733_pr_view_filter_pushdown",
    "03746_buffers_input_output_format_misc",
    "03748_async_insert_reset_setting",
    "03757_optimize_skip_unused_shards_with_type_cast",
    "03778_print_time_initial_query",
    "03783_serialize_into_sparse_with_subcolumn_extraction",
    "03786_AST_formatting_inconsistencies_in_debug_check",
    "03787_no_excessive_output_on_syntax_error",
    "03794_global_in_nullable_type_mismatch",
    "03811_negative_limit_offset_distributed_large",
    "03821_remote_grouping_set_aggregation_keys_assert",
    "03836_distributed_index_analysis_pk_expression",
    "03836_distributed_index_analysis_skip_index_expression",
    "03911_fractional_limit_offset_distributed_large",
    "03928_loop_row_policy",
    "03976_geometry_functions_accept_subtypes",
    "04001_materialized_cte_distributed",
    "04010_describe_remote_rbac_bypass",
    "04029_distributed_index_analysis_sampling",
    "04036_materialized_cte_distributed_race",
    "04037_obfuscator_dictionary_definitions",
    "04041_materialized_cte_parallel_replicas",
    "04043_materialized_cte_serialize_query_plan",
    "04049_dictionary_local_create_with_bogus_function",
    "04052_distributed_index_analysis_in_subquery_no_quadratic",
    "04054_backup_restore_validate_entry_paths",
    "04056_execute_as_format",
    "04056_npy_large_shape_validation",
    "04061_trivial_count_aggregate_function_argument_types_distributed",
)


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Fast Test Job")
    parser.add_argument(
        "--test",
        help="Optional. Space-separated test name patterns",
        default=[],
        nargs="+",
        action="extend")
    parser.add_argument(
        "--skip",
        help="Optional. Space-separated test names to skip",
        default=[],
        nargs="+",
        action="extend")
    parser.add_argument("--param", help="Optional custom job start stage", default=None)
    parser.add_argument("--set-status-success", help="Forcefully set a green status", action="store_true")
    return parser.parse_args()

def main():
    args = parse_args()
    if platform.system() == "Darwin":
        args.skip = list(_DARWIN_SKIP_TESTS) + args.skip
    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    clickhouse_bin_path = Path(f"{build_dir}/programs/clickhouse")

    for path in [
        Path(temp_dir) / "clickhouse",
        clickhouse_bin_path,
        Path(current_directory) / "clickhouse",
    ]:
        if path.is_file():
            clickhouse_bin_path = path
            print(f"NOTE: clickhouse binary is found [{clickhouse_bin_path}] - skip the build")

            stages = [JobStages.CONFIG, JobStages.TEST]
            resolved_clickhouse_bin_path = clickhouse_bin_path.resolve()
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-server")
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-client")
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-local")
            Shell.check(f"chmod +x {resolved_clickhouse_bin_path}", strict=True)

            break
    else:
        print(
            f"NOTE: clickhouse binary is not found [{clickhouse_bin_path}] - will be built"
        )

    # Global sccache settings for local and CI runs
    os.environ["SCCACHE_DIR"] = f"{temp_dir}/sccache"
    os.environ["SCCACHE_CACHE_SIZE"] = "40G"
    os.environ["SCCACHE_IDLE_TIMEOUT"] = "7200"
    os.environ["SCCACHE_BUCKET"] = Settings.S3_ARTIFACT_PATH
    os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    os.environ["SCCACHE_ERROR_LOG"] = f"{build_dir}/sccache.log"
    os.environ["SCCACHE_LOG"] = "info"
    info = Info()
    if info.is_local_run:
        print("NOTE: It's a local run")
        if os.environ.get("SCCACHE_ENDPOINT"):
            print(f"NOTE: Using custom sccache endpoint: {os.environ['SCCACHE_ENDPOINT']}")
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            print("NOTE: Using custom AWS credentials for sccache")
        else:
            os.environ["SCCACHE_S3_NO_CREDENTIALS"] = "true"
    else:
        os.environ["CH_HOSTNAME"] = (
            "https://build-cache.eu-west-1.aws.clickhouse-staging.com"
        )
        os.environ["CH_USER"] = "ci_builder"
        os.environ["CH_PASSWORD"] = chcache_secret.get_value()
        os.environ["CH_USE_LOCAL_CACHE"] = "false"

    Utils.add_to_PATH(
        f"{os.path.dirname(clickhouse_bin_path)}:{current_directory}/tests"
    )

    res = True
    results = []
    attach_files = []
    job_info = ""

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        results.append(
            Result.from_commands_run(
                name="Checkout Submodules",
                command=clone_submodules,
            )
        )
        res = results[-1].is_ok()

    os.makedirs(build_dir, exist_ok=True)

    if res and JobStages.CMAKE in stages:
        # The sccache server sometimes fails to start because of issues with S3.
        # Start it explicitly with retries before cmake, since cmake can invoke
        # the compiler during configuration. Non-fatal: build can proceed without it.
        if not Shell.check("sccache --start-server", retries=3):
            print("WARNING: sccache server failed to start, build will proceed without it")
        results.append(
            # TODO: commented out to make job platform agnostic
            #   -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake \
            Result.from_commands_run(
                name="Cmake configuration",
                command=f"cmake {repo_path_normalized} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} \
                -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} \
                -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} \
                -DENABLE_LIBRARIES=0 \
                -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 -DENABLE_NURAFT=1 -DENABLE_SIMDJSON=1 \
                -DENABLE_LEXER_TEST=1 \
                -DBUILD_STRIPPED_BINARY=1 \
                -DENABLE_JEMALLOC=1 -DENABLE_LIBURING=1 -DENABLE_YAML_CPP=1 -DENABLE_RUST=1 \
                -DUSE_SYSTEM_COMPILER_RT=1 \
                -B {build_dir_normalized}",
                workdir=repo_path_normalized,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        Shell.check("sccache --show-stats")
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"command time -v cmake --build {build_dir_normalized} --"
                " clickhouse-bundle clickhouse-stripped lexer_test",
            )
        )
        Shell.check(f"{build_dir}/rust/chcache/chcache stats")
        Shell.check("sccache --show-stats")
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        commands = [
            "sccache --show-stats",
            "clickhouse-client --version",
        ]
        results.append(
            Result.from_commands_run(
                name="Check and Compress binary",
                command=commands,
                workdir=build_dir_normalized,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CONFIG in stages:
        commands = [
            f"mkdir -p {temp_dir}/etc/clickhouse-server",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {temp_dir}/etc/clickhouse-server/",
            f"./tests/config/install.sh {temp_dir}/etc/clickhouse-server {temp_dir}/etc/clickhouse-client --fast-test",
            # f"cp -a {current_directory}/programs/server/config.d/log_to_console.xml {temp_dir}/etc/clickhouse-server/config.d/",
            f"rm -f {temp_dir}/etc/clickhouse-server/config.d/secure_ports.xml",
            update_path_ch_config,
        ]
        results.append(
            Result.from_commands_run(
                name="Install ClickHouse Config",
                command=commands,
            )
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc(
        ch_config_dir=f"{temp_dir}/etc/clickhouse-server",
        ch_var_lib_dir=f"{temp_dir}/var/lib/clickhouse",
    )
    CH.install_configs()

    attach_debug = False
    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        res = CH.start()
        res = res and CH.wait_ready()
        results.append(
            Result.create_from(name=step_name, status=res, stopwatch=stop_watch_)
        )
        if not results[-1].is_ok():
            attach_debug = True

    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)

        # Fast test runs lightweight SQL tests that are not CPU-bound,
        # so we can use more parallelism than the default cpu_count/2.
        nproc_fast = max(1, int(Utils.cpu_count() * 3 / 4))

        fast_test_command = f"cd {temp_dir} && clickhouse-test --hung-check --trace --capture-client-stacktrace --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --check-zookeeper-session --order random --report-logs-stats --fast-tests-only --no-stateful --timeout 60 --jobs {nproc_fast}"
        if args.skip:
            skip_args = " ".join(args.skip)
            fast_test_command += f" --skip {skip_args}"
        if args.test:
            test_pattern = "|".join(args.test)
            fast_test_command += f" -- '{test_pattern}'"

        res = CH.run_test(fast_test_command)

        test_results = FTResultsProcessor(wd=Settings.OUTPUT_DIR).run()
        if not res:
            test_results.results.append(
                Result.create_from(
                    name="clickhouse-test",
                    status=Result.Status.FAIL,
                    info="clickhouse-test error",
                )
            )
            attach_debug = True

        results.append(test_results)
        results[-1].set_timing(stopwatch=stop_watch_)
        if not results[-1].is_ok():
            attach_debug = True
        job_info = results[-1].info

    if attach_debug:
        attach_files += [
            clickhouse_bin_path,
            *CH.prepare_logs(info=info, all=True),
        ]

    CH.terminate(force=True)

    status = Result.Status.OK if args.set_status_success else ""
    Result.create_from(
        results=results, status=status, stopwatch=stop_watch, files=attach_files, info=job_info
    ).complete_job()


if __name__ == "__main__":
    main()
