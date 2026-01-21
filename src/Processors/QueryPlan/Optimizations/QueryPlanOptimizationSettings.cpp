#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_aggregate_partitions_independently;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsBool optimize_aggregation_in_order;
    extern const SettingsBool optimize_distinct_in_order;
    extern const SettingsBool optimize_read_in_order;
    extern const SettingsBool optimize_sorting_by_input_stream_properties;
    extern const SettingsBool optimize_use_implicit_projections;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool query_plan_aggregation_in_order;
    extern const SettingsBool query_plan_convert_outer_join_to_inner_join;
    extern const SettingsBool query_plan_convert_any_join_to_semi_or_anti_join;
    extern const SettingsBool query_plan_merge_filter_into_join_condition;
    extern const SettingsBool query_plan_enable_optimizations;
    extern const SettingsBool query_plan_execute_functions_after_sorting;
    extern const SettingsBool query_plan_filter_push_down;
    extern const SettingsBool query_plan_lift_up_array_join;
    extern const SettingsBool query_plan_lift_up_union;
    extern const SettingsBool query_plan_merge_expressions;
    extern const SettingsBool query_plan_merge_filters;
    extern const SettingsBool query_plan_optimize_prewhere;
    extern const SettingsBool query_plan_push_down_limit;
    extern const SettingsBool query_plan_read_in_order;
    extern const SettingsBool query_plan_remove_redundant_distinct;
    extern const SettingsBool query_plan_remove_redundant_sorting;
    extern const SettingsBool query_plan_reuse_storage_ordering_for_window_functions;
    extern const SettingsBool query_plan_split_filter;
    extern const SettingsBool query_plan_try_use_vector_search;
    extern const SettingsBool query_plan_convert_join_to_in;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool query_condition_cache_store_conditions_as_plaintext;
    extern const SettingsBool collect_hash_table_stats_during_joins;
    extern const SettingsBool query_plan_join_shard_by_pk_ranges;
    extern const SettingsBool query_plan_optimize_lazy_materialization;
    extern const SettingsBool vector_search_with_rescoring;
    extern const SettingsUInt64 query_plan_optimize_join_order_limit;
    extern const SettingsBoolAuto query_plan_join_swap_table;
    extern const SettingsMaxThreads max_threads;
    extern const SettingsOverflowMode transfer_overflow_mode;
    extern const SettingsSeconds lock_acquire_timeout;
    extern const SettingsString force_optimize_projection_name;
    extern const SettingsUInt64 max_bytes_to_transfer;
    extern const SettingsUInt64 max_limit_for_vector_search_queries;
    extern const SettingsUInt64 max_rows_to_transfer;
    extern const SettingsUInt64 max_size_to_preallocate_for_joins;
    extern const SettingsUInt64 query_plan_max_limit_for_lazy_materialization;
    extern const SettingsUInt64 query_plan_max_optimizations_to_apply;
    extern const SettingsUInt64 use_index_for_in_with_subqueries_max_values;
    extern const SettingsVectorSearchFilterStrategy vector_search_filter_strategy;
    extern const SettingsBool parallel_replicas_local_plan;
    extern const SettingsBool parallel_replicas_support_projection;
    extern const SettingsBool make_distributed_plan;
    extern const SettingsUInt64 distributed_plan_default_shuffle_join_bucket_count;
    extern const SettingsUInt64 distributed_plan_default_reader_bucket_count;
    extern const SettingsBool distributed_plan_optimize_exchanges;
    extern const SettingsString distributed_plan_force_exchange_kind;
    extern const SettingsUInt64 distributed_plan_max_rows_to_broadcast;
    extern const SettingsBool distributed_plan_force_shuffle_aggregation;
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsBool use_join_disjunctions_push_down;
    extern const SettingsBool enable_join_runtime_filters;
    extern const SettingsUInt64 join_runtime_filter_exact_values_limit;
    extern const SettingsUInt64 join_runtime_bloom_filter_bytes;
    extern const SettingsUInt64 join_runtime_bloom_filter_hash_functions;
    extern const SettingsBool query_plan_direct_read_from_text_index;
    extern const SettingsBool use_skip_indexes;
    extern const SettingsBool use_skip_indexes_on_data_read;
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_entries_for_hash_table_stats;
}

namespace ErrorCodes
{
    extern const int UNSUPPORTED_METHOD;
    extern const int INVALID_SETTING_VALUE;
}

QueryPlanOptimizationSettings::QueryPlanOptimizationSettings(
    const Settings & from,
    UInt64 max_entries_for_hash_table_stats_,
    String initial_query_id_,
    ExpressionActionsSettings actions_settings_,
    PreparedSetsCachePtr prepared_sets_cache_,
    bool is_parallel_replicas_initiator_with_projection_support_)
{
    optimize_plan = from[Setting::query_plan_enable_optimizations];
    max_optimizations_to_apply = from[Setting::query_plan_max_optimizations_to_apply];

    remove_redundant_sorting = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_remove_redundant_sorting];

    lift_up_array_join = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_lift_up_array_join];
    push_down_limit = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_push_down_limit];
    split_filter = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_split_filter];
    merge_expressions = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_merge_expressions];
    merge_filters = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_merge_filters];
    filter_push_down = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_filter_push_down];
    convert_outer_join_to_inner_join = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_convert_outer_join_to_inner_join];
    execute_functions_after_sorting = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_execute_functions_after_sorting];
    reuse_storage_ordering_for_window_functions = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_reuse_storage_ordering_for_window_functions];
    lift_up_union = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_lift_up_union];
    aggregate_partitions_independently = from[Setting::query_plan_enable_optimizations] && from[Setting::allow_aggregate_partitions_independently];
    remove_redundant_distinct = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_remove_redundant_distinct];
    try_use_vector_search = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_try_use_vector_search];
    convert_join_to_in = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_convert_join_to_in];
    merge_filter_into_join_condition = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_merge_filter_into_join_condition];
    convert_any_join_to_semi_or_anti_join = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_convert_any_join_to_semi_or_anti_join];

    bool use_parallel_replicas = from[Setting::allow_experimental_parallel_reading_from_replicas] && from[Setting::max_parallel_replicas] > 1;
    query_plan_optimize_join_order_limit = use_parallel_replicas ? 0 : from[Setting::query_plan_optimize_join_order_limit];
    if (query_plan_optimize_join_order_limit > 64)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "The value of the setting `query_plan_optimize_join_order_limit` is too large: {}, "
            "maximum allowed value is 64", query_plan_optimize_join_order_limit);

    join_swap_table = from[Setting::query_plan_join_swap_table].is_auto
        ? std::nullopt
        : std::make_optional(from[Setting::query_plan_join_swap_table].base);
    use_join_disjunctions_push_down = from[Setting::query_plan_enable_optimizations] && from[Setting::use_join_disjunctions_push_down];

    optimize_prewhere = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_optimize_prewhere];
    read_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_read_in_order] && from[Setting::query_plan_read_in_order];
    distinct_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_distinct_in_order];
    optimize_sorting_by_input_stream_properties = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_sorting_by_input_stream_properties];
    aggregation_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_aggregation_in_order] && from[Setting::query_plan_aggregation_in_order];
    optimize_projection = from[Setting::optimize_use_projections];
    use_query_condition_cache = from[Setting::use_query_condition_cache] && from[Setting::allow_experimental_analyzer];
    query_condition_cache_store_conditions_as_plaintext = from[Setting::query_condition_cache_store_conditions_as_plaintext];
    direct_read_from_text_index = from[Setting::query_plan_direct_read_from_text_index] && from[Setting::use_skip_indexes] && from[Setting::use_skip_indexes_on_data_read];

    optimize_use_implicit_projections = optimize_projection && from[Setting::optimize_use_implicit_projections];
    force_use_projection = optimize_projection && from[Setting::force_optimize_projection];
    force_projection_name = optimize_projection ? from[Setting::force_optimize_projection_name].value : "";
    is_parallel_replicas_initiator_with_projection_support = is_parallel_replicas_initiator_with_projection_support_;

    make_distributed_plan = from[Setting::make_distributed_plan];
    distributed_plan_default_shuffle_join_bucket_count = from[Setting::distributed_plan_default_shuffle_join_bucket_count];
    distributed_plan_default_reader_bucket_count = from[Setting::distributed_plan_default_reader_bucket_count];
    distributed_plan_optimize_exchanges = from[Setting::distributed_plan_optimize_exchanges];
#ifdef OS_LINUX
    distributed_plan_force_exchange_kind = from[Setting::distributed_plan_force_exchange_kind].value;
#else
    if (from[Setting::distributed_plan_force_exchange_kind].changed && from[Setting::distributed_plan_force_exchange_kind].value != "Persisted")
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Only Persisted exchange is supported");
    distributed_plan_force_exchange_kind = "Persisted";
#endif
    distributed_plan_max_rows_to_broadcast = from[Setting::distributed_plan_max_rows_to_broadcast];
    distributed_plan_force_shuffle_aggregation = from[Setting::distributed_plan_force_shuffle_aggregation];
    distributed_aggregation_memory_efficient = from[Setting::distributed_aggregation_memory_efficient];

    optimize_lazy_materialization = from[Setting::query_plan_optimize_lazy_materialization] && from[Setting::allow_experimental_analyzer];
    max_limit_for_lazy_materialization = from[Setting::query_plan_max_limit_for_lazy_materialization];

    max_limit_for_vector_search_queries = from[Setting::max_limit_for_vector_search_queries].value;
    vector_search_with_rescoring = from[Setting::vector_search_with_rescoring];
    vector_search_filter_strategy = from[Setting::vector_search_filter_strategy].value;

    query_plan_join_shard_by_pk_ranges = from[Setting::query_plan_join_shard_by_pk_ranges].value;

    network_transfer_limits = SizeLimits(from[Setting::max_rows_to_transfer], from[Setting::max_bytes_to_transfer], from[Setting::transfer_overflow_mode]);
    use_index_for_in_with_subqueries_max_values = from[Setting::use_index_for_in_with_subqueries_max_values];
    prepared_sets_cache = std::move(prepared_sets_cache_);

    /// These settings comes from EXPLAIN settings not query settings and outside of the scope of this class
    keep_logical_steps = false;
    is_explain = false;

    max_entries_for_hash_table_stats = max_entries_for_hash_table_stats_;
    max_size_to_preallocate_for_joins = from[Setting::max_size_to_preallocate_for_joins];
    collect_hash_table_stats_during_joins = from[Setting::collect_hash_table_stats_during_joins];
    initial_query_id = initial_query_id_;
    lock_acquire_timeout = from[Setting::lock_acquire_timeout];
    actions_settings = std::move(actions_settings_);

    enable_join_runtime_filters = from[Setting::query_plan_enable_optimizations] && from[Setting::enable_join_runtime_filters];
    join_runtime_filter_exact_values_limit = from[Setting::join_runtime_filter_exact_values_limit];
    join_runtime_bloom_filter_bytes = from[Setting::join_runtime_bloom_filter_bytes];
    join_runtime_bloom_filter_hash_functions = from[Setting::join_runtime_bloom_filter_hash_functions];

    max_threads = from[Setting::max_threads];
}

QueryPlanOptimizationSettings::QueryPlanOptimizationSettings(ContextPtr from)
    : QueryPlanOptimizationSettings(
        from->getSettingsRef(),
        from->getServerSettings()[ServerSetting::max_entries_for_hash_table_stats],
        from->getInitialQueryId(),
        ExpressionActionsSettings(from),
        from->getPreparedSetsCache(),
        from->canUseParallelReplicasOnInitiator()
            && from->getSettingsRef()[Setting::parallel_replicas_local_plan]
            && from->getSettingsRef()[Setting::parallel_replicas_support_projection])
{
}

}
