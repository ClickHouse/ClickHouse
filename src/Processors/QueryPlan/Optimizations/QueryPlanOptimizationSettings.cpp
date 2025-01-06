#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_aggregate_partitions_independently;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsBool optimize_aggregation_in_order;
    extern const SettingsBool optimize_distinct_in_order;
    extern const SettingsBool optimize_read_in_order;
    extern const SettingsBool optimize_sorting_by_input_stream_properties;
    extern const SettingsBool optimize_use_implicit_projections;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool query_plan_aggregation_in_order;
    extern const SettingsBool query_plan_convert_outer_join_to_inner_join;
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
    extern const SettingsString force_optimize_projection_name;
    extern const SettingsUInt64 max_limit_for_ann_queries;
    extern const SettingsUInt64 query_plan_max_optimizations_to_apply;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool allow_experimental_analyzer;
}

QueryPlanOptimizationSettings::QueryPlanOptimizationSettings(const Settings & from)
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

    optimize_prewhere = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_optimize_prewhere];
    read_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_read_in_order] && from[Setting::query_plan_read_in_order];
    distinct_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_distinct_in_order];
    optimize_sorting_by_input_stream_properties = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_sorting_by_input_stream_properties];
    aggregation_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_aggregation_in_order] && from[Setting::query_plan_aggregation_in_order];
    optimize_projection = from[Setting::optimize_use_projections];

    optimize_use_implicit_projections = optimize_projection && from[Setting::optimize_use_implicit_projections];
    force_use_projection = optimize_projection && from[Setting::force_optimize_projection];
    force_projection_name = optimize_projection ? from[Setting::force_optimize_projection_name].value : "";

    max_limit_for_ann_queries = from[Setting::max_limit_for_ann_queries].value;

    use_query_condition_cache = from[Setting::use_query_condition_cache] && from[Setting::allow_experimental_analyzer];
}

QueryPlanOptimizationSettings::QueryPlanOptimizationSettings(ContextPtr from)
    : QueryPlanOptimizationSettings(from->getSettingsRef())
{
}

}
