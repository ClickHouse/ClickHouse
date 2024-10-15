#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_aggregate_partitions_independently;
    extern const SettingsBool force_optimize_projection;
    extern const SettingsString force_optimize_projection_name;
    extern const SettingsBool optimize_aggregation_in_order;
    extern const SettingsBool optimize_distinct_in_order;
    extern const SettingsBool optimize_read_in_order;
    extern const SettingsBool optimize_use_implicit_projections;
    extern const SettingsBool optimize_use_projections;
    extern const SettingsBool query_plan_aggregation_in_order;
    extern const SettingsBool query_plan_convert_outer_join_to_inner_join;
    extern const SettingsBool query_plan_enable_optimizations;
    extern const SettingsBool query_plan_execute_functions_after_sorting;
    extern const SettingsBool query_plan_filter_push_down;
    extern const SettingsBool query_plan_lift_up_array_join;
    extern const SettingsBool query_plan_lift_up_union;
    extern const SettingsUInt64 query_plan_max_optimizations_to_apply;
    extern const SettingsBool query_plan_merge_expressions;
    extern const SettingsBool query_plan_merge_filters;
    extern const SettingsBool query_plan_optimize_prewhere;
    extern const SettingsBool query_plan_push_down_limit;
    extern const SettingsBool query_plan_read_in_order;
    extern const SettingsBool query_plan_remove_redundant_distinct;
    extern const SettingsBool query_plan_remove_redundant_sorting;
    extern const SettingsBool optimize_sorting_by_input_stream_properties;
    extern const SettingsBool query_plan_reuse_storage_ordering_for_window_functions;
    extern const SettingsBool query_plan_split_filter;
}

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromSettings(const Settings & from)
{
    QueryPlanOptimizationSettings settings;

    settings.optimize_plan = from[Setting::query_plan_enable_optimizations];
    settings.max_optimizations_to_apply = from[Setting::query_plan_max_optimizations_to_apply];

    settings.lift_up_array_join = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_lift_up_array_join];

    settings.push_down_limit = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_push_down_limit];

    settings.split_filter = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_split_filter];

    settings.merge_expressions = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_merge_expressions];

    settings.merge_filters = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_merge_filters];

    settings.filter_push_down = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_filter_push_down];

    settings.convert_outer_join_to_inner_join = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_convert_outer_join_to_inner_join];

    settings.optimize_prewhere = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_optimize_prewhere];

    settings.execute_functions_after_sorting = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_execute_functions_after_sorting];

    settings.reuse_storage_ordering_for_window_functions = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_reuse_storage_ordering_for_window_functions];

    settings.lift_up_union = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_lift_up_union];

    settings.distinct_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_distinct_in_order];

    settings.read_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_read_in_order] && from[Setting::query_plan_read_in_order];

    settings.aggregation_in_order = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_aggregation_in_order] && from[Setting::query_plan_aggregation_in_order];

    settings.remove_redundant_sorting = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_remove_redundant_sorting];

    settings.optimize_sorting_by_input_stream_properties = from[Setting::query_plan_enable_optimizations] && from[Setting::optimize_sorting_by_input_stream_properties];

    settings.aggregate_partitions_independently = from[Setting::query_plan_enable_optimizations] && from[Setting::allow_aggregate_partitions_independently];

    settings.remove_redundant_distinct = from[Setting::query_plan_enable_optimizations] && from[Setting::query_plan_remove_redundant_distinct];

    settings.optimize_projection = from[Setting::optimize_use_projections];
    settings.force_use_projection = settings.optimize_projection && from[Setting::force_optimize_projection];
    settings.force_projection_name = settings.optimize_projection ? from[Setting::force_optimize_projection_name].value : "";
    settings.optimize_use_implicit_projections = settings.optimize_projection && from[Setting::optimize_use_implicit_projections];

    return settings;
}

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
