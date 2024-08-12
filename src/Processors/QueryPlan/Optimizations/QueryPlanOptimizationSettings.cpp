#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromSettings(const Settings & from)
{
    QueryPlanOptimizationSettings settings;

    settings.optimize_plan = from.query_plan_enable_optimizations;
    settings.max_optimizations_to_apply = from.query_plan_max_optimizations_to_apply;

    settings.lift_up_array_join = from.query_plan_enable_optimizations && from.query_plan_lift_up_array_join;

    settings.push_down_limit = from.query_plan_enable_optimizations && from.query_plan_push_down_limit;

    settings.split_filter = from.query_plan_enable_optimizations && from.query_plan_split_filter;

    settings.merge_expressions = from.query_plan_enable_optimizations && from.query_plan_merge_expressions;

    settings.filter_push_down = from.query_plan_enable_optimizations && from.query_plan_filter_push_down;

    settings.optimize_prewhere = from.query_plan_enable_optimizations && from.query_plan_optimize_prewhere;

    settings.execute_functions_after_sorting = from.query_plan_enable_optimizations && from.query_plan_execute_functions_after_sorting;

    settings.reuse_storage_ordering_for_window_functions = from.query_plan_enable_optimizations && from.query_plan_reuse_storage_ordering_for_window_functions;

    settings.lift_up_union = from.query_plan_enable_optimizations && from.query_plan_lift_up_union;

    settings.distinct_in_order = from.query_plan_enable_optimizations && from.optimize_distinct_in_order;

    settings.read_in_order = from.query_plan_enable_optimizations && from.optimize_read_in_order && from.query_plan_read_in_order;

    settings.aggregation_in_order = from.query_plan_enable_optimizations && from.optimize_aggregation_in_order && from.query_plan_aggregation_in_order;

    settings.remove_redundant_sorting = from.query_plan_enable_optimizations && from.query_plan_remove_redundant_sorting;

    settings.aggregate_partitions_independently = from.query_plan_enable_optimizations && from.allow_aggregate_partitions_independently;

    settings.remove_redundant_distinct = from.query_plan_enable_optimizations && from.query_plan_remove_redundant_distinct;

    settings.optimize_projection = from.optimize_use_projections;
    settings.force_use_projection = settings.optimize_projection && from.force_optimize_projection;
    settings.force_projection_name = from.force_optimize_projection_name;
    settings.optimize_use_implicit_projections = settings.optimize_projection && from.optimize_use_implicit_projections;

    return settings;
}

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
