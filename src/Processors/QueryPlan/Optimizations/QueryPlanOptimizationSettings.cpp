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
    settings.filter_push_down = from.query_plan_filter_push_down;
    settings.optimize_read_in_window_order = from.optimize_read_in_window_order;
    return settings;
}

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
