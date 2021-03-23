#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromSettings(const Settings & from)
{
    QueryPlanOptimizationSettings settings;
    settings.max_optimizations_to_apply = from.query_plan_max_optimizations_to_apply;
    return settings;
}

QueryPlanOptimizationSettings QueryPlanOptimizationSettings::fromContext(const Context & from)
{
    return fromSettings(from.getSettingsRef());
}

}
