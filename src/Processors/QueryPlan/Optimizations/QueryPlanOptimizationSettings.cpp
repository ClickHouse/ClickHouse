#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Core/Settings.h>

namespace DB
{

QueryPlanOptimizationSettings::QueryPlanOptimizationSettings(const Settings & settings)
{
    max_optimizations_to_apply = settings.query_plan_max_optimizations_to_apply;
}

}
