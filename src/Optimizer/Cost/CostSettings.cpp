#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Optimizer/Cost/CostSettings.h>

namespace DB
{

Cost::Weight CostSettings::getCostWeight()
{
    return {cost_cpu_weight, cost_mem_weight, cost_net_weight};
}

CostSettings CostSettings::fromSettings(const Settings & from)
{
    CostSettings settings;
    settings.cost_cpu_weight = from.cost_cpu_weight;
    settings.cost_mem_weight = from.cost_mem_weight;
    settings.cost_net_weight = from.cost_net_weight;
    settings.cost_pre_sorting_operation_weight = from.cost_pre_sorting_operation_weight;
    settings.cost_merge_agg_uniq_calculation_weight = from.cost_merge_agg_uniq_calculation_weight;
    return settings;
}

CostSettings CostSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}
