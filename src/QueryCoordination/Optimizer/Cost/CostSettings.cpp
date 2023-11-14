#include "CostSettings.h"
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

CostSettings CostSettings::fromSettings(const Settings & from)
{
    CostSettings settings;
    settings.cost_pre_sorting_operation_weight = from.cost_pre_sorting_operation_weight;
    settings.cost_merge_agg_uniq_calculation_weight = from.cost_merge_agg_uniq_calculation_weight;
    return settings;
}

CostSettings CostSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}

