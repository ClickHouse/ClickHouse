#include "StatisticsSettings.h"
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

StatisticsSettings StatisticsSettings::fromSettings(const Settings & from)
{
    StatisticsSettings settings;
    settings.statistics_agg_unknown_column_first_key_coefficient = from.statistics_agg_unknown_column_first_key_coefficient;
    settings.statistics_agg_unknown_column_rest_key_coefficient = from.statistics_agg_unknown_column_rest_key_coefficient;
    settings.statistics_agg_full_cardinality_coefficient = from.statistics_agg_full_cardinality_coefficient;
    return settings;
}

StatisticsSettings StatisticsSettings::fromContext(ContextPtr from)
{
    return fromSettings(from->getSettingsRef());
}

}


