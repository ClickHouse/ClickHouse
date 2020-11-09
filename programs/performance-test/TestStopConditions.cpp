#include "TestStopConditions.h"

namespace DB
{

void TestStopConditions::loadFromConfig(ConfigurationPtr & stop_conditions_config)
{
    if (stop_conditions_config->has("all_of"))
    {
        ConfigurationPtr config_all_of(stop_conditions_config->createView("all_of"));
        conditions_all_of.loadFromConfig(config_all_of);
    }
    if (stop_conditions_config->has("any_of"))
    {
        ConfigurationPtr config_any_of(stop_conditions_config->createView("any_of"));
        conditions_any_of.loadFromConfig(config_any_of);
    }
}

bool TestStopConditions::areFulfilled() const
{
    return (conditions_all_of.initialized_count && conditions_all_of.fulfilled_count >= conditions_all_of.initialized_count)
        || (conditions_any_of.initialized_count && conditions_any_of.fulfilled_count);
}

UInt64 TestStopConditions::getMaxExecTime() const
{
    UInt64 all_of_time = conditions_all_of.total_time_ms.value;
    if (all_of_time == 0 && conditions_all_of.initialized_count != 0) /// max time is not set in all conditions
        return 0;
    else if(all_of_time != 0 && conditions_all_of.initialized_count > 1) /// max time is set, but we have other conditions
        return 0;

    UInt64 any_of_time = conditions_any_of.total_time_ms.value;
    return std::max(all_of_time, any_of_time);
}

}
