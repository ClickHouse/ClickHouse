#include "TestStopConditions.h"

namespace DB
{

void ConnectionTestStopConditions::loadFromConfig(ConfigurationPtr & stop_conditions_config)
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

bool ConnectionTestStopConditions::areFulfilled() const
{
    return (conditions_all_of.initialized_count && conditions_all_of.fulfilled_count >= conditions_all_of.initialized_count)
        || (conditions_any_of.initialized_count && conditions_any_of.fulfilled_count);
}

UInt64 ConnectionTestStopConditions::getMaxExecTime() const
{
    const StopCondition & all_of_time = conditions_all_of.total_time_ms;
    const StopCondition & any_of_time = conditions_any_of.total_time_ms;

    if (any_of_time.initialized || (all_of_time.initialized && conditions_all_of.initialized_count == 1))
        return std::max(all_of_time.value, any_of_time.value);

    return 0;

}

}

