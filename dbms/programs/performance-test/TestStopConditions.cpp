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
    bool is_single_in_all_off = all_of_time.initialized && conditions_all_of.initialized_count == 1;

    /// max time is set in all_of and is single.
    if (!any_of_time.initialized && is_single_in_all_off)
        return all_of_time.value;

    /// max time is set in any_of and doesn't exist or single in all_of section
    if (any_of_time.initialized && !is_single_in_all_off)
        return any_of_time.value;

    /// max time is set in any_of and there is a single max time condition in all_of
    if (any_of_time.initialized && is_single_in_all_off)
        return std::max(any_of_time.value, all_of_time.value);

    return 0;
}

}
