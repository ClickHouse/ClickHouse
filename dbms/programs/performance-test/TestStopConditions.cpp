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

}
