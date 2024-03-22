#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRetention.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionRetention(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Not enough event arguments for aggregate function {}", name);

    if (arguments.size() > AggregateFunctionRetentionData::max_events)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Too many event arguments for aggregate function {}", name);

    return std::make_shared<AggregateFunctionRetention>(arguments);
}

}

void registerAggregateFunctionRetention(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retention", createAggregateFunctionRetention);
}

}
