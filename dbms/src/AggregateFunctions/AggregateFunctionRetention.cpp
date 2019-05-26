#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionRetention.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionRetention(const std::string & name, const DataTypes & arguments, const Array & params)
{
    assertNoParameters(name, params);

    if (arguments.size() < 2)
        throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments.size() > AggregateFunctionRetentionData::max_events)
        throw Exception("Too many event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionRetention>(arguments);
}

}

void registerAggregateFunctionRetention(AggregateFunctionFactory & factory)
{
    factory.registerFunction("retention", createAggregateFunctionRetention, AggregateFunctionFactory::CaseInsensitive);
}

}
