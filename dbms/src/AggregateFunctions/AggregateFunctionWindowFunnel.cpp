#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWindowFunnel.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionWindowFunnel(const std::string & name, const DataTypes & arguments, const Array & params)
{

    if (params.size() <= 0 || params.size() > 32)
        throw Exception("Aggregate function " + name + " requires (1, 32] event ids.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    return std::make_shared<AggregateFunctionWindowFunnel>(arguments, params);
}

}

void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("windowFunnel", createAggregateFunctionWindowFunnel, AggregateFunctionFactory::CaseInsensitive);
}

}
