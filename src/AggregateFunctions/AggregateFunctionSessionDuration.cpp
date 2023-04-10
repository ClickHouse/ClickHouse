#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSessionDuration.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>



namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionSessionDuration(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (arguments.size() != 2)
        throw Exception("Not enough event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    
    return std::make_shared<AggregateFunctionSessionDuration>(params);
}

}

void registerAggregateFunctionSessionDuration(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sessionDuration", createAggregateFunctionSessionDuration, AggregateFunctionFactory::CaseInsensitive);
}

}
