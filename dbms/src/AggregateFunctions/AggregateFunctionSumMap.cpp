#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionSumMap(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionSumMap>();
}

}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumMap", createAggregateFunctionSumMap);
}

}
