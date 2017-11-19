#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSumMap.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionSumMap(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() < 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name + ", should be at least 2",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const auto * array_type = checkAndGetDataType<DataTypeArray>(argument_types[0].get());
    if (!array_type)
        throw Exception("First argument for function " + name + " must be an array.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return AggregateFunctionPtr(createWithNumericType<AggregateFunctionSumMap>(*array_type->getNestedType()));
}

}

void registerAggregateFunctionSumMap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sumMap", createAggregateFunctionSumMap);
}

}
