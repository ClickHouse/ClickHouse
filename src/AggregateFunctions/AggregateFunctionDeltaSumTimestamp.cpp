#include <AggregateFunctions/AggregateFunctionDeltaSumTimestamp.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionDeltaSumTimestamp(
    const String & name,
    const DataTypes & arguments,
    const Array & params)
{
    assertNoParameters(name, params);

    if (arguments.size() != 2)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (!isInteger(arguments[0]) && !isFloat(arguments[0]) && !isDateOrDateTime(arguments[0]))
        throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " +
            name + ", must be Int, Float, Date, DateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    if (!isInteger(arguments[1]) && !isFloat(arguments[1]) && !isDateOrDateTime(arguments[1]))
        throw Exception("Illegal type " + arguments[1]->getName() + " of argument for aggregate function " +
            name + ", must be Int, Float, Date, DateTime", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return AggregateFunctionPtr(createWithTwoNumericOrDateTypes<AggregationFunctionDeltaSumTimestamp>(
        *arguments[0], *arguments[1], arguments, params));
}
}

void registerAggregateFunctionDeltaSumTimestamp(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("deltaSumTimestamp", { createAggregateFunctionDeltaSumTimestamp, properties });
}

}
