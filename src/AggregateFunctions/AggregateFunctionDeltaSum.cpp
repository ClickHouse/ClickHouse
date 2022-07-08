#include <AggregateFunctions/AggregateFunctionDeltaSum.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionDeltaSum(
    const String & name,
    const DataTypes & arguments,
    const Array & params,
    const Settings *)
{
    assertNoParameters(name, params);

    if (arguments.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    const DataTypePtr & data_type = arguments[0];

    if (isInteger(data_type) || isFloat(data_type))
        return AggregateFunctionPtr(createWithNumericType<AggregationFunctionDeltaSum>(
            *data_type, arguments, params));
    else
        throw Exception("Illegal type " + arguments[0]->getName() + " of argument for aggregate function " + name,
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}
}

void registerAggregateFunctionDeltaSum(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("deltaSum", { createAggregateFunctionDeltaSum, properties });
}

}
