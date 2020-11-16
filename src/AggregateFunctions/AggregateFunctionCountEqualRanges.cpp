#include <AggregateFunctions/AggregateFunctionCountEqualRanges.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionCountEqualRanges(
    const String & name,
    const DataTypes & arguments,
    const Array & params)
{
    assertNoParameters(name, params);

    // TODO: it is possible to support variadic like in AggregateFunctionUniq
    if (arguments.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name,
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionCountEqualRanges>(arguments, params);
}

}

void registerAggregateFunctionCountEqualRanges(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = true };

    factory.registerFunction("countEqualRanges", { createAggregateFunctionCountEqualRanges, properties });
}

}
