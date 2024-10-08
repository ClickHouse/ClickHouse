#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct Settings;

AggregateFunctionPtr AggregateFunctionCount::getOwnNullAdapter(
    const AggregateFunctionPtr &, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const
{
    return std::make_shared<AggregateFunctionCountNotNullUnary>(types[0], params);
}

namespace
{

AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);

    if (argument_types.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires zero or one argument", name);

    return std::make_shared<AggregateFunctionCount>(argument_types);
}

}

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("count", {createAggregateFunctionCount, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
