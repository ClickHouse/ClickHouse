#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

AggregateFunctionPtr createAggregateFunctionNothing(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    return std::make_shared<AggregateFunctionNothing>(argument_types, parameters);
}

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("nothing", { createAggregateFunctionNothing, properties });
}

}
