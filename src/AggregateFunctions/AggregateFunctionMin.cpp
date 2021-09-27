#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionMin(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionSingleValue<AggregateFunctionsSingleValue, AggregateFunctionMinData>(name, argument_types, parameters));
}

AggregateFunctionPtr createAggregateFunctionArgMin(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters));
}

}

void registerAggregateFunctionsMin(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);

    /// The functions below depend on the order of data.
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };
    factory.registerFunction("argMin", { createAggregateFunctionArgMin, properties });
}

}
