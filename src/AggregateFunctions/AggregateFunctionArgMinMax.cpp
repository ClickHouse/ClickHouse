#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/HelpersArgMinMaxAny.h>

#include <AggregateFunctions/AggregateFunctionArgMinMax.h>


namespace DB
{
struct Settings;

namespace
{

    AggregateFunctionPtr createAggregateFunctionArgMin(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters, settings));
    }

    AggregateFunctionPtr createAggregateFunctionArgMax(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMaxData>(name, argument_types, parameters, settings));
    }

}

void registerAggregateFunctionsArgMinMax(AggregateFunctionFactory & factory)
{
    /// The functions depend on the order of data.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("argMin", {createAggregateFunctionArgMin, properties});
    factory.registerFunction("argMax", {createAggregateFunctionArgMax, properties});
}

}
