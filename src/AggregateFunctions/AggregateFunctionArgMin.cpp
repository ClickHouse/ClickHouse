#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionArgMin(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionArgMinMax<ComparatorMin>(name, argument_types, parameters, settings));
    }
}

void registerAggregateFunctionsArgMin(AggregateFunctionFactory & factory)
{
    /// The functions depend on the order of data.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("argMin", {createAggregateFunctionArgMin, properties});
}

}
