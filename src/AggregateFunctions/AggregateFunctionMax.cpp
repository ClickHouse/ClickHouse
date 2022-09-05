#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMinMax.h>

namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionMax(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionMinMax<ComparatorMax>(name, argument_types, parameters, settings));
    }
}

void registerAggregateFunctionsMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);
}

}
