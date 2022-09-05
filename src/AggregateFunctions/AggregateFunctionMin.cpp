#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMinMax.h>

namespace DB
{

namespace
{
    AggregateFunctionPtr createAggregateFunctionMin(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionMinMax<ComparatorMin>(name, argument_types, parameters, settings));
    }
}

void registerAggregateFunctionsMin(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);
}

}
