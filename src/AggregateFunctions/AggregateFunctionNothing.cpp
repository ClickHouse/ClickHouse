#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

struct Settings;

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    factory.registerFunction("nothing", [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        return std::make_shared<AggregateFunctionNothing>(argument_types, parameters);
    });
}

}
