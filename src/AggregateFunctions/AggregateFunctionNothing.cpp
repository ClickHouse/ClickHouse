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

        auto result_type = argument_types.empty() ? std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()) : argument_types.front();
        return std::make_shared<AggregateFunctionNothing>(argument_types, parameters, result_type);
    });
}

}
