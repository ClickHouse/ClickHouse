#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

struct Settings;

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    factory.registerFunction(NameAggregateFunctionNothing::name,
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothing>(argument_types, parameters);
        });

    factory.registerFunction(NameAggregateFunctionNothingNull::name,
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothingNull>(argument_types, parameters);
        });


    factory.registerFunction(NameAggregateFunctionNothingUInt64::name, {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothingUInt64>(argument_types, parameters);
        },
        AggregateFunctionProperties{ .returns_default_when_only_null = true }
    });
}

}
