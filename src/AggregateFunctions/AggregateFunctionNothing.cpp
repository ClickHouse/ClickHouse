#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

struct Settings;

void registerAggregateFunctionNothing(AggregateFunctionFactory & factory);
void registerAggregateFunctionNothing(AggregateFunctionFactory & factory)
{
    factory.registerFunction(NameAggregateFunctionNothing::name, {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothing>(argument_types, parameters);
        },
        {.description = R"DOC(Internal aggregate function that accepts any arguments and returns a value of type Nothing; used as a placeholder, for example for the state of an aggregation over an empty set.)DOC", .category = FunctionDocumentation::Category::AggregateFunction}
    });

    factory.registerFunction(NameAggregateFunctionNothingNull::name, {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothingNull>(argument_types, parameters);
        },
        {.description = R"DOC(Internal aggregate function that accepts any arguments and returns a value of type Nullable(Nothing); the nullable variant of the nothing aggregate function.)DOC", .category = FunctionDocumentation::Category::AggregateFunction}
    });


    factory.registerFunction(NameAggregateFunctionNothingUInt64::name, {
        [](const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
        {
            assertNoParameters(name, parameters);
            return std::make_shared<AggregateFunctionNothingUInt64>(argument_types, parameters);
        },
        {.description = R"DOC(Internal aggregate function that accepts any arguments and returns a UInt64; a variant of the nothing aggregate function used where a default numeric value is required.)DOC", .category = FunctionDocumentation::Category::AggregateFunction},
        AggregateFunctionProperties{ .returns_default_when_only_null = true }
    });
}

}
