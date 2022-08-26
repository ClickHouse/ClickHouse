#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMin.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>


namespace DB
{
struct Settings;

namespace
{

AggregateFunctionPtr createAggregateFunctionMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    AggregateFunctionPtr f = AggregateFunctionPtr(createWithNumericBasedType<AggregateFunctionMin>(*argument_type, argument_type));
    if (f)
        return f;
    f.reset(createWithDecimalType<AggregateFunctionMin>(*argument_type, argument_type));
    if (f)
        return f;

    // TODO: Add String and generic type support to AggregateFunctionMin
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::String)
        return AggregateFunctionPtr(new AggregateFunctionsSingleValue<AggregateFunctionMinData<SingleValueDataString>>(argument_type));
    return AggregateFunctionPtr(new AggregateFunctionsSingleValue<AggregateFunctionMinData<SingleValueDataGeneric>>(argument_type));
}

AggregateFunctionPtr createAggregateFunctionArgMin(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    return AggregateFunctionPtr(createAggregateFunctionArgMinMax<AggregateFunctionMinData>(name, argument_types, parameters, settings));
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
