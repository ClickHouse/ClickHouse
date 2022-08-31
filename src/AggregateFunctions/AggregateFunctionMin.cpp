#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMin.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/HelpersMinMaxAny.h>
#include <Columns/ColumnFixedString.h>


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

    /// TODO: Since some types have changed from generic to other agg types, and that means serialization changes
    /// we might need to add the version or revert those changes to match the old status
    AggregateFunctionPtr f = AggregateFunctionPtr(
        createWithNumericBasedAndDecimalType<AggregateFunctionMin, AggregateFunctionMinDataNumeric>(*argument_type, argument_type));
    if (f)
        return f;

    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::String)
        return AggregateFunctionPtr(new AggregateFunctionMin<String, AggregateFunctionMinDataString<ColumnString>>(argument_type));
    return AggregateFunctionPtr(new AggregateFunctionMin<String, AggregateFunctionMinDataGeneric>(argument_type));
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
