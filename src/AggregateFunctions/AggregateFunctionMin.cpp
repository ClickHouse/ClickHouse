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

    WhichDataType which(argument_type);
#define NUMERIC_DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return AggregateFunctionPtr(new AggregateFunctionMin<AggregateFunctionMinDataNumeric<TYPE>>(argument_type));
    FOR_NUMERIC_TYPES(NUMERIC_DISPATCH)

    NUMERIC_DISPATCH(DateTime64)
    NUMERIC_DISPATCH(Decimal32)
    NUMERIC_DISPATCH(Decimal64)
    NUMERIC_DISPATCH(Decimal128)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return AggregateFunctionPtr(new AggregateFunctionMin<AggregateFunctionMinDataNumeric<DataTypeDate::FieldType>>(argument_type));
    if (which.idx == TypeIndex::DateTime)
        return AggregateFunctionPtr(new AggregateFunctionMin<AggregateFunctionMinDataNumeric<DataTypeDateTime::FieldType>>(argument_type));
    if (which.idx == TypeIndex::String)
        return AggregateFunctionPtr(new AggregateFunctionMin<AggregateFunctionMinDataString>(argument_type));
    return AggregateFunctionPtr(new AggregateFunctionMin<AggregateFunctionMinDataGeneric>(argument_type));
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
