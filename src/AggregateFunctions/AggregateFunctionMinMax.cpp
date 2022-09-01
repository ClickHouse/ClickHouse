#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <base/types.h>

namespace DB
{
struct Settings;

namespace
{
    /// min, max
    template <template <typename> class Comparator>
    static IAggregateFunction *
    createAggregateFunctionExtreme(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertUnary(name, argument_types);

        const DataTypePtr & argument_type = argument_types[0];

        WhichDataType which(argument_type);
#define NUMERIC_DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionsMinMax<AggregateFunctionsMinMaxDataNumeric<Comparator<TYPE>>>(argument_type); /// NOLINT
        FOR_NUMERIC_TYPES(NUMERIC_DISPATCH)

        NUMERIC_DISPATCH(DateTime64)
        NUMERIC_DISPATCH(Decimal32)
        NUMERIC_DISPATCH(Decimal64)
        NUMERIC_DISPATCH(Decimal128)
#undef NUMERIC_DISPATCH

        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionsMinMax<AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDate::FieldType>>>(argument_type);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionsMinMax<AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDateTime::FieldType>>>(
                argument_type);
        if (which.idx == TypeIndex::String)
            return new AggregateFunctionsMinMax<AggregateFunctionsMinMaxDataString<Comparator<StringRef>>>(argument_type);
        return new AggregateFunctionsMinMax<AggregateFunctionsMinMaxDataGeneric<Comparator<Field>>>(argument_type);
    }

    AggregateFunctionPtr createAggregateFunctionMin(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionExtreme<ComparatorMin>(name, argument_types, parameters, settings));
    }

    AggregateFunctionPtr createAggregateFunctionMax(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionExtreme<ComparatorMax>(name, argument_types, parameters, settings));
    }
}

void registerAggregateFunctionsMinMax(AggregateFunctionFactory & factory)
{
    factory.registerFunction("min", createAggregateFunctionMin, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("max", createAggregateFunctionMax, AggregateFunctionFactory::CaseInsensitive);
}

}
