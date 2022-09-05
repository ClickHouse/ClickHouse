#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
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
    template <template <typename> class Comparator, typename ResultData>
    static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type)
    {
        WhichDataType which(val_type);

#define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) \
            return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResultData, AggregateFunctionsMinMaxDataNumeric<Comparator<TYPE>>>>(res_type, val_type); /// NOLINT

        FOR_NUMERIC_TYPES(DISPATCH)
        DISPATCH(DateTime64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)

#undef DISPATCH

        if (which.idx == TypeIndex::Date)
            return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResultData, AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDate::FieldType>>>>(res_type, val_type);
        if (which.idx == TypeIndex::DateTime)
            return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResultData, AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDateTime::FieldType>>>>(res_type, val_type);
        if (which.idx == TypeIndex::String)
            return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResultData, AggregateFunctionsMinMaxDataString<Comparator<StringRef>>>>(res_type, val_type);
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResultData, AggregateFunctionsMinMaxDataGeneric<Comparator<Field>>>>(res_type, val_type);
    }

    template <template <typename> class Comparator>
    static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);

        const DataTypePtr & res_type = argument_types[0];
        const DataTypePtr & val_type = argument_types[1];

        WhichDataType which(res_type);
#define DISPATCH(TYPE) \
        if (which.idx == TypeIndex::TYPE) \
            return createAggregateFunctionArgMinMaxSecond<Comparator, AggregateFunctionsMinMaxDataNumeric<Comparator<TYPE>>>(res_type, val_type); /// NOLINT

        FOR_NUMERIC_TYPES(DISPATCH)
        DISPATCH(DateTime64)
        DISPATCH(Decimal32)
        DISPATCH(Decimal64)
        DISPATCH(Decimal128)
#undef DISPATCH

        if (which.idx == TypeIndex::Date)
            return createAggregateFunctionArgMinMaxSecond<Comparator, AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDate::FieldType>>>(res_type, val_type);
        if (which.idx == TypeIndex::DateTime)
            return createAggregateFunctionArgMinMaxSecond<Comparator, AggregateFunctionsMinMaxDataNumeric<Comparator<DataTypeDateTime::FieldType>>>(res_type, val_type);
        if (which.idx == TypeIndex::String)
            return createAggregateFunctionArgMinMaxSecond<Comparator, AggregateFunctionsMinMaxDataString<Comparator<StringRef>>>(res_type, val_type);

        return createAggregateFunctionArgMinMaxSecond<Comparator, AggregateFunctionsMinMaxDataGeneric<Comparator<Field>>>(res_type, val_type);
    }


    AggregateFunctionPtr createAggregateFunctionArgMin(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionArgMinMax<ComparatorMin>(name, argument_types, parameters, settings));
    }

    AggregateFunctionPtr createAggregateFunctionArgMax(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
    {
        return AggregateFunctionPtr(createAggregateFunctionArgMinMax<ComparatorMax>(name, argument_types, parameters, settings));
    }

}

void registerAggregateFunctionsArgMinMax(AggregateFunctionFactory & factory)
{
    /// The functions depend on the order of data.
    AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};
    factory.registerFunction("argMin", {createAggregateFunctionArgMin, properties});
    factory.registerFunction("argMax", {createAggregateFunctionArgMax, properties});
}

}
