#pragma once

#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/SingleValueData.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>

namespace DB
{
struct Settings;

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate>
static IAggregateFunction *
createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<SingleValueDataFixed<TYPE>>(argument_type); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDate::FieldType>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<SingleValueDataFixed<DataTypeDateTime::FieldType>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<SingleValueDataString>(argument_type);

    return new AggregateFunctionTemplate<SingleValueDataGeneric>(argument_type);
}

/// Functions that inherit from SingleValueData*
/// singleValueOrNull
template <template <typename> class AggregateFunctionTemplate, template <typename> class ChildType>
static IAggregateFunction * createAggregateFunctionSingleValueComposite(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];
    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<TYPE>>>(argument_type); /// NOLINT
    FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<ChildType<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<ChildType<SingleValueDataGeneric>>(argument_type);
}


/// For possible values for template parameters, see 'AggregateFunctionMinMaxAny.h'.
struct AggregateFunctionArgMinMaxData
{
    using Data = std::unique_ptr<SingleValueDataBase>;

    static Data generateSingleValueFromTypeIndex(TypeIndex idx)
    {
#define DISPATCH(TYPE) \
    if (idx == TypeIndex::TYPE) \
        return std::make_unique<SingleValueDataFixed<TYPE>>();

        FOR_SINGLE_VALUE_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

        if (idx == TypeIndex::Date)
            return std::make_unique<SingleValueDataFixed<DataTypeDate::FieldType>>();
        if (idx == TypeIndex::DateTime)
            return std::make_unique<SingleValueDataFixed<DataTypeDateTime::FieldType>>();
        if (idx == TypeIndex::String)
            return std::make_unique<SingleValueDataString>();
        return std::make_unique<SingleValueDataGeneric>();
    }

    Data result; // the argument at which the minimum/maximum value is reached.
    Data value; // value for which the minimum/maximum is calculated.

    explicit AggregateFunctionArgMinMaxData()
    {
        result = nullptr;
        value = nullptr;
    }

    explicit AggregateFunctionArgMinMaxData(TypeIndex result_type, TypeIndex value_type)
    {
        result = generateSingleValueFromTypeIndex(result_type);
        value = generateSingleValueFromTypeIndex(value_type);
    }
};

template <template <typename> class AggregateFunctionTemplate>
static IAggregateFunction *
createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    return new AggregateFunctionTemplate<AggregateFunctionArgMinMaxData>(res_type, val_type);
}
}
