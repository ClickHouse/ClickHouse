#pragma once

#include <AggregateFunctions/SingleValueData.h>
//#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

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

///// argMin, argMax
//template <template <typename> class MinMaxData, typename ResData>
//static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type)
//{
//    WhichDataType which(val_type);
//
//#define DISPATCH(TYPE) \
//    if (which.idx == TypeIndex::TYPE) \
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<TYPE>>>>(res_type, val_type); /// NOLINT
//    FOR_NUMERIC_TYPES(DISPATCH)
//#undef DISPATCH
//
//    if (which.idx == TypeIndex::Date)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::DateTime)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::DateTime64)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DateTime64>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal32)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal32>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal64)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal64>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal128)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal128>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal256)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<Decimal256>>>>(res_type, val_type);
//    if (which.idx == TypeIndex::String)
//        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataString>>>(res_type, val_type);
//
//    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataGeneric>>>(res_type, val_type);
//}

//template <template <typename> class MinMaxData>
//static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
//{
//    assertNoParameters(name, parameters);
//    assertBinary(name, argument_types);
//
//    const DataTypePtr & res_type = argument_types[0];
//    const DataTypePtr & val_type = argument_types[1];
//
//    WhichDataType which(res_type);
//#define DISPATCH(TYPE) \
//    if (which.idx == TypeIndex::TYPE) \
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<TYPE>>(res_type, val_type); /// NOLINT
//    FOR_NUMERIC_TYPES(DISPATCH)
//#undef DISPATCH
//
//    if (which.idx == TypeIndex::Date)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(res_type, val_type);
//    if (which.idx == TypeIndex::DateTime)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(res_type, val_type);
//    if (which.idx == TypeIndex::DateTime64)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DateTime64>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal32)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal32>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal64)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal64>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal128)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal128>>(res_type, val_type);
//    if (which.idx == TypeIndex::Decimal256)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<Decimal256>>(res_type, val_type);
//    if (which.idx == TypeIndex::String)
//        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataString>(res_type, val_type);
//
//    return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataGeneric>(res_type, val_type);
//}
}
