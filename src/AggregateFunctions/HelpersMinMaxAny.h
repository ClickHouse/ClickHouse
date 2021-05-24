#pragma once

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>


namespace DB
{

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE, SINGLEVALUEDATATYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionTemplate<Data<SINGLEVALUEDATATYPE>>(argument_type); // NOLINT
#define DISPATCH_FIXED(TYPE) DISPATCH(TYPE, SingleValueDataFixed<TYPE>)

    FOR_NUMERIC_TYPES(DISPATCH_FIXED)
    DISPATCH(Date, SingleValueDataFixed<DataTypeDate::FieldType>)
    DISPATCH(DateTime, SingleValueDataFixed<DataTypeDateTime::FieldType>)
    DISPATCH_FIXED(DateTime64)
    DISPATCH_FIXED(Decimal32)
    DISPATCH_FIXED(Decimal64)
    DISPATCH_FIXED(Decimal128)
    DISPATCH(String, SingleValueDataString)

#undef DISPATCH_FIXED
#undef DISPATCH

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}


template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

    WhichDataType which(res_type);
#define DISPATCH(TYPE, SINGLEVALUEDATATYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<SINGLEVALUEDATATYPE, MinMaxData<SINGLEVALUEDATATYPE>>>(res_type, val_type); // NOLINT
#define DISPATCH_FIXED(TYPE) DISPATCH(TYPE, SingleValueDataFixed<TYPE>)

    FOR_NUMERIC_TYPES(DISPATCH_FIXED)
    DISPATCH(Date, SingleValueDataFixed<DataTypeDate::FieldType>)
    DISPATCH(DateTime, SingleValueDataFixed<DataTypeDateTime::FieldType>)
    DISPATCH_FIXED(DateTime64)
    DISPATCH_FIXED(Decimal32)
    DISPATCH_FIXED(Decimal64)
    DISPATCH_FIXED(Decimal128)
    DISPATCH(String, SingleValueDataString)

#undef DISPATCH_FIXED
#undef DISPATCH

    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<SingleValueDataGeneric, MinMaxData<SingleValueDataGeneric>>>(
        res_type, val_type);
}

}
