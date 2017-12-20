#pragma once

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

/// min, max, any, anyLast
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

#define DISPATCH(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(argument_type.get())) return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>>(argument_type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(argument_type.get()))
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (typeid_cast<const DataTypeDateTime *>(argument_type.get()))
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (typeid_cast<const DataTypeString *>(argument_type.get()))
        return new AggregateFunctionTemplate<Data<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}


/// argMin, argMax
template <template <typename> class MinMaxData, typename ResData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type)
{
#define DISPATCH(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(val_type.get())) \
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<TYPE>>>>(res_type, val_type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(val_type.get()))
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>>(res_type, val_type);
    if (typeid_cast<const DataTypeDateTime*>(val_type.get()))
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>(res_type, val_type);
    if (typeid_cast<const DataTypeString*>(val_type.get()))
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataString>>>(res_type, val_type);

    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataGeneric>>>(res_type, val_type);
}

template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);

    const DataTypePtr & res_type = argument_types[0];
    const DataTypePtr & val_type = argument_types[1];

#define DISPATCH(TYPE) \
    if (typeid_cast<const DataType ## TYPE *>(res_type.get())) \
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<TYPE>>(res_type, val_type);
    FOR_NUMERIC_TYPES(DISPATCH)
#undef DISPATCH

    if (typeid_cast<const DataTypeDate *>(res_type.get()))
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(res_type, val_type);
    if (typeid_cast<const DataTypeDateTime*>(res_type.get()))
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(res_type, val_type);
    if (typeid_cast<const DataTypeString*>(res_type.get()))
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataString>(res_type, val_type);

    return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataGeneric>(res_type, val_type);
}

}
