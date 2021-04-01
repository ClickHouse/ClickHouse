#pragma once

#include <AggregateFunctions/AggregateFunctionMinMaxAny.h>
#include <AggregateFunctions/AggregateFunctionArgMinMax.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/// min, max, any, anyLast, anyHeavy, etc...
template <template <typename> class AggregateFunctionTemplate, template <typename> class Data>
static IAggregateFunction * createAggregateFunctionSingleValue(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    const DataTypePtr & argument_type = argument_types[0];

    WhichDataType which(argument_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) return new AggregateFunctionTemplate<Data<SingleValueDataFixed<TYPE>>>(argument_type);
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDate::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DataTypeDateTime::FieldType>>>(argument_type);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionTemplate<Data<SingleValueDataFixed<DateTime64>>>(argument_type);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionTemplate<Data<SingleValueDataString>>(argument_type);

    return new AggregateFunctionTemplate<Data<SingleValueDataGeneric>>(argument_type);
}


/// argMin, argMax
template <template <typename> class MinMaxData, typename ResData>
static IAggregateFunction * createAggregateFunctionArgMinMaxSecond(const DataTypePtr & res_type, const DataTypePtr & val_type, bool is_tuple)
{
    WhichDataType which(val_type);

#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<TYPE>>>>(res_type, val_type, is_tuple);
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDate::FieldType>>>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DataTypeDateTime::FieldType>>>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::DateTime64)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataFixed<DateTime64>>>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::String)
        return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataString>>>(res_type, val_type, is_tuple);

    return new AggregateFunctionArgMinMax<AggregateFunctionArgMinMaxData<ResData, MinMaxData<SingleValueDataGeneric>>>(res_type, val_type, is_tuple);

}

template <template <typename> class MinMaxData>
static IAggregateFunction * createAggregateFunctionArgMinMax(const String & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);

    DataTypePtr res_type, val_type;
    bool is_tuple = false;

    // argMin and argMax could get tuple of two as arguments
    if (argument_types.size() == 1 && argument_types[0]->getTypeId() == TypeIndex::Tuple)
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(argument_types[0].get());

        if (tuple_type->getElements().size() != 2)
        {
            throw Exception("Aggregate function " + name + " expects two elements in tuple argument", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }

        res_type = tuple_type->getElements()[0];
        val_type = tuple_type->getElements()[1];
        is_tuple = true;
     }
     else
     {
        assertBinary(name, argument_types);
        res_type = argument_types[0];
        val_type = argument_types[1];
     }

    WhichDataType which(res_type);
#define DISPATCH(TYPE) \
    if (which.idx == TypeIndex::TYPE) \
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<TYPE>>(res_type, val_type, is_tuple);
    FOR_NUMERIC_TYPES(DISPATCH)
    FOR_DECIMAL_TYPES(DISPATCH)
#undef DISPATCH

    if (which.idx == TypeIndex::Date)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDate::FieldType>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::DateTime)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DataTypeDateTime::FieldType>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::DateTime64)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataFixed<DateTime64>>(res_type, val_type, is_tuple);
    if (which.idx == TypeIndex::String)
        return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataString>(res_type, val_type, is_tuple);

    return createAggregateFunctionArgMinMaxSecond<MinMaxData, SingleValueDataGeneric>(res_type, val_type, is_tuple);
}

}
