#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IBinaryAggregateFunction.h>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_INSERT_AT_MAX_SIZE 0xFFFFFF


namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}


/** Aggregate function, that takes two arguments: value and position,
  *  and as a result, builds an array with values are located at corresponding positions.
  *
  * If more than one value was inserted to single position, the any value (first in case of single thread) is stored.
  * If no values was inserted to some position, then default value will be substituted.
  *
  * Default value is optional parameter for aggregate function.
  */


/// Generic case (inefficient).
struct AggregateFunctionGroupArrayInsertAtDataGeneric
{
    Array value;    /// TODO Add MemoryTracker
};


class AggregateFunctionGroupArrayInsertAtGeneric final
    : public IBinaryAggregateFunction<AggregateFunctionGroupArrayInsertAtDataGeneric, AggregateFunctionGroupArrayInsertAtGeneric>
{
private:
    DataTypePtr type;
    Field default_value;

public:
    String getName() const override { return "groupArrayInsertAt"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        type = arguments.front();
    }

    void setParameters(const Array & params) override
    {
        if (params.empty())
        {
            default_value = type->getDefault();
            return;
        }

        if (params.size() != 1)
            throw Exception("Aggregate function " + getName() + " requires at most one parameter.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        default_value = params.front();
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_value, const IColumn & column_position, size_t row_num, Arena *) const
    {
        /// TODO Do positions need to be 1-based for this function?
        size_t position = column_position.get64(row_num);
        if (position >= AGGREGATE_FUNCTION_GROUP_ARRAY_INSERT_AT_MAX_SIZE)
            throw Exception("Too large array size: position argument (" + toString(position) + ")"
                " is greater or equals to limit (" + toString(AGGREGATE_FUNCTION_GROUP_ARRAY_INSERT_AT_MAX_SIZE) + ")",
                ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        Array & arr = data(place).value;

        if (arr.size() <= position)
            arr.resize(position + 1);
        else if (!arr[position].isNull())
            return; /// Element was already inserted to the specified position.

        column_value.get(row_num, arr[position]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        Array & arr_lhs = data(place).value;
        const Array & arr_rhs = data(rhs).value;

        if (arr_lhs.size() < arr_rhs.size())
            arr_lhs.resize(arr_rhs.size());

        for (size_t i = 0, size = arr_lhs.size(); i < size; ++i)
            if (arr_lhs[i].isNull() && !arr_rhs[i].isNull())
                arr_lhs[i] = arr_rhs[i];
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const Array & arr = data(place).value;
        size_t size = arr.size();
        writeVarUInt(size, buf);
        for (const Field & elem : arr)
        {
            if (elem.isNull())
            {
                writeBinary(UInt8(1), buf);
            }
            else
            {
                writeBinary(UInt8(0), buf);
                type->serializeBinary(elem, buf);
            }
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (size > AGGREGATE_FUNCTION_GROUP_ARRAY_INSERT_AT_MAX_SIZE)
            throw Exception("Too large array size", ErrorCodes::TOO_LARGE_ARRAY_SIZE);

        Array & arr = data(place).value;

        arr.resize(size);
        for (size_t i = 0; i < size; ++i)
        {
            UInt8 is_null = 0;
            readBinary(is_null, buf);
            if (!is_null)
                type->deserializeBinary(arr[i], buf);
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        ColumnArray & to_array = static_cast<ColumnArray &>(to);
        IColumn & to_data = to_array.getData();
        ColumnArray::Offsets_t & to_offsets = to_array.getOffsets();

        const Array & arr = data(place).value;

        for (const Field & elem : arr)
        {
            if (!elem.isNull())
                to_data.insert(elem);
            else
                to_data.insert(default_value);
        }

        to_offsets.push_back(arr.size());
    }
};


#undef AGGREGATE_FUNCTION_GROUP_ARRAY_INSERT_AT_MAX_SIZE

}
