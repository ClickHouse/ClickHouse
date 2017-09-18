#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnVector.h>

#include <Core/FieldVisitors.h>
#include <AggregateFunctions/IBinaryAggregateFunction.h>
#include <Functions/FunctionHelpers.h>
#include <map>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

struct AggregateFunctionSumMapData
{
    std::map<Field, Field> merged_maps;
};

/** Aggregate function, that takes two arguments: keys and values, and as a result, builds an array of 2 arrays -
  * ordered keys and values summed up  by corresponding keys.
  *
  * This function is the most useful when using SummingMergeTree to sum Nested columns, which name ends in "Map".
  *
  * Example: sumMap(k, v) of:
  *  k           v
  *  [1,2,3]     [10,10,10]
  *  [3,4,5]     [10,10,10]
  *  [4,5,6]     [10,10,10]
  *  [6,7,8]     [10,10,10]
  *  [7,5,3]     [5,15,25]
  *  [8,9,10]    [20,20,20]
  * will return:
  *  [[1,2,3,4,5,6,7,8,9,10],[10,10,45,20,35,20,15,30,20,20]]
  */
class AggregateFunctionSumMap final : public IBinaryAggregateFunction<struct AggregateFunctionSumMapData, AggregateFunctionSumMap>
{
private:
    DataTypePtr type;
    DataTypePtr keys_type;
    DataTypePtr values_type;

public:
    String getName() const override { return "sumMap"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeArray>(type);
    }

    void setArgumentsImpl(const DataTypes & arguments)
    {
        if (2 != arguments.size())
            throw Exception("Aggregate function " + getName() + "require exactly two arguments of array type.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        keys_type = array_type->getNestedType();

        array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());
        if (!array_type)
            throw Exception("Second argument for function " + getName() + " must be an array.",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        values_type = array_type->getNestedType();

        type = arguments.front();
    }

    void setParameters(const Array & params) override
    {
        if (!params.empty())
            throw Exception("This instantiation of " + getName() + "aggregate function doesn't accept any parameters.",
                            ErrorCodes::LOGICAL_ERROR);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column_keys, const IColumn & column_values, size_t row_num, Arena *) const
    {
        Field field_keys;
        column_keys.get(row_num, field_keys);
        const auto & keys = field_keys.get<Array &>();

        Field field_values;
        column_values.get(row_num, field_values);
        const auto & values = field_values.get<Array &>();

        auto & merged_maps = this->data(place).merged_maps;

        if (keys.size() != values.size())
            throw Exception("Sizes of keys and values arrays do not match", ErrorCodes::LOGICAL_ERROR);

        size_t size = keys.size();

        for (size_t i = 0; i < size; ++i)
        {
            if (merged_maps.find(keys[i]) != merged_maps.end())
                applyVisitor(FieldVisitorSum(values[i]), merged_maps[keys[i]]);
            else
                merged_maps[keys[i]] = values[i];
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto &rhs_map : rhs_maps)
        {
            if (merged_maps.find(rhs_map.first) != merged_maps.end())
                applyVisitor(FieldVisitorSum(rhs_map.second), merged_maps[rhs_map.first]);
            else
                merged_maps[rhs_map.first] = rhs_map.second;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();
        writeVarUInt(size, buf);

        for (const auto &v : merged_maps)
        {
            keys_type->serializeBinary(v.first, buf);
            values_type->serializeBinary(v.second, buf);
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;

        size_t size = 0;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            Field key, value;
            keys_type->deserializeBinary(key, buf);
            values_type->deserializeBinary(value, buf);
            merged_maps[key] = value;
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & to_array = static_cast<ColumnArray &>(to);
        auto & to_data = to_array.getData();
        auto & to_offsets = to_array.getOffsets();

        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();

        Array keys, values;
        keys.reserve(size);
        values.reserve(size);
        for (const auto &v : merged_maps)
        {
            keys.push_back(v.first);
            values.push_back(v.second);
        }

        to_data.insert(keys);
        to_data.insert(values);
        to_offsets.push_back((to_offsets.empty() ? 0 : to_offsets.back()) + 2);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
