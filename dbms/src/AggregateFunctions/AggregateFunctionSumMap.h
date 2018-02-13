#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>

#include <Common/FieldVisitors.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename T>
struct AggregateFunctionSumMapData
{
    // Map needs to be ordered to maintain function properties
    std::map<T, Array> merged_maps;
};

/** Aggregate function, that takes at least two arguments: keys and values, and as a result, builds a tuple of of at least 2 arrays -
  * ordered keys and variable number of argument values summed up by corresponding keys.
  *
  * This function is the most useful when using SummingMergeTree to sum Nested columns, which name ends in "Map".
  *
  * Example: sumMap(k, v...) of:
  *  k           v
  *  [1,2,3]     [10,10,10]
  *  [3,4,5]     [10,10,10]
  *  [4,5,6]     [10,10,10]
  *  [6,7,8]     [10,10,10]
  *  [7,5,3]     [5,15,25]
  *  [8,9,10]    [20,20,20]
  * will return:
  *  ([1,2,3,4,5,6,7,8,9,10],[10,10,45,20,35,20,15,30,20,20])
  */

template <typename T>
class AggregateFunctionSumMap final : public IAggregateFunctionDataHelper<
    AggregateFunctionSumMapData<typename NearestFieldType<T>::Type>, AggregateFunctionSumMap<T>>
{
private:
    DataTypePtr keys_type;
    DataTypes values_types;

public:
    AggregateFunctionSumMap(const DataTypePtr & keys_type, const DataTypes & values_types)
        : keys_type(keys_type), values_types(values_types) {}

    String getName() const override { return "sumMap"; }

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        types.emplace_back(std::make_shared<DataTypeArray>(keys_type));

        for (const auto & value_type : values_types)
            types.emplace_back(std::make_shared<DataTypeArray>(value_type));

        return std::make_shared<DataTypeTuple>(types);
    }

    void add(AggregateDataPtr place, const IColumn ** columns, const size_t row_num, Arena *) const override
    {
        // Column 0 contains array of keys of known type
        const ColumnArray & array_column = static_cast<const ColumnArray &>(*columns[0]);
        const IColumn::Offsets & offsets = array_column.getOffsets();
        const auto & keys_vec = static_cast<const ColumnVector<T> &>(array_column.getData());
        const size_t keys_vec_offset = row_num == 0 ? 0 : offsets[row_num - 1];
        const size_t keys_vec_size = (offsets[row_num] - keys_vec_offset);

        // Columns 1..n contain arrays of numeric values to sum
        auto & merged_maps = this->data(place).merged_maps;
        for (size_t col = 0, size = values_types.size(); col < size; ++col)
        {
            Field value;
            const ColumnArray & array_column = static_cast<const ColumnArray &>(*columns[col + 1]);
            const IColumn::Offsets & offsets = array_column.getOffsets();
            const size_t values_vec_offset = row_num == 0 ? 0 : offsets[row_num - 1];
            const size_t values_vec_size = (offsets[row_num] - values_vec_offset);

            // Expect key and value arrays to be of same length
            if (keys_vec_size != values_vec_size)
                throw Exception("Sizes of keys and values arrays do not match", ErrorCodes::LOGICAL_ERROR);

            // Insert column values for all keys
            for (size_t i = 0; i < keys_vec_size; ++i)
            {
                array_column.getData().get(values_vec_offset + i, value);
                const auto & key = keys_vec.getData()[keys_vec_offset + i];
                const auto & it = merged_maps.find(key);

                if (it != merged_maps.end())
                    applyVisitor(FieldVisitorSum(value), it->second[col]);
                else
                {
                    // Create a value array for this key
                    Array new_values;
                    new_values.resize(values_types.size());
                    for (size_t k = 0; k < new_values.size(); ++k)
                        new_values[k] = (k == col) ? value : values_types[k]->getDefault();

                    merged_maps[key] = std::move(new_values);
                }
            }
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);
            if (it != merged_maps.end())
            {
                for (size_t col = 0; col < values_types.size(); ++col)
                    applyVisitor(FieldVisitorSum(elem.second[col]), it->second[col]);
            }
            else
                merged_maps[elem.first] = elem.second;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        const auto & merged_maps = this->data(place).merged_maps;
        size_t size = merged_maps.size();
        writeVarUInt(size, buf);

        for (const auto & elem : merged_maps)
        {
            keys_type->serializeBinary(elem.first, buf);
            for (size_t col = 0; col < values_types.size(); ++col)
                values_types[col]->serializeBinary(elem.second[col], buf);
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        size_t size = 0;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; ++i)
        {
            Field key;
            keys_type->deserializeBinary(key, buf);

            Array values;
            values.resize(values_types.size());
            for (size_t col = 0; col < values_types.size(); ++col)
                values_types[col]->deserializeBinary(values[col], buf);

            merged_maps[key.get<T>()] = values;
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        // Final step does compaction of keys that have zero values, this mutates the state
        auto & merged_maps = this->data(const_cast<AggregateDataPtr>(place)).merged_maps;
        for (auto it = merged_maps.cbegin(); it != merged_maps.cend();)
        {
            // Key is not compacted if it has at least one non-zero value
            bool erase = true;
            for (size_t col = 0; col < values_types.size(); ++col)
            {
                if (it->second[col] != values_types[col]->getDefault())
                {
                    erase = false;
                    break;
                }
            }

            if (erase)
                it = merged_maps.erase(it);
            else
                ++it;
        }

        size_t size = merged_maps.size();

        auto & to_tuple = static_cast<ColumnTuple &>(to);
        auto & to_keys_arr = static_cast<ColumnArray &>(to_tuple.getColumn(0));
        auto & to_keys_col = to_keys_arr.getData();

        // Advance column offsets
        auto & to_keys_offsets = to_keys_arr.getOffsets();
        to_keys_offsets.push_back((to_keys_offsets.empty() ? 0 : to_keys_offsets.back()) + size);
        to_keys_col.reserve(size);

        for (size_t col = 0; col < values_types.size(); ++col)
        {
            auto & to_values_arr = static_cast<ColumnArray &>(to_tuple.getColumn(col + 1));
            auto & to_values_offsets = to_values_arr.getOffsets();
            to_values_offsets.push_back((to_values_offsets.empty() ? 0 : to_values_offsets.back()) + size);
            to_values_arr.getData().reserve(size);
        }

        // Write arrays of keys and values
        for (const auto & elem : merged_maps)
        {
            // Write array of keys into column
            to_keys_col.insert(elem.first);

            // Write 0..n arrays of values
            for (size_t col = 0; col < values_types.size(); ++col)
            {
                auto & to_values_col = static_cast<ColumnArray &>(to_tuple.getColumn(col + 1)).getData();
                to_values_col.insert(elem.second[col]);
            }
        }
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};

}
