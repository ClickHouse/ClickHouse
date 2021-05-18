#pragma once

#include <unordered_map>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include "AggregateFunctions/AggregateFunctionFactory.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename KeyType>
struct AggregateFunctionMapCombinatorData
{
    std::unordered_map<KeyType, AggregateDataPtr> merged_maps;

    static void writeKey(KeyType key, WriteBuffer & buf) { writeBinary(key, buf); }
    static void readKey(KeyType & key, ReadBuffer & buf) { readBinary(key, buf); }
};

template <>
struct AggregateFunctionMapCombinatorData<String>
{
    std::unordered_map<String, AggregateDataPtr> merged_maps;

    static void writeKey(String key, WriteBuffer & buf) { writeString(key, buf); }
    static void readKey(String & key, ReadBuffer & buf) { readString(key, buf); }
};

template <typename KeyType>
class AggregateFunctionMap final
    : public IAggregateFunctionDataHelper<AggregateFunctionMapCombinatorData<KeyType>, AggregateFunctionMap<KeyType>>
{
private:
    DataTypePtr key_type;
    AggregateFunctionPtr nested_func;
    using Base = IAggregateFunctionDataHelper<AggregateFunctionMapCombinatorData<KeyType>, AggregateFunctionMap<KeyType>>;

public:
    AggregateFunctionMap(AggregateFunctionPtr nested, const DataTypes & types) : Base(types, nested->getParameters()), nested_func(nested)
    {
        if (types.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function " + getName() + " require at least one argument");

        const auto * map_type = checkAndGetDataType<DataTypeMap>(types[0].get());
        if (!map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function " + getName() + " requires map as argument");

        key_type = map_type->getKeyType();
    }

    String getName() const override { return nested_func->getName() + "Map"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeMap>(DataTypes{key_type, nested_func->getReturnType()}); }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & map_column = assert_cast<const ColumnMap &>(*columns[0]);
        const auto & map_nested_tuple = map_column.getNestedData();
        const IColumn::Offsets & map_array_offsets = map_column.getNestedColumn().getOffsets();

        const size_t offset = map_array_offsets[row_num - 1];
        const size_t size = (map_array_offsets[row_num] - offset);

        const auto & key_column = map_nested_tuple.getColumn(0);
        const auto & val_column = map_nested_tuple.getColumn(1);

        auto & merged_maps = this->data(place).merged_maps;

        for (size_t i = 0; i < size; ++i)
        {
            KeyType key;
            if constexpr (std::is_same<KeyType, String>::value)
            {
                key = key_column.operator[](offset + i).get<KeyType>();
            }
            else
            {
                key = assert_cast<const ColumnVector<KeyType> &>(key_column).getData()[offset + i];
            }

            AggregateDataPtr nested_place;
            auto it = merged_maps.find(key);

            if (it == merged_maps.end())
            {
                // create a new place for each key
                nested_place = arena->alloc(nested_func->sizeOfData());
                nested_func->create(nested_place);
                merged_maps.emplace(key, nested_place);
            }
            else
                nested_place = it->second;

            const IColumn * nested_columns[1] = {&val_column};
            nested_func->add(nested_place, nested_columns, offset + i, arena);
        }
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);

            if (it != merged_maps.end())
            {
                nested_func->merge(it->second, elem.second, arena);
            }
            else
                merged_maps[elem.first] = elem.second;
        }
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        writeVarUInt(merged_maps.size(), buf);

        for (const auto & elem : merged_maps)
        {
            this->data(place).writeKey(elem.first, buf);
            nested_func->serialize(elem.second, buf);
        }
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        size_t size;

        readVarUInt(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            KeyType key;
            AggregateDataPtr nested_place;

            this->data(place).readKey(key, buf);
            nested_place = arena->alloc(nested_func->sizeOfData());
            nested_func->create(nested_place);
            merged_maps.emplace(key, nested_place);
            nested_func->deserialize(nested_place, buf, arena);
        }
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override
    {
        auto & map_column = assert_cast<ColumnMap &>(to);
        auto & nested_column = map_column.getNestedColumn();
        auto & nested_data_column = map_column.getNestedData();

        auto & key_column = nested_data_column.getColumn(0);
        auto & val_column = nested_data_column.getColumn(1);

        auto & merged_maps = this->data(place).merged_maps;

        size_t res_offset = 0;

        // sort the keys
        std::vector<KeyType> keys;
        keys.reserve(merged_maps.size());
        for (auto & it : merged_maps)
        {
            keys.push_back(it.first);
        }
        std::sort(keys.begin(), keys.end());

        // insert using sorted keys to result column
        for (auto & key : keys)
        {
            res_offset++;
            key_column.insert(key);
            nested_func->insertResultInto(merged_maps[key], val_column, arena);
        }

        IColumn::Offsets & res_offsets = nested_column.getOffsets();
        auto last_offset = res_offsets[res_offsets.size() - 1];
        res_offsets.push_back(last_offset + res_offset);
    }

    bool allocatesMemoryInArena() const override { return true; }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
