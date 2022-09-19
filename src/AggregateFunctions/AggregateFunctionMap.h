#pragma once

#include <unordered_map>
#include <base/sort.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
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
#include "base/types.h"
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
    using SearchType = KeyType;
    std::unordered_map<KeyType, AggregateDataPtr> merged_maps;

    static void writeKey(KeyType key, WriteBuffer & buf) { writeBinary(key, buf); }
    static void readKey(KeyType & key, ReadBuffer & buf) { readBinary(key, buf); }
};

template <>
struct AggregateFunctionMapCombinatorData<String>
{
    struct StringHash
    {
        using hash_type = std::hash<std::string_view>;
        using is_transparent = void;

        size_t operator()(std::string_view str) const { return hash_type{}(str); }
    };

#ifdef __cpp_lib_generic_unordered_lookup
    using SearchType = std::string_view;
#else
    using SearchType = std::string;
#endif
    std::unordered_map<String, AggregateDataPtr, StringHash, std::equal_to<>> merged_maps;

    static void writeKey(String key, WriteBuffer & buf)
    {
        writeVarUInt(key.size(), buf);
        writeString(key, buf);
    }
    static void readKey(String & key, ReadBuffer & buf)
    {
        UInt64 size;
        readVarUInt(size, buf);
        key.resize(size);
        buf.readStrict(key.data(), size);
    }
};

template <typename KeyType>
class AggregateFunctionMap final
    : public IAggregateFunctionDataHelper<AggregateFunctionMapCombinatorData<KeyType>, AggregateFunctionMap<KeyType>>
{
private:
    DataTypePtr key_type;
    AggregateFunctionPtr nested_func;

    using Data = AggregateFunctionMapCombinatorData<KeyType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionMap<KeyType>>;

public:
    AggregateFunctionMap(AggregateFunctionPtr nested, const DataTypes & types) : Base(types, nested->getParameters()), nested_func(nested)
    {
        if (types.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function " + getName() + " requires at least one argument");

        if (types.size() > 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function " + getName() + " requires only one map argument");

        const auto * map_type = checkAndGetDataType<DataTypeMap>(types[0].get());
        if (!map_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function " + getName() + " requires map as argument");

        key_type = map_type->getKeyType();
    }

    String getName() const override { return nested_func->getName() + "Map"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeMap>(DataTypes{key_type, nested_func->getReturnType()}); }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
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
            typename Data::SearchType key;

            if constexpr (std::is_same_v<KeyType, String>)
            {
                StringRef key_ref;
                if (key_type->getTypeId() == TypeIndex::FixedString)
                    key_ref = assert_cast<const ColumnFixedString &>(key_column).getDataAt(offset + i);
                else
                    key_ref = assert_cast<const ColumnString &>(key_column).getDataAt(offset + i);

#ifdef __cpp_lib_generic_unordered_lookup
                key = key_ref.toView();
#else
                key = key_ref.toString();
#endif
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
                nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
                nested_func->create(nested_place);
                merged_maps.emplace(key, nested_place);
            }
            else
                nested_place = it->second;

            const IColumn * nested_columns[1] = {&val_column};
            nested_func->add(nested_place, nested_columns, offset + i, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);

            AggregateDataPtr nested_place;
            if (it == merged_maps.end())
            {
                // elem.second cannot be copied since this it will be destroyed after merging,
                // and lead to use-after-free.
                nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
                nested_func->create(nested_place);
                merged_maps.emplace(elem.first, nested_place);
            }
            else
            {
                nested_place = it->second;
            }

            nested_func->merge(nested_place, elem.second, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        writeVarUInt(merged_maps.size(), buf);

        for (const auto & elem : merged_maps)
        {
            this->data(place).writeKey(elem.first, buf);
            nested_func->serialize(elem.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        UInt64 size;

        readVarUInt(size, buf);
        for (UInt64 i = 0; i < size; ++i)
        {
            KeyType key;
            AggregateDataPtr nested_place;

            this->data(place).readKey(key, buf);
            nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
            nested_func->create(nested_place);
            merged_maps.emplace(key, nested_place);
            nested_func->deserialize(nested_place, buf, std::nullopt, arena);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & map_column = assert_cast<ColumnMap &>(to);
        auto & nested_column = map_column.getNestedColumn();
        auto & nested_data_column = map_column.getNestedData();

        auto & key_column = nested_data_column.getColumn(0);
        auto & val_column = nested_data_column.getColumn(1);

        auto & merged_maps = this->data(place).merged_maps;

        // sort the keys
        std::vector<KeyType> keys;
        keys.reserve(merged_maps.size());
        for (auto & it : merged_maps)
        {
            keys.push_back(it.first);
        }
        ::sort(keys.begin(), keys.end());

        // insert using sorted keys to result column
        for (auto & key : keys)
        {
            key_column.insert(key);
            nested_func->insertResultInto(merged_maps[key], val_column, arena);
        }

        IColumn::Offsets & res_offsets = nested_column.getOffsets();
        res_offsets.push_back(val_column.size());
    }

    bool allocatesMemoryInArena() const override { return true; }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};

}
