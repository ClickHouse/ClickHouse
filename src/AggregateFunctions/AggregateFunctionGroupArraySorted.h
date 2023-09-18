#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <AggregateFunctions/AggregateFunctionGroupArray.h>
#include <Functions/array/arraySort.h>

#include <Common/Exception.h>
#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/IDataType.h>
#include <base/sort.h>
#include <Columns/IColumn.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Common/RadixSort.h>
#include <algorithm>
#include <type_traits>
#include <utility>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int INCORRECT_DATA;
}

template <typename T, typename Trait>
class GroupArraySortedNumericImpl final
    : public IAggregateFunctionDataHelper<GroupArrayNumericData<T, false>, GroupArraySortedNumericImpl<T, Trait>>
{
    using Data = GroupArrayNumericData<T, Trait::sampler != Sampler::NONE>;
    static constexpr bool limit_num_elems = Trait::has_limit;
    UInt64 max_elems;
    SerializationPtr serialization;

public:
    explicit GroupArraySortedNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<GroupArrayNumericData<T, false>, GroupArraySortedNumericImpl<T, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , max_elems(max_elems_)
        , serialization(data_type_->getDefaultSerialization())
    {
    }

    String getName() const override { return "groupArraySorted"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_values = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();

         if (limit_num_elems && row_values.size() < max_elems)
             throw Exception(ErrorCodes::INCORRECT_DATA, "The max size of result array is bigger than the actual array size");

        const auto & row_value = row_values[row_num];
        auto & cur_elems = this->data(place);

        cur_elems.value.push_back(row_value, arena);

        /// To optimize, we sort (2 * max_size) elements of input array over and over again
        /// and after each loop we delete the last half of sorted array
        if (cur_elems.value.size() >= max_elems * 2)
        {
            RadixSort<RadixSortNumTraits<T>>::executeLSD(cur_elems.value.data(), cur_elems.value.size());
            cur_elems.value.resize(max_elems, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        auto & rhs_elems = this->data(rhs);

        if (rhs_elems.value.empty())
            return;

        if (rhs_elems.value.size())
            cur_elems.value.insertByOffsets(rhs_elems.value, 0, rhs_elems.value.size(), arena);

        std::sort(cur_elems.value.begin(), cur_elems.value.end());
        if (limit_num_elems)
            cur_elems.value.resize(max_elems, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);

        for (const Field & elem : value)
        {
            if (elem.isNull())
            {
                writeBinary(false, buf);
            }
            else
            {
                writeBinary(true, buf);
                serialization->serializeBinary(elem, buf, {});
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = this->data(place).value;

        value.resize(size, arena);
        for (auto & element : value)
        {
            UInt8 is_null = 0;
            readBinary(is_null, buf);
            if (!is_null)
                readBinaryLittleEndian(element, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * a) const override
    {
        auto& value = this->data(place).value;

        RadixSort<RadixSortNumTraits<T>>::executeLSD(value.data(), value.size());
        if (limit_num_elems)
            value.resize(max_elems, a);
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};


template <typename Node, bool has_sampler>
struct GroupArraySortedGeneralData;

template <typename Node>
struct GroupArraySortedGeneralData<Node, false>
{
    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(Node *), 4096>;
    using Array = PODArray<Field, 32, Allocator>;

    Array value;
};

template <typename Node>
struct GroupArraySortedGeneralData<Node, true> : public GroupArraySamplerData<Node *>
{
};

/// Implementation of groupArraySorted for Generic data via Array
template <typename Node, typename Trait>
class GroupArraySortedGeneralImpl final
    : public IAggregateFunctionDataHelper<GroupArraySortedGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node, Trait>>
{
    static constexpr bool limit_num_elems = Trait::has_limit;
    using Data = GroupArraySortedGeneralData<Node, false>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;
    SerializationPtr serialization;


public:
    GroupArraySortedGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<GroupArraySortedGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
        , serialization(data_type->getDefaultSerialization())
    {
    }

    String getName() const override { return "groupArraySorted"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_elems = data(place);

        cur_elems.value.push_back(columns[0][0][row_num], arena);

        /// To optimize, we sort (2 * max_size) elements of input array over and over again and
        /// after each loop we delete the last half of sorted array

        if (cur_elems.value.size() >= max_elems * 2)
        {
            std::sort(cur_elems.value.begin(), cur_elems.value.begin() + (max_elems * 2));
            cur_elems.value.erase(cur_elems.value.begin() + max_elems, cur_elems.value.begin() + (max_elems * 2));
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = data(place);
        auto & rhs_elems = data(rhs);

        if (rhs_elems.value.empty())
            return;

        UInt64 new_elems;
        if (limit_num_elems)
        {
            new_elems = std::min(rhs_elems.value.size(), static_cast<size_t>(max_elems) - cur_elems.value.size());
        }
        else
            new_elems = rhs_elems.value.size();

        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i], arena);

        std::sort(cur_elems.value.begin(), cur_elems.value.end());
        if (limit_num_elems)
            cur_elems.value.resize(max_elems, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & value = data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);

        for (const Field & elem : value)
        {
            if (elem.isNull())
            {
                writeBinary(false, buf);
            }
            else
            {
                writeBinary(true, buf);
                serialization->serializeBinary(elem, buf, {});
            }
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = data(place).value;

        value.resize(size, arena);
        for (Field & elem : value)
        {
            UInt8 is_null = 0;
            readBinary(is_null, buf);
            if (!is_null)
                serialization->deserializeBinary(elem, buf, {});
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);
        auto & value = data(place).value;
        std::sort(value.begin(), value.end());

        if (limit_num_elems)
        {
            if (value.size() < max_elems)
                throw Exception(ErrorCodes::INCORRECT_DATA, "The max size of result array is bigger than the actual array size");
            value.resize_exact(max_elems, arena);
        }
        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + value.size());

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArrayNodeString>)
        {
            auto & string_offsets = assert_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + value.size());
        }

        for (const Field& field : value)
            column_data.insert(field);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
