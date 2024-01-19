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

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE 0xFFFFFF

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}

template <typename T>
struct GroupArraySortedData;

template <typename T>
struct GroupArraySortedData
{
    /// For easy serialization.
    static_assert(std::has_unique_object_representations_v<T> || std::is_floating_point_v<T>);

    // Switch to ordinary Allocator after 4096 bytes to avoid fragmentation and trash in Arena
    using Allocator = MixedAlignedArenaAllocator<alignof(T), 4096>;
    using Array = PODArray<T, 32, Allocator>;

    Array value;
};

template <typename T>
class GroupArraySortedNumericImpl final
    : public IAggregateFunctionDataHelper<GroupArraySortedData<T>, GroupArraySortedNumericImpl<T>>
{
    using Data = GroupArraySortedData<T>;
    UInt64 max_elems;
    SerializationPtr serialization;

public:
    explicit GroupArraySortedNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<GroupArraySortedData<T>, GroupArraySortedNumericImpl<T>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , max_elems(max_elems_)
        , serialization(data_type_->getDefaultSerialization())
    {
    }

    String getName() const override { return "groupArraySorted"; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_value = assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num];
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

        RadixSort<RadixSortNumTraits<T>>::executeLSD(cur_elems.value.data(), cur_elems.value.size());

        size_t elems_size = cur_elems.value.size() < max_elems ? cur_elems.value.size() : max_elems;
        cur_elems.value.resize(elems_size, arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & value = this->data(place).value;
        size_t size = value.size();
        writeVarUInt(size, buf);

        for (const auto & elem : value)
            writeBinaryLittleEndian(elem, buf);
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
            readBinaryLittleEndian(element, buf);
    }

    static void checkArraySize(size_t elems, size_t max_elems)
    {
        if (unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size {} (maximum: {})", elems, max_elems);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        auto& value = this->data(place).value;

        RadixSort<RadixSortNumTraits<T>>::executeLSD(value.data(), value.size());
        size_t elems_size = value.size() < max_elems ? value.size() : max_elems;
        value.resize(elems_size, arena);
        size_t size = value.size();

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        offsets_to.push_back(offsets_to.back() + size);

        if (size)
        {
            typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
            data_to.insert(this->data(place).value.begin(), this->data(place).value.end());
            RadixSort<RadixSortNumTraits<T>>::executeLSD(value.data(), value.size());
            value.resize(elems_size, arena);
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
struct GroupArraySortedNodeBase
{
    UInt64 size; // size of payload

    /// Returns pointer to actual payload
    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }
};

struct GroupArraySortedNodeString : public GroupArraySortedNodeBase<GroupArraySortedNodeString>
{
    using Node = GroupArraySortedNodeString;

};

struct GroupArraySortedNodeGeneral : public GroupArraySortedNodeBase<GroupArraySortedNodeGeneral>
{
    using Node = GroupArraySortedNodeGeneral;

};

/// Implementation of groupArraySorted for Generic data via Array
template <typename Node>
class GroupArraySortedGeneralImpl final
    : public IAggregateFunctionDataHelper<GroupArraySortedGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node>>
{
    using Data = GroupArraySortedGeneralData<Node, false>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;
    SerializationPtr serialization;


public:
    GroupArraySortedGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<GroupArraySortedGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node>>(
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

        UInt64 new_elems = rhs_elems.value.size();

        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i], arena);

        checkArraySize(cur_elems.value.size(), AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE);

        if (!cur_elems.value.empty())
        {
            std::sort(cur_elems.value.begin(), cur_elems.value.end());

            if (cur_elems.value.size() > max_elems)
                cur_elems.value.resize(max_elems, arena);
        }
    }

    static void checkArraySize(size_t elems, size_t max_elems)
    {
        if (unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size {} (maximum: {})", elems, max_elems);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & value = data(place).value;
        size_t size = value.size();
        checkArraySize(size, AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE);
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

        checkArraySize(size, AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ELEMENT_SIZE);
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

        if (!value.empty())
        {
            std::sort(value.begin(), value.end());

            if (value.size() > max_elems)
                value.resize_exact(max_elems, arena);
        }
        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + value.size());

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArraySortedNodeString>)
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
