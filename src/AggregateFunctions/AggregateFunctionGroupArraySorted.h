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
#include <base/sort.h>
#include <Columns/IColumn.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <algorithm>
#include <type_traits>
#include <utility>

#define AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE 0xFFFFFF


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
    UInt64 seed;

public:
    explicit GroupArraySortedNumericImpl(
        const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayNumericData<T, false>, GroupArraySortedNumericImpl<T, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return "groupArraySorted"; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & row_values = assert_cast<const ColumnVector<T> &>(*columns[0]).getData();

        if (limit_num_elems && row_values.size() < max_elems)
            throw Exception(ErrorCodes::INCORRECT_DATA, "The max size of result array is bigger than the actual array size");

        const auto & row_value = row_values[row_num];
        auto & cur_elems = this->data(place);

        ++cur_elems.total_values;
        cur_elems.value.push_back(row_value, arena);

        if (!limit_num_elems && columns[0]->size() == cur_elems.value.size())
            std::sort(cur_elems.value.begin(), cur_elems.value.end());
        else if (columns[0]->size() == cur_elems.value.size())
        {
            /// To optimize, we sort (2 * max_size) elements of input array over and over again
            /// and after each loop we delete the last half of sorted array
            bool sorted = false, part_sorted = false;
            while (!sorted)
            {
                if (cur_elems.value.size() >= max_elems * 2)
                {
                    std::sort(cur_elems.value.begin(), cur_elems.value.begin() + (max_elems * 2));
                    cur_elems.value.erase(cur_elems.value.begin() + max_elems, cur_elems.value.begin() + (max_elems * 2));
                    part_sorted = true;
                }
                else if (cur_elems.value.size() > max_elems)
                {
                    std::sort(cur_elems.value.begin(), cur_elems.value.end());
                    cur_elems.value.resize(max_elems, arena);
                    sorted = true;
                }
                else if (cur_elems.value.size() == max_elems)
                {
                    if (part_sorted)
                        sorted = true;
                    else
                    {
                        std::sort(cur_elems.value.begin(), cur_elems.value.end());
                        sorted = true;
                    }
                }
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = this->data(place);
        size_t cur_size = cur_elems.value.size();
        auto & rhs_elems = this->data(rhs);

        if (rhs_elems.value.empty())
            return;

        if (limit_num_elems)
        {
            UInt64 elems_to_insert = std::min(static_cast<size_t>(max_elems) - cur_size, rhs_elems.value.size());
            if (elems_to_insert)
            {
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, elems_to_insert, arena);
            }
        }
        else
        {
            if (rhs_elems.value.size())
                cur_elems.value.insertByOffsets(rhs_elems.value, 0, rhs_elems.value.size(), arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & value = this->data(place).value;
        const size_t size = value.size();
        writeVarUInt(size, buf);
        for (const auto & element : value)
            writeBinaryLittleEndian(element, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        size_t size = 0;
        readVarUInt(size, buf);

        if (unlikely(size > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE);

        if (limit_num_elems && unlikely(size > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = this->data(place).value;

        value.resize_exact(size, arena);
        for (auto & element : value)
            readBinaryLittleEndian(element, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & value = this->data(place).value;
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


/// Implementation of groupArraySorted for Generic data via Array
template <typename Node, typename Trait>
class GroupArraySortedGeneralImpl final
    : public IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node, Trait>>
{
    static constexpr bool limit_num_elems = Trait::has_limit;
    using Data = GroupArrayGeneralData<Node, false>;
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt64 max_elems;
    UInt64 seed;

public:
    GroupArraySortedGeneralImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_ = std::numeric_limits<UInt64>::max(), UInt64 seed_ = 123456)
        : IAggregateFunctionDataHelper<GroupArrayGeneralData<Node, false>, GroupArraySortedGeneralImpl<Node, Trait>>(
            {data_type_}, parameters_, std::make_shared<DataTypeArray>(data_type_))
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
        , seed(seed_)
    {
    }

    String getName() const override { return "groupArraySorted"; }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
    {
        [[maybe_unused]] auto a = new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        if (limit_num_elems && (columns[0]->size() < max_elems))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Max size of result array is bigger than actual array size");

        auto & cur_elems = data(place);

        ++cur_elems.total_values;
        Node * node = Node::allocate(*columns[0], row_num, arena);
        cur_elems.value.push_back(node, arena);

        if (!limit_num_elems && columns[0]->size() == cur_elems.value.size())
            std::sort(cur_elems.value.begin(), cur_elems.value.end(), [](const Node *a, const Node *b)
            {
                return a->field < b->field;
            });
        else if (columns[0]->size() == cur_elems.value.size())
        {
            /// To optimize, we sort (2 * max_size) elements of input array over and over again and
            /// after each loop we delete the last half of sorted array
            bool sorted = false, part_sorted = false;
            while (!sorted)
            {
                if (cur_elems.value.size() >= max_elems * 2)
                {
                    std::sort(cur_elems.value.begin(), cur_elems.value.begin() + (max_elems * 2), [](const Node *a, const Node *b)
                    {
                        return a->field < b->field;
                    });
                    cur_elems.value.erase(cur_elems.value.begin() + max_elems, cur_elems.value.begin() + (max_elems * 2));
                    part_sorted = true;
                }
                else if (cur_elems.value.size() > max_elems)
                {
                    std::sort(cur_elems.value.begin(), cur_elems.value.end(), [](const Node *a, const Node *b)
                    {
                        return a->field < b->field;
                    });
                    cur_elems.value.resize(max_elems, arena);
                    sorted = true;
                }
                else if (cur_elems.value.size() == max_elems)
                {
                    if (part_sorted)
                        sorted = true;
                    else
                    {
                        std::sort(cur_elems.value.begin(), cur_elems.value.end(), [](const Node *a, const Node *b)
                        {
                            return a->field < b->field;
                        });
                        sorted = true;
                    }
                }
            }
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_elems = data(place);
        auto & rhs_elems = data(rhs);

        if (rhs_elems.value.empty())
            return;
        mergeNoSampler(cur_elems, rhs_elems, arena);
    }

    void ALWAYS_INLINE mergeNoSampler(Data & cur_elems, const Data & rhs_elems, Arena * arena) const
    {
        UInt64 new_elems;
        if (limit_num_elems)
        {
            if (cur_elems.value.size() >= max_elems)
                return;
            new_elems = std::min(rhs_elems.value.size(), static_cast<size_t>(max_elems) - cur_elems.value.size());
        }
        else
            new_elems = rhs_elems.value.size();

        for (UInt64 i = 0; i < new_elems; ++i)
            cur_elems.value.push_back(rhs_elems.value[i]->clone(arena), arena);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).value.size(), buf);

        auto & value = data(place).value;
        for (auto & node : value)
            node->write(buf);

        if constexpr (Trait::last)
            writeBinaryLittleEndian(data(place).total_values, buf);

        if constexpr (Trait::sampler == Sampler::RNG)
        {
            writeBinaryLittleEndian(data(place).total_values, buf);
            WriteBufferFromOwnString rng_buf;
            rng_buf << data(place).rng;
            writeStringBinary(rng_buf.str(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        UInt64 elems;
        readVarUInt(elems, buf);

        if (unlikely(elems == 0))
            return;

        if (unlikely(elems > AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE);

        if (limit_num_elems && unlikely(elems > max_elems))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large array size, it should not exceed {}", max_elems);

        auto & value = data(place).value;

        value.resize_exact(elems, arena);
        for (UInt64 i = 0; i < elems; ++i)
            value[i] = Node::read(buf, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & column_array = assert_cast<ColumnArray &>(to);

        auto & offsets = column_array.getOffsets();
        offsets.push_back(offsets.back() + data(place).value.size());

        auto & column_data = column_array.getData();

        if (std::is_same_v<Node, GroupArrayNodeString>)
        {
            auto & string_offsets = assert_cast<ColumnString &>(column_data).getOffsets();
            string_offsets.reserve(string_offsets.size() + data(place).value.size());
        }

        auto & value = data(place).value;
        for (auto & node : value)
            node->insertInto(column_data);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

#undef AGGREGATE_FUNCTION_GROUP_ARRAY_MAX_ARRAY_SIZE

}
