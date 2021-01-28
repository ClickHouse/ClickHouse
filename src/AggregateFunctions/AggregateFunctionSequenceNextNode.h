#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionNull.h>

#include <type_traits>


namespace DB
{

template <typename Node>
struct NodeBase
{
    UInt64 size; // size of payload

    DataTypeDateTime::FieldType event_time;
    UInt32 events_bitset; // UInt32 for combiniant comparesons between bitsets (< operator on bitsets).

    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

    Node * clone(Arena * arena) const
    {
        return reinterpret_cast<Node *>(
            const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size, alignof(Node))));
    }

    void write(WriteBuffer & buf) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);

        writeBinary(event_time, buf);
        writeBinary(events_bitset, buf);
    }

    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.read(node->data(), size);

        readBinary(node->event_time, buf);
        readBinary(node->events_bitset, buf);

        return node;
    }
};

struct NodeString : public NodeBase<NodeString>
{
    using Node = NodeString;

    static Node * allocate(const IColumn & column, size_t row_num, Arena * arena)
    {
        StringRef string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size, alignof(Node)));
        node->size = string.size;
        memcpy(node->data(), string.data, string.size);

        return node;
    }

    void insertInto(IColumn & column)
    {
        assert_cast<ColumnString &>(column).insertData(data(), size);
    }
};

template <typename T, typename Node, bool Descending>
struct SequenceNextNodeGeneralData
{
    using Allocator = MixedAlignedArenaAllocator<alignof(Node *), 4096>;
    using Array = PODArray<Node *, 32, Allocator>;

    Array value;
    bool sorted = false;

    struct Comparator final
    {
        bool operator()(const Node * lhs, const Node * rhs) const
        {
            if constexpr (Descending)
                return lhs->event_time == rhs->event_time ?
                        lhs->events_bitset < rhs->events_bitset: lhs->event_time > rhs->event_time;
            else
                return lhs->event_time == rhs->event_time ?
                        lhs->events_bitset < rhs->events_bitset : lhs->event_time < rhs->event_time;
        }
    };

    void sort()
    {
        if (!sorted)
        {
            std::stable_sort(std::begin(value), std::end(value), Comparator{});
            sorted = true;
        }
    }
};

template <typename T, typename Node, bool Descending>
class SequenceNextNodeImpl final
    : public IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<T, Node, Descending>, SequenceNextNodeImpl<T, Node, Descending>>
{
    using Data = SequenceNextNodeGeneralData<T, Node, Descending>;
    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt8 events_size;
    UInt64 max_elems;

public:
    SequenceNextNodeImpl(const DataTypePtr & data_type_, const DataTypes & arguments, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<T, Node, Descending>, SequenceNextNodeImpl<T, Node, Descending>>(
            {data_type_}, {})
        , data_type(this->argument_types[0])
        , events_size(arguments.size() - 2)
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "sequenceNextNode"; }

    DataTypePtr getReturnType() const override { return data_type; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params,
        const AggregateFunctionProperties & /*properties*/) const override
    {
        // This aggregate function sets insertion_requires_nullable_column on.
        // Even though some values are mapped to aggregating key, it could return nulls for the below case.
        //   aggregated events: [A -> B -> C]
        //   events to find: [C -> D]
        //   [C -> D] is not matched to 'A -> B -> C' so that it returns null.
        return std::make_shared<AggregateFunctionNullVariadic<true, false, true, true>>(nested_function, arguments, params);
    }

    void insert(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        a.value.push_back(v->clone(arena), arena);
    }

    void create(AggregateDataPtr place) const override
    {
        [[maybe_unused]] auto a = new (place) Data;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Node * node = Node::allocate(*columns[1], row_num, arena);

        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        UInt32 events_bitset = 0;
        for (UInt8 i = 0; i < events_size; ++i)
            if (assert_cast<const ColumnVector<UInt8> *>(columns[2 + i])->getData()[row_num])
                events_bitset += (1 << i);

        node->event_time = timestamp;
        node->events_bitset = events_bitset;

        data(place).value.push_back(node, arena);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (data(rhs).value.empty())
            return;

        if (data(place).value.size() >= max_elems)
            return;

        auto & a = data(place).value;
        auto & b = data(rhs).value;
        const auto a_size = a.size();

        const UInt64 new_elems = std::min(data(rhs).value.size(), static_cast<size_t>(max_elems) - data(place).value.size());
        for (UInt64 i = 0; i < new_elems; ++i)
            a.push_back(b[i]->clone(arena), arena);

        /// either sort whole container or do so partially merging ranges afterwards
        using Comparator = typename SequenceNextNodeGeneralData<T, Node, Descending>::Comparator;

        if (!data(place).sorted && !data(rhs).sorted)
            std::stable_sort(std::begin(a), std::end(a), Comparator{});
        else
        {
            const auto begin = std::begin(a);
            const auto middle = std::next(begin, a_size);
            const auto end = std::end(a);

            if (!data(place).sorted)
                std::stable_sort(begin, middle, Comparator{});

            if (!data(rhs).sorted)
                std::stable_sort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        data(place).sorted = true;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(data(place).sorted, buf);

        auto & value = data(place).value;
        writeVarUInt(value.size(), buf);
        for (auto & node : value)
            node->write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        readBinary(data(place).sorted, buf);

        UInt64 size;
        readVarUInt(size, buf);

        if (unlikely(size == 0))
            return;

        auto & value = data(place).value;

        value.resize(size, arena);
        for (UInt64 i = 0; i < size; ++i)
            value[i] = Node::read(buf, arena);
    }

    inline UInt32 calculateJump(const Data & data, const UInt32 i, const UInt32 j) const
    {
        UInt32 k = 0;
        for (; k < events_size - j; ++k)
            if (data.value[i - j]->events_bitset & (1 << (events_size - 1 - j - k)))
                return k;
        return k;
    }

    // This method returns an index of next node that matched the events.
    // It is one as referring Boyer-Moore-Algorithm.
    UInt32 getNextNodeIndex(Data & data) const
    {
        if (data.value.size() <= events_size)
            return 0;

        data.sort();

        UInt32 i = events_size - 1;
        while (i < data.value.size())
        {
            UInt32 j = 0;
            for (; j < events_size; ++j)
                if (!(data.value[i - j]->events_bitset & (1 << (events_size - 1 - j))))
                    break;

            if (j == events_size)
                return i + 1;

            i += calculateJump(data, i, j);
        }

        return 0;
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        auto & value = data(place).value;

        UInt32 event_idx = getNextNodeIndex(this->data(place));
        if (event_idx != 0 && event_idx < value.size())
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            value[event_idx]->insertInto(to_concrete.getNestedColumn());
            to_concrete.getNullMapData().push_back(0);
        }
        else
            to.insertDefault();
    }

    bool allocatesMemoryInArena() const override { return true; }
};

}
