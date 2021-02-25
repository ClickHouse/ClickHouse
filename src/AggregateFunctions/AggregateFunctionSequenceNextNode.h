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

/**
  * When sorting the list of events the EMPTY_EVENTS_BITSET will be moved to the last.
  * In the case of events,
  *   dt                  action
  *   2020-01-01 00:00:01 'D'
  *   2020-01-01 00:00:01 'A'
  *   2020-01-01 00:00:01 'B'
  *   2020-01-01 00:00:01 'C'
  * The next node of a chain of events 'A' -> 'B' -> 'C' is expected to be the 'D'.
  * Because EMPTY_EVENTS_BITSET is 0x80000000 the order of the sorted events is ['A", 'B', 'C', 'D']. The result value of this aggregation is 'D'.
  * If EMPTY_EVENTS_BITSET is 0 hen the order of the sorted events is ['D', 'A', 'B', 'C']. This time, the result value is NULL.
  */
static const UInt32 EMPTY_EVENTS_BITSET = 0x80000000;

/// NodeBase used to implement a linked list for storage of SequenceNextNodeImpl
template <typename Node>
struct NodeBase
{
    UInt64 size; /// size of payload

    DataTypeDateTime::FieldType event_time;
    UInt32 events_bitset; /// Bitsets of UInt32 are easy to compare. (< operator on bitsets)
                          /// Nodes in the list must be sorted in order to find a chain of events at the method getNextNodeIndex().
                          /// While sorting, events_bitset is one of sorting criteria.

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

/// It stores String, timestamp, bitset of matched events.
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

    bool compare(const Node * rhs) const
    {
        return strcmp(data(), rhs->data()) < 0;
    }
};

/// TODO : Expends SequenceNextNodeGeneralData to support other types
template <typename Node, bool Descending>
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
                        (lhs->events_bitset == rhs->events_bitset ? lhs->compare(rhs) : lhs->events_bitset < rhs->events_bitset)
                        : lhs->event_time > rhs->event_time;
            else
                return lhs->event_time == rhs->event_time ?
                        (lhs->events_bitset == rhs->events_bitset ? lhs->compare(rhs) : lhs->events_bitset < rhs->events_bitset)
                        : lhs->event_time < rhs->event_time;
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

/// Implementation of sequenceNextNode
template <typename T, typename Node, bool Descending>
class SequenceNextNodeImpl final
    : public IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node, Descending>, SequenceNextNodeImpl<T, Node, Descending>>
{
    using Data = SequenceNextNodeGeneralData<Node, Descending>;
    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;
    UInt8 events_size;
    UInt64 max_elems;

public:
    SequenceNextNodeImpl(const DataTypePtr & data_type_, const DataTypes & arguments, UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node, Descending>, SequenceNextNodeImpl<T, Node, Descending>>(
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
        /// This aggregate function sets insertion_requires_nullable_column on.
        /// Even though some values are mapped to aggregating key, it could return nulls for the below case.
        ///   aggregated events: [A -> B -> C]
        ///   events to find: [C -> D]
        ///   [C -> D] is not matched to 'A -> B -> C' so that it returns null.
        return std::make_shared<AggregateFunctionNullVariadic<true, false, true, true>>(nested_function, arguments, params);
    }

    void insert(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        a.value.push_back(v->clone(arena), arena);
    }

    void create(AggregateDataPtr place) const override
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Node * node = Node::allocate(*columns[1], row_num, arena);

        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        /// The events_bitset variable stores matched events in the form of bitset.
        /// It uses UInt32 instead of std::bitset because bitsets of UInt32 are easy to compare. (< operator on bitsets)
        /// Each Nth-bit indicates that the Nth-event are matched.
        /// For example, event1 and event3 is matched then the values of events_bitset is 0x00000005.
        ///   0x00000000
        /// +          1 (bit of event1)
        /// +          4 (bit of event3)
        UInt32 events_bitset = 0;
        for (UInt8 i = 0; i < events_size; ++i)
            if (assert_cast<const ColumnVector<UInt8> *>(columns[2 + i])->getData()[row_num])
                events_bitset += (1 << i);
        if (events_bitset == 0) events_bitset = EMPTY_EVENTS_BITSET; // Any events are not matched.

        node->event_time = timestamp;
        node->events_bitset = events_bitset;

        data(place).value.push_back(node, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
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

        /// Either sort whole container or do so partially merging ranges afterwards
        using Comparator = typename SequenceNextNodeGeneralData<Node, Descending>::Comparator;

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

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeBinary(data(place).sorted, buf);

        auto & value = data(place).value;
        writeVarUInt(value.size(), buf);
        for (auto & node : value)
            node->write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
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

    /// This method returns an index of next node that matched the events.
    /// It is one as referring Boyer-Moore-Algorithm(https://en.wikipedia.org/wiki/Boyer%E2%80%93Moore_string-search_algorithm).
    /// But, there are some differences.
    /// In original Boyer-Moore-Algorithm compares strings, but this algorithm compares events_bits.
    /// events_bitset consists of events_bits.
    /// matched events in the chain of events are represented as a bitmask of UInt32.
    /// The first matched event is 0x00000001, the second one is 0x00000002, the third one is 0x00000004, and so on.
    UInt32 getNextNodeIndex(Data & data) const
    {
        if (data.value.size() <= events_size)
            return 0;

        data.sort();

        UInt32 i = events_size - 1;
        while (i < data.value.size())
        {
            UInt32 j = 0;
            /// It checks whether the chain of events are matched or not.
            for (; j < events_size; ++j)
                /// It compares each matched events.
                /// The lower bitmask is the former matched event.
                if (!(data.value[i - j]->events_bitset & (1 << (events_size - 1 - j))))
                    break;

            /// If the chain of events are matched returns the index of result value.
            if (j == events_size)
                return i + 1;

            i += calculateJump(data, i, j);
        }

        return 0;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
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

/// Implementation of sequenceFirstNode
template <typename T, typename Node, bool Descending>
class SequenceFirstNodeImpl final
    : public IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node, Descending>, SequenceFirstNodeImpl<T, Node, Descending>>
{
    using Data = SequenceNextNodeGeneralData<Node, Descending>;
    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data *>(place); }

    DataTypePtr & data_type;

public:
    explicit SequenceFirstNodeImpl(const DataTypePtr & data_type_)
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node, Descending>, SequenceFirstNodeImpl<T, Node, Descending>>(
            {data_type_}, {})
        , data_type(this->argument_types[0])
    {
    }

    String getName() const override { return "sequenceFirstNode"; }

    DataTypePtr getReturnType() const override { return data_type; }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params,
        const AggregateFunctionProperties &) const override
    {
        return std::make_shared<AggregateFunctionNullVariadic<true, false, true, true>>(nested_function, arguments, params);
    }

    void insert(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        a.value.push_back(v->clone(arena), arena);
    }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) Data;
    }

    bool compare(const T lhs_timestamp, const T rhs_timestamp) const
    {
        return Descending ? lhs_timestamp < rhs_timestamp : lhs_timestamp > rhs_timestamp;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        bool is_first = true;
        auto & value = data(place).value;
        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        if (value.size() != 0)
        {
            if (compare(value[0]->event_time, timestamp))
                value.pop_back();
            else
                is_first = false;
        }


        if (is_first)
        {
            Node * node = Node::allocate(*columns[1], row_num, arena);
            node->event_time = timestamp;
            node->events_bitset = EMPTY_EVENTS_BITSET;

            data(place).value.push_back(node, arena);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & a = data(place).value;
        auto & b = data(rhs).value;

        if (b.empty())
            return;

        if (a.empty())
        {
            a.push_back(b[0]->clone(arena), arena);
            return;
        }

        if (compare(a[0]->event_time, b[0]->event_time))
        {
            data(place).value.pop_back();
            a.push_back(b[0]->clone(arena), arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeBinary(data(place).sorted, buf);

        auto & value = data(place).value;
        writeVarUInt(value.size(), buf);
        for (auto & node : value)
            node->write(buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena * arena) const override
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

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & value = data(place).value;

        if (value.size() > 0)
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            value[0]->insertInto(to_concrete.getNestedColumn());
            to_concrete.getNullMapData().push_back(0);
        }
        else
            to.insertDefault();
    }

    bool allocatesMemoryInArena() const override { return true; }
};

}
