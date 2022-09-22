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
#include <bitset>


namespace DB
{
struct Settings;

enum class SequenceDirection
{
    Forward,
    Backward,
};

enum SequenceBase
{
    Head,
    Tail,
    FirstMatch,
    LastMatch,
};

/// NodeBase used to implement a linked list for storage of SequenceNextNodeImpl
template <typename Node, size_t MaxEventsSize>
struct NodeBase
{
    UInt64 size; /// size of payload

    DataTypeDateTime::FieldType event_time;
    std::bitset<MaxEventsSize> events_bitset;
    bool can_be_base;

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
        UInt64 ulong_bitset = events_bitset.to_ulong();
        writeBinary(ulong_bitset, buf);
        writeBinary(can_be_base, buf);
    }

    static Node * read(ReadBuffer & buf, Arena * arena)
    {
        UInt64 size;
        readVarUInt(size, buf);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.read(node->data(), size);

        readBinary(node->event_time, buf);
        UInt64 ulong_bitset;
        readBinary(ulong_bitset, buf);
        node->events_bitset = ulong_bitset;
        readBinary(node->can_be_base, buf);

        return node;
    }
};

/// It stores String, timestamp, bitset of matched events.
template <size_t MaxEventsSize>
struct NodeString : public NodeBase<NodeString<MaxEventsSize>, MaxEventsSize>
{
    using Node = NodeString<MaxEventsSize>;

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
        assert_cast<ColumnString &>(column).insertData(this->data(), this->size);
    }

    bool compare(const Node * rhs) const
    {
        auto cmp = strncmp(this->data(), rhs->data(), std::min(this->size, rhs->size));
        return (cmp == 0) ? this->size < rhs->size : cmp < 0;
    }
};

/// TODO : Support other types than string
template <typename Node>
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
            return lhs->event_time == rhs->event_time ? lhs->compare(rhs) : lhs->event_time < rhs->event_time;
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

/// Implementation of sequenceFirstNode
template <typename T, typename Node>
class SequenceNextNodeImpl final
    : public IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node>, SequenceNextNodeImpl<T, Node>>
{
    using Self = SequenceNextNodeImpl<T, Node>;

    using Data = SequenceNextNodeGeneralData<Node>;
    static Data & data(AggregateDataPtr place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr place) { return *reinterpret_cast<const Data *>(place); }

    static constexpr size_t base_cond_column_idx = 2;
    static constexpr size_t event_column_idx = 1;

    SequenceBase seq_base_kind;
    SequenceDirection seq_direction;
    const size_t min_required_args;

    DataTypePtr & data_type;
    UInt8 events_size;
    UInt64 max_elems;
public:
    SequenceNextNodeImpl(
        const DataTypePtr & data_type_,
        const DataTypes & arguments,
        const Array & parameters_,
        SequenceBase seq_base_kind_,
        SequenceDirection seq_direction_,
        size_t min_required_args_,
        UInt64 max_elems_ = std::numeric_limits<UInt64>::max())
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node>, Self>({data_type_}, parameters_)
        , seq_base_kind(seq_base_kind_)
        , seq_direction(seq_direction_)
        , min_required_args(min_required_args_)
        , data_type(this->argument_types[0])
        , events_size(arguments.size() - min_required_args)
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "sequenceNextNode"; }

    DataTypePtr getReturnType() const override { return data_type; }

    bool haveSameStateRepresentation(const IAggregateFunction & rhs) const override
    {
        return this->getName() == rhs.getName() && this->haveEqualArgumentTypes(rhs);
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params,
        const AggregateFunctionProperties &) const override
    {
        /// Even though some values are mapped to aggregating key, it could return nulls for the below case.
        ///   aggregated events: [A -> B -> C]
        ///   events to find: [C -> D]
        ///   [C -> D] is not matched to 'A -> B -> C' so that it returns null.
        return std::make_shared<AggregateFunctionNullVariadic<false, false, true>>(nested_function, arguments, params);
    }

    void insert(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        a.value.push_back(v->clone(arena), arena);
    }

    void create(AggregateDataPtr place) const override /// NOLINT
    {
        new (place) Data;
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        Node * node = Node::allocate(*columns[event_column_idx], row_num, arena);

        const auto timestamp = assert_cast<const ColumnVector<T> *>(columns[0])->getData()[row_num];

        /// The events_bitset variable stores matched events in the form of bitset.
        /// Each Nth-bit indicates that the Nth-event are matched.
        /// For example, event1 and event3 is matched then the values of events_bitset is 0x00000005.
        ///   0x00000000
        /// +          1 (bit of event1)
        /// +          4 (bit of event3)
        node->events_bitset.reset();
        for (UInt8 i = 0; i < events_size; ++i)
            if (assert_cast<const ColumnVector<UInt8> *>(columns[min_required_args + i])->getData()[row_num])
                node->events_bitset.set(i);
        node->event_time = timestamp;

        node->can_be_base = assert_cast<const ColumnVector<UInt8> *>(columns[base_cond_column_idx])->getData()[row_num];

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
        using Comparator = typename SequenceNextNodeGeneralData<Node>::Comparator;

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

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        /// Temporarily do a const_cast to sort the values. It helps to reduce the computational burden on the initiator node.
        this->data(const_cast<AggregateDataPtr>(place)).sort();

        writeBinary(data(place).sorted, buf);

        auto & value = data(place).value;

        size_t size = std::min(static_cast<size_t>(events_size + 1), value.size());
        switch (seq_base_kind)
        {
            case SequenceBase::Head:
                writeVarUInt(size, buf);
                for (size_t i = 0; i < size; ++i)
                    value[i]->write(buf);
                break;

            case SequenceBase::Tail:
                writeVarUInt(size, buf);
                for (size_t i = 0; i < size; ++i)
                    value[value.size() - size + i]->write(buf);
                break;

            case SequenceBase::FirstMatch:
            case SequenceBase::LastMatch:
                writeVarUInt(value.size(), buf);
                for (auto & node : value)
                    node->write(buf);
                break;
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
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

    inline std::optional<size_t> getBaseIndex(Data & data) const
    {
        if (data.value.size() == 0)
            return {};

        switch (seq_base_kind)
        {
            case SequenceBase::Head:
                if (data.value[0]->can_be_base)
                    return 0;
                break;

            case SequenceBase::Tail:
                if (data.value[data.value.size() - 1]->can_be_base)
                    return data.value.size() - 1;
                break;

            case SequenceBase::FirstMatch:
                for (size_t i = 0; i < data.value.size(); ++i)
                {
                    if (data.value[i]->events_bitset.test(0) && data.value[i]->can_be_base)
                        return i;
                }
                break;

            case SequenceBase::LastMatch:
                for (size_t i = 0; i < data.value.size(); ++i)
                {
                    auto reversed_i = data.value.size() - i - 1;
                    if (data.value[reversed_i]->events_bitset.test(0) && data.value[reversed_i]->can_be_base)
                        return reversed_i;
                }
                break;
        }

        return {};
    }

    /// This method returns an index of next node that matched the events.
    /// matched events in the chain of events are represented as a bitmask.
    /// The first matched event is 0x00000001, the second one is 0x00000002, the third one is 0x00000004, and so on.
    UInt32 getNextNodeIndex(Data & data) const
    {
        const UInt32 unmatched_idx = data.value.size();

        if (data.value.size() <= events_size)
            return unmatched_idx;

        data.sort();

        std::optional<size_t> base_opt = getBaseIndex(data);
        if (!base_opt.has_value())
            return unmatched_idx;
        UInt32 base = static_cast<UInt32>(base_opt.value());

        if (events_size == 0)
            return data.value.size() > 0 ? base : unmatched_idx;

        UInt32 i = 0;
        switch (seq_direction)
        {
            case SequenceDirection::Forward:
                for (i = 0; i < events_size && base + i < data.value.size(); ++i)
                    if (!data.value[base + i]->events_bitset.test(i))
                        break;
                return (i == events_size) ? base + i : unmatched_idx;

            case SequenceDirection::Backward:
                for (i = 0; i < events_size && i < base; ++i)
                    if (!data.value[base - i]->events_bitset.test(i))
                        break;
                return (i == events_size) ? base - i : unmatched_idx;
        }
        __builtin_unreachable();
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & value = data(place).value;

        UInt32 event_idx = getNextNodeIndex(this->data(place));
        if (event_idx < value.size())
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            value[event_idx]->insertInto(to_concrete.getNestedColumn());
            to_concrete.getNullMapData().push_back(0);
        }
        else
        {
            to.insertDefault();
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};

}
