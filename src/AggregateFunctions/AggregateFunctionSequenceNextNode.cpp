#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <base/sort.h>
#include <Core/ProtocolDefines.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <base/range.h>

#include <bitset>
#include <numeric>


namespace DB
{
namespace Setting
{
    extern const SettingsBool enable_funnel_functions;
}

constexpr size_t max_events_size = 64;
constexpr size_t min_required_args = 3;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_AGGREGATE_FUNCTION;
    extern const int TOO_LARGE_ARRAY_SIZE;
    extern const int INCORRECT_DATA;
}

namespace
{

enum class SequenceDirection : uint8_t
{
    Forward,
    Backward,
};

enum SequenceBase : uint8_t
{
    Head,
    Tail,
    FirstMatch,
    LastMatch,
};

/// This is for security
const UInt64 max_node_size_deserialize = 0xFFFFFF;

/// NodeBase used to implement a linked list for storage of SequenceNextNodeImpl
template <typename Node, size_t MaxEventsSize>
struct NodeBase
{
    UInt64 size; /// size of payload

    UInt64 event_time;
    std::bitset<MaxEventsSize> events_bitset;
    bool can_be_base;

    char * data() { return reinterpret_cast<char *>(this) + sizeof(Node); }

    const char * data() const { return reinterpret_cast<const char *>(this) + sizeof(Node); }

    Node * clone(Arena * arena) const
    {
        return reinterpret_cast<Node *>(
            const_cast<char *>(arena->alignedInsert(reinterpret_cast<const char *>(this), sizeof(Node) + size, alignof(Node))));
    }

    void write(WriteBuffer & buf, bool full_timestamp) const
    {
        writeVarUInt(size, buf);
        buf.write(data(), size);

        if (full_timestamp)
            writeBinary(event_time, buf);
        else
            writeBinary(static_cast<UInt32>(event_time), buf);
        UInt64 ulong_bitset = events_bitset.to_ulong();
        writeBinary(ulong_bitset, buf);
        writeBinary(can_be_base, buf);
    }

    static Node * read(ReadBuffer & buf, Arena * arena, bool full_timestamp)
    {
        UInt64 size = 0;
        readVarUInt(size, buf);
        if (unlikely(size > max_node_size_deserialize))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large node state size");

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.readStrict(node->data(), size);

        if (full_timestamp)
        {
            readBinary(node->event_time, buf);
        }
        else
        {
            UInt32 event_time = 0;
            readBinary(event_time, buf);
            node->event_time = event_time;
        }
        UInt64 ulong_bitset = 0;
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
        auto string = assert_cast<const ColumnString &>(column).getDataAt(row_num);

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + string.size(), alignof(Node)));
        node->size = string.size();
        memcpy(node->data(), string.data(), string.size());

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
    /// The event times hold only their low 32 bits, as in a version 0 state.
    bool truncated_timestamps = false;

    /// `low_32_bits` orders by the low 32 bits of the event time, as a version 0 state is ordered,
    /// and breaks the ties of that order by the full event time.
    template <bool low_32_bits = false>
    struct Comparator final
    {
        bool operator()(const Node * lhs, const Node * rhs) const
        {
            if constexpr (low_32_bits)
            {
                const auto lhs_time = static_cast<UInt32>(lhs->event_time);
                const auto rhs_time = static_cast<UInt32>(rhs->event_time);
                if (lhs_time != rhs_time)
                    return lhs_time < rhs_time;
                if (lhs->compare(rhs))
                    return true;
                if (rhs->compare(lhs))
                    return false;
                return lhs->event_time < rhs->event_time;
            }
            else
                return lhs->event_time == rhs->event_time ? lhs->compare(rhs) : lhs->event_time < rhs->event_time;
        }
    };

    void sort()
    {
        if (!sorted)
        {
            ::stableSort(std::begin(value), std::end(value), Comparator<>{});
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
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }  /// NOLINT(readability-non-const-parameter)
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

    static constexpr size_t base_cond_column_idx = 2;
    static constexpr size_t event_column_idx = 1;

    /// Version 1 of the state stores 8-byte event times; only a `UInt64` timestamp needs them.
    static constexpr bool versioned = std::is_same_v<T, UInt64>;

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
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node>, Self>(arguments, parameters_, data_type_)
        , seq_base_kind(seq_base_kind_)
        , seq_direction(seq_direction_)
        , min_required_args(min_required_args_)
        , data_type(this->argument_types[0])
        , events_size(static_cast<UInt8>(arguments.size() - min_required_args))
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "sequenceNextNode"; }

    bool isVersioned() const override { return versioned; }

    /// An unversioned type keeps the version 0 layout of persisted states.
    size_t getDefaultVersion() const override { return 0; }

    DataTypePtr getStateType() const override
    {
        if constexpr (versioned)
            return std::make_shared<DataTypeAggregateFunction>(this->shared_from_this(), this->argument_types, this->parameters, 1);
        else
            return IAggregateFunction::getStateType();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        if (versioned && revision >= DBMS_MIN_REVISION_WITH_SEQUENCE_NEXT_NODE_FULL_TIMESTAMPS)
            return 1;
        return 0;
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return this->getName() == rhs.getName() && this->haveEqualArgumentTypes(rhs);
    }

    void insert(Data & a, const Node * v, Arena * arena) const
    {
        ++a.total_values;
        a.value.push_back(v->clone(arena), arena);
    }

    void create(AggregateDataPtr __restrict place) const override /// NOLINT
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
        if constexpr (versioned)
            if (data(place).truncated_timestamps)
                node->event_time = static_cast<UInt32>(timestamp);

        node->can_be_base = assert_cast<const ColumnVector<UInt8> *>(columns[base_cond_column_idx])->getData()[row_num];

        data(place).value.push_back(node, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
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

        const auto begin = std::begin(a);
        const auto middle = std::next(begin, a_size);
        const auto end = std::end(a);

        bool place_sorted = data(place).sorted;
        bool rhs_sorted = data(rhs).sorted;

        /// The high 32 bits lost by one side cannot be restored, so the other side drops them too.
        if constexpr (versioned)
        {
            if (data(place).truncated_timestamps != data(rhs).truncated_timestamps)
            {
                if (data(rhs).truncated_timestamps)
                {
                    truncateTimestamps(begin, middle);
                    data(place).truncated_timestamps = true;
                    place_sorted = true;
                }
                else
                {
                    truncateTimestamps(middle, end);
                    rhs_sorted = true;
                }
            }
        }

        /// Either sort whole container or do so partially merging ranges afterwards
        using Comparator = typename Data::template Comparator<>;

        if (!place_sorted && !rhs_sorted)
            ::stableSort(begin, end, Comparator{});
        else
        {
            if (!place_sorted)
                ::stableSort(begin, middle, Comparator{});

            if (!rhs_sorted)
                ::stableSort(middle, end, Comparator{});

            std::inplace_merge(begin, middle, end, Comparator{});
        }

        data(place).sorted = true;
    }

    /// Leaves the range sorted by its truncated event times.
    static void truncateTimestamps(Node ** begin, Node ** end)
    {
        ::stableSort(begin, end, typename Data::template Comparator<true>{});
        for (auto * it = begin; it != end; ++it)
            (*it)->event_time = static_cast<UInt32>((*it)->event_time);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override
    {
        /// serialize() must not mutate the state: it takes a ConstAggregateDataPtr and the same
        /// state can be shared between rows (a replicated/const aggregate-state column used as a
        /// GROUP BY key) and serialized concurrently by several pipeline threads. sort() reorders
        /// the value buffer in place, so sorting the shared buffer directly is a data race. Sort a
        /// local copy of the node pointers; the values are always written in sorted order.
        const auto & data_ref = data(place);
        const bool full_timestamps = versioned && version.value_or(getDefaultVersion()) >= 1;
        const bool has_full_timestamps = versioned && !data_ref.truncated_timestamps;

        VectorWithMemoryTracking<Node *> sorted_value(data_ref.value.begin(), data_ref.value.end());
        /// Version 0 is ordered and pruned by the truncated event times, which a version 0 reader sorts by.
        if (has_full_timestamps && !full_timestamps)
            ::stableSort(sorted_value.begin(), sorted_value.end(), typename Data::template Comparator<true>{});
        else if (!data_ref.sorted)
            ::stableSort(sorted_value.begin(), sorted_value.end(), typename Data::template Comparator<>{});

        if (full_timestamps)
            writeBinary(static_cast<UInt8>(data_ref.truncated_timestamps), buf);

        writeBinary(true, buf);

        const size_t n = sorted_value.size();
        const size_t size = std::min(static_cast<size_t>(events_size + 1), n);

        /// Version 1 also keeps the extreme nodes of the low 32-bit order, which a merge with a version 0 state sorts by.
        if (full_timestamps && has_full_timestamps && n > size
            && (seq_base_kind == SequenceBase::Head || seq_base_kind == SequenceBase::Tail)
            && (sorted_value.front()->event_time >> 32) != (sorted_value.back()->event_time >> 32))
        {
            VectorWithMemoryTracking<size_t> low_32_bit_order(n);
            std::iota(low_32_bit_order.begin(), low_32_bit_order.end(), 0);
            ::stableSort(low_32_bit_order.begin(), low_32_bit_order.end(), [&](size_t lhs, size_t rhs)
            {
                return typename Data::template Comparator<true>{}(sorted_value[lhs], sorted_value[rhs]);
            });

            VectorWithMemoryTracking<UInt8> keep(n);
            const size_t first = seq_base_kind == SequenceBase::Head ? 0 : n - size;
            for (size_t i = first; i < first + size; ++i)
            {
                keep[i] = 1;
                keep[low_32_bit_order[i]] = 1;
            }

            writeVarUInt(static_cast<UInt64>(std::count(keep.begin(), keep.end(), 1)), buf);
            for (size_t i = 0; i < n; ++i)
                if (keep[i])
                    sorted_value[i]->write(buf, true);
            return;
        }

        switch (seq_base_kind)
        {
            case SequenceBase::Head:
                writeVarUInt(size, buf);
                for (size_t i = 0; i < size; ++i)
                    sorted_value[i]->write(buf, full_timestamps);
                break;

            case SequenceBase::Tail:
                writeVarUInt(size, buf);
                for (size_t i = 0; i < size; ++i)
                    sorted_value[n - size + i]->write(buf, full_timestamps);
                break;

            case SequenceBase::FirstMatch:
            case SequenceBase::LastMatch:
                writeVarUInt(n, buf);
                for (auto & node : sorted_value)
                    node->write(buf, full_timestamps);
                break;
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override
    {
        const bool full_timestamps = versioned && version.value_or(getDefaultVersion()) >= 1;
        if (full_timestamps)
        {
            UInt8 flags = 0;
            readBinary(flags, buf);
            if (flags & ~1)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot read sequenceNextNode state: invalid flags {}", static_cast<size_t>(flags));
            data(place).truncated_timestamps = flags & 1;
        }

        readBinary(data(place).sorted, buf);

        UInt64 size = 0;
        readVarUInt(size, buf);

        if (unlikely(size == 0))
            return;

        if (unlikely(size > max_node_size_deserialize))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", max_node_size_deserialize);

        if constexpr (versioned)
            if (!full_timestamps)
                data(place).truncated_timestamps = true;

        auto & value = data(place).value;

        value.resize(size, arena);
        for (UInt64 i = 0; i < size; ++i)
            value[i] = Node::read(buf, arena, full_timestamps);
    }

    std::optional<size_t> getBaseIndex(Data & data) const
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
        const UInt32 unmatched_idx = static_cast<UInt32>(data.value.size());

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
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & value = data(place).value;

        UInt32 event_idx = getNextNodeIndex(this->data(place));
        if (event_idx < value.size())
        {
            ColumnNullable & to_concrete = assert_cast<ColumnNullable &>(to);
            value[event_idx]->insertInto(to_concrete.getNestedColumn());
            to_concrete.getNullMapData().push_back(false);
        }
        else
        {
            to.insertDefault();
        }
    }

    bool allocatesMemoryInArena() const override { return true; }
};


template <typename T>
inline AggregateFunctionPtr createAggregateFunctionSequenceNodeImpl(
    const DataTypePtr data_type, const DataTypes & argument_types, const Array & parameters, SequenceDirection direction, SequenceBase base)
{
    return std::make_shared<SequenceNextNodeImpl<T, NodeString<max_events_size>>>(
        data_type, argument_types, parameters, base, direction, min_required_args);
}

AggregateFunctionPtr
createAggregateFunctionSequenceNode(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings * settings)
{
    if (settings == nullptr || !(*settings)[Setting::enable_funnel_functions])
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function {} is experimental. "
            "Set `enable_funnel_functions` setting to enable it", name);
    }

    if (parameters.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function '{}' requires 2 parameters (direction, head)", name);
    auto expected_param_type = Field::Types::Which::String;
    if (parameters.at(0).getType() != expected_param_type || parameters.at(1).getType() != expected_param_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function '{}' requires 'String' parameters", name);

    String param_dir = parameters.at(0).safeGet<String>();
    UnorderedMapWithMemoryTracking<std::string, SequenceDirection> seq_dir_mapping{
        {"forward", SequenceDirection::Forward},
        {"backward", SequenceDirection::Backward},
    };
    if (!seq_dir_mapping.contains(param_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, param_dir);
    SequenceDirection direction = seq_dir_mapping[param_dir];

    String param_base = parameters.at(1).safeGet<String>();
    UnorderedMapWithMemoryTracking<std::string, SequenceBase> seq_base_mapping{
        {"head", SequenceBase::Head},
        {"tail", SequenceBase::Tail},
        {"first_match", SequenceBase::FirstMatch},
        {"last_match", SequenceBase::LastMatch},
    };
    if (!seq_base_mapping.contains(param_base))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, param_base);
    SequenceBase base = seq_base_mapping[param_base];

    if ((base == SequenceBase::Head && direction == SequenceDirection::Backward) ||
        (base == SequenceBase::Tail && direction == SequenceDirection::Forward))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid argument combination of '{}' with '{}'", param_base, param_dir);

    if (argument_types.size() < min_required_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function {} requires at least {} arguments.", name, toString(min_required_args));

    bool is_base_match_type = base == SequenceBase::FirstMatch || base == SequenceBase::LastMatch;
    if (is_base_match_type && argument_types.size() < min_required_args + 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least {} arguments when base is first_match or last_match.",
            name, toString(min_required_args + 1));

    if (argument_types.size() > max_events_size + min_required_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function '{}' requires at most {} (timestamp, value_column, ...{} events) arguments.",
                name, max_events_size + min_required_args, max_events_size);

    if (const auto * cond_arg = argument_types[2].get(); cond_arg && !isUInt8(cond_arg))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of third argument of aggregate function {}, "
                        "must be UInt8", cond_arg->getName(), name);

    for (const auto i : collections::range(min_required_args, argument_types.size()))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type '{}' of {} argument of aggregate function '{}', must be UInt8", cond_arg->getName(), i + 1, name);
    }

    if (WhichDataType(argument_types[1].get()).idx != TypeIndex::String)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of second argument of aggregate function {}, must be String",
                        argument_types[1]->getName(), name);

    DataTypePtr data_type = makeNullable(argument_types[1]);

    WhichDataType timestamp_type(argument_types[0].get());
    if (timestamp_type.idx == TypeIndex::UInt8)
        return createAggregateFunctionSequenceNodeImpl<UInt8>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt16)
        return createAggregateFunctionSequenceNodeImpl<UInt16>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt32)
        return createAggregateFunctionSequenceNodeImpl<UInt32>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt64)
        return createAggregateFunctionSequenceNodeImpl<UInt64>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.isDate())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDate::FieldType>(data_type, argument_types, parameters, direction, base);
    if (timestamp_type.isDateTime())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDateTime::FieldType>(data_type, argument_types, parameters, direction, base);

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of aggregate function {}, must "
                    "be Unsigned Number, Date, DateTime", argument_types.front()->getName(), name);
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory);
void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNode, {.description = R"DOC(Returns the value of the event that directly follows a matched chain of events, according to the specified direction and base point.)DOC", .category = FunctionDocumentation::Category::AggregateFunction}, properties });
}

}
