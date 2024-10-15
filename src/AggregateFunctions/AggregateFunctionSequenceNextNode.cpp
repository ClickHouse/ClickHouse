#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnNullable.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <base/range.h>

#include <bitset>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_funnel_functions;
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
        if (unlikely(size > max_node_size_deserialize))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE, "Too large node state size");

        Node * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node) + size, alignof(Node)));
        node->size = size;
        buf.readStrict(node->data(), size);

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
    static Data & data(AggregateDataPtr __restrict place) { return *reinterpret_cast<Data *>(place); }
    static const Data & data(ConstAggregateDataPtr __restrict place) { return *reinterpret_cast<const Data *>(place); }

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
        : IAggregateFunctionDataHelper<SequenceNextNodeGeneralData<Node>, Self>(arguments, parameters_, data_type_)
        , seq_base_kind(seq_base_kind_)
        , seq_direction(seq_direction_)
        , min_required_args(min_required_args_)
        , data_type(this->argument_types[0])
        , events_size(arguments.size() - min_required_args)
        , max_elems(max_elems_)
    {
    }

    String getName() const override { return "sequenceNextNode"; }

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
        node->event_time = static_cast<DataTypeDateTime::FieldType>(timestamp);

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

        if (unlikely(size > max_node_size_deserialize))
            throw Exception(ErrorCodes::TOO_LARGE_ARRAY_SIZE,
                            "Too large array size (maximum: {})", max_node_size_deserialize);

        auto & value = data(place).value;

        value.resize(size, arena);
        for (UInt64 i = 0; i < size; ++i)
            value[i] = Node::read(buf, arena);
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
            to_concrete.getNullMapData().push_back(0);
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
    if (settings == nullptr || !(*settings)[Setting::allow_experimental_funnel_functions])
    {
        throw Exception(ErrorCodes::UNKNOWN_AGGREGATE_FUNCTION, "Aggregate function {} is experimental. "
            "Set `allow_experimental_funnel_functions` setting to enable it", name);
    }

    if (parameters.size() < 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Aggregate function '{}' requires 2 parameters (direction, head)", name);
    auto expected_param_type = Field::Types::Which::String;
    if (parameters.at(0).getType() != expected_param_type || parameters.at(1).getType() != expected_param_type)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function '{}' requires 'String' parameters", name);

    String param_dir = parameters.at(0).safeGet<String>();
    std::unordered_map<std::string, SequenceDirection> seq_dir_mapping{
        {"forward", SequenceDirection::Forward},
        {"backward", SequenceDirection::Backward},
    };
    if (!seq_dir_mapping.contains(param_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} doesn't support a parameter: {}", name, param_dir);
    SequenceDirection direction = seq_dir_mapping[param_dir];

    String param_base = parameters.at(1).safeGet<String>();
    std::unordered_map<std::string, SequenceBase> seq_base_mapping{
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
                        argument_types[1].get()->getName(), name);

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
                    "be Unsigned Number, Date, DateTime", argument_types.front().get()->getName(), name);
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNode, properties });
}

}
