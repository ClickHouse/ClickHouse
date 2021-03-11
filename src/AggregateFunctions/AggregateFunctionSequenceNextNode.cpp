#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceNextNode.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <ext/range.h>


namespace DB
{

constexpr size_t MAX_EVENTS_SIZE = 64;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename T, SeqDirection Direction>
inline AggregateFunctionPtr createAggregateFunctionSequenceNodeImpl2(const DataTypePtr data_type, const DataTypes & argument_types, SeqBase base)
{
    if (base == HEAD)
        return std::make_shared<SequenceNextNodeImpl<T, NodeString<MAX_EVENTS_SIZE>, Direction, HEAD>>(data_type, argument_types);
    else if (base == TAIL)
        return std::make_shared<SequenceNextNodeImpl<T, NodeString<MAX_EVENTS_SIZE>, Direction, TAIL>>(data_type, argument_types);
    else if (base == FIRST_MATCH)
        return std::make_shared<SequenceNextNodeImpl<T, NodeString<MAX_EVENTS_SIZE>, Direction, FIRST_MATCH>>(data_type, argument_types);
    else
        return std::make_shared<SequenceNextNodeImpl<T, NodeString<MAX_EVENTS_SIZE>, Direction, LAST_MATCH>>(data_type, argument_types);
}

template <typename T>
inline AggregateFunctionPtr createAggregateFunctionSequenceNodeImpl1(const DataTypePtr data_type, const DataTypes & argument_types, SeqDirection direction, SeqBase base)
{
    if (direction == FORWARD)
        return createAggregateFunctionSequenceNodeImpl2<T, FORWARD>(data_type, argument_types, base);
    else
        return createAggregateFunctionSequenceNodeImpl2<T, BACKWARD>(data_type, argument_types, base);
}

AggregateFunctionPtr
createAggregateFunctionSequenceNode(const std::string & name, UInt64 max_args, const DataTypes & argument_types, const Array & parameters)
{
    assert(max_args <= MAX_EVENTS_SIZE);

    if (parameters.size() < 2)
        throw Exception("Aggregate function " + name + " requires 2 parameters (direction, head)",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    String param_dir = parameters.at(0).safeGet<String>();
    SeqDirection direction;
    if (param_dir == "forward")
        direction = FORWARD;
    else if (param_dir == "backward")
        direction = BACKWARD;
    else
        throw Exception{"Aggregate function " + name + " doesn't support a parameter: " + param_dir, ErrorCodes::BAD_ARGUMENTS};

    String param_base = parameters.at(1).safeGet<String>();
    SeqBase base;
    if (param_base == "head")
        base = HEAD;
    else if (param_base == "tail")
        base = TAIL;
    else if (param_base == "first_match")
        base = FIRST_MATCH;
    else if (param_base == "last_match")
        base = LAST_MATCH;
    else
        throw Exception{"Aggregate function " + name + " doesn't support a parameter: " + param_base, ErrorCodes::BAD_ARGUMENTS};

    if ((base == FIRST_MATCH || base == LAST_MATCH) && argument_types.size() < 3)
        throw Exception("Aggregate function " + name + " requires at least three arguments when base is first_match or last_match.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    else if (argument_types.size() < 2)
        throw Exception("Aggregate function " + name + " requires at least two arguments.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    else if (argument_types.size() > max_args + 2)
        throw Exception("Aggregate function " + name + " requires at most " +
                            std::to_string(max_args + 2) +
                            " (timestamp, value_column, " +  std::to_string(max_args) + " events) arguments.",
                        ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (const auto i : ext::range(2, argument_types.size()))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) + " of aggregate function "
                    + name + ", must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    if (WhichDataType(argument_types[1].get()).idx != TypeIndex::String)
        throw Exception{"Illegal type " + argument_types[1].get()->getName()
                + " of second argument of aggregate function " + name + ", must be String",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    DataTypePtr data_type = makeNullable(argument_types[1]);

    WhichDataType timestamp_type(argument_types[0].get());
    if (timestamp_type.idx == TypeIndex::UInt8)
        return createAggregateFunctionSequenceNodeImpl1<UInt8>(data_type, argument_types, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt16)
        return createAggregateFunctionSequenceNodeImpl1<UInt16>(data_type, argument_types, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt32)
        return createAggregateFunctionSequenceNodeImpl1<UInt32>(data_type, argument_types, direction, base);
    if (timestamp_type.idx == TypeIndex::UInt64)
        return createAggregateFunctionSequenceNodeImpl1<UInt64>(data_type, argument_types, direction, base);
    if (timestamp_type.isDate())
        return createAggregateFunctionSequenceNodeImpl1<DataTypeDate::FieldType>(data_type, argument_types, direction, base);
    if (timestamp_type.isDateTime())
        return createAggregateFunctionSequenceNodeImpl1<DataTypeDateTime::FieldType>(data_type, argument_types, direction, base);

    throw Exception{"Illegal type " + argument_types.front().get()->getName()
            + " of first argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

auto createAggregateFunctionSequenceNodeMaxArgs(UInt64 max_args)
{
    return [max_args](const std::string & name, const DataTypes & argument_types, const Array & parameters)
    {
        return createAggregateFunctionSequenceNode(name, max_args, argument_types, parameters);
    };
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };
    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNodeMaxArgs(MAX_EVENTS_SIZE), properties });
    factory.registerFunction("sequenceFirstNode", { createAggregateFunctionSequenceNodeMaxArgs(0), properties });
}

}
