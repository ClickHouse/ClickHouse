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

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

template <typename T>
inline AggregateFunctionPtr createAggregateFunctionSequenceNodeImpl(const DataTypePtr data_type, const DataTypes & argument_types, bool descending_order)
{
    if (argument_types.size() == 2)
    {
        // If the number of arguments of sequenceNextNode is 2, the sequenceNextNode acts as sequenceFirstNode.
        if (descending_order)
            return std::make_shared<SequenceFirstNodeImpl<T, NodeString, true>>(data_type);
        else
            return std::make_shared<SequenceFirstNodeImpl<T, NodeString, false>>(data_type);
    }
    else
    {
        if (descending_order)
            return std::make_shared<SequenceNextNodeImpl<T, NodeString, true>>(data_type, argument_types);
        else
            return std::make_shared<SequenceNextNodeImpl<T, NodeString, false>>(data_type, argument_types);
    }
}

AggregateFunctionPtr
createAggregateFunctionSequenceNode(const std::string & name, UInt64 max_args, const DataTypes & argument_types, const Array & parameters)
{
    bool descending_order = false;

    if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        bool is_correct_type = type == Field::Types::Int64 || type == Field::Types::UInt64;
        if (!is_correct_type || (parameters[0].get<UInt64>() != 0 && parameters[0].get<UInt64>() != 1))
            throw Exception("The first parameter for aggregate function " + name + " should be 0 or 1", ErrorCodes::BAD_ARGUMENTS);

        descending_order = parameters[0].get<UInt64>();
    }
    else
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() < 2)
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
        return createAggregateFunctionSequenceNodeImpl<UInt8>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt16)
        return createAggregateFunctionSequenceNodeImpl<UInt16>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt32)
        return createAggregateFunctionSequenceNodeImpl<UInt32>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt64)
        return createAggregateFunctionSequenceNodeImpl<UInt64>(data_type, argument_types, descending_order);
    if (timestamp_type.isDate())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDate::FieldType>(data_type, argument_types, descending_order);
    if (timestamp_type.isDateTime())
        return createAggregateFunctionSequenceNodeImpl<DataTypeDateTime::FieldType>(data_type, argument_types, descending_order);

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
