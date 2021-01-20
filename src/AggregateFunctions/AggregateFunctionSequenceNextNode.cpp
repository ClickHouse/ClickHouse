#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceNextNode.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <ext/range.h>
#include "registerAggregateFunctions.h"


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

template <typename TYPE>
inline AggregateFunctionPtr createAggregateFunctionSequenceNextNodeImpl(const DataTypePtr data_type, const DataTypes & argument_types, bool descending_order)
{
    if (descending_order)
        return std::make_shared<SequenceNextNodeImpl<TYPE, NodeString, true>>(data_type, argument_types);
    else
        return std::make_shared<SequenceNextNodeImpl<TYPE, NodeString, false>>(data_type, argument_types);
}

AggregateFunctionPtr createAggregateFunctionSequenceNextNode(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    bool descending_order = false;

    if (parameters.size() == 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception("The first parameter for aggregate function " + name + " should be 0 or 1", ErrorCodes::BAD_ARGUMENTS);

        descending_order = parameters[0].get<UInt64>();
    }
    else
        throw Exception("Incorrect number of parameters for aggregate function " + name + ", should be 1",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (argument_types.size() < 3)
        throw Exception("Aggregate function " + name + " requires at least three arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    else if (argument_types.size() > 2 + 64)
        throw Exception("Aggregate function " + name + " requires at most 66(timestamp, value_column, 64 events) arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (const auto i : ext::range(2, argument_types.size()))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) + " of aggregate function "
                    + name + ", must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    if (WhichDataType(argument_types[1].get()).idx != TypeIndex::String)
        throw Exception{"Illegal type " + argument_types.front().get()->getName()
                + " of second argument of aggregate function " + name + ", must be String",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    DataTypePtr data_type;
    if (typeid_cast<const DataTypeNullable *>(argument_types[1].get()))
        data_type = argument_types[1];
    else
        data_type = std::make_shared<DataTypeNullable>(argument_types[1]);

    WhichDataType timestamp_type(argument_types[0].get());
    if (timestamp_type.idx == TypeIndex::UInt8)
        return createAggregateFunctionSequenceNextNodeImpl<UInt8>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt16)
        return createAggregateFunctionSequenceNextNodeImpl<UInt16>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt32)
        return createAggregateFunctionSequenceNextNodeImpl<UInt32>(data_type, argument_types, descending_order);
    if (timestamp_type.idx == TypeIndex::UInt64)
        return createAggregateFunctionSequenceNextNodeImpl<UInt64>(data_type, argument_types, descending_order);
    if (timestamp_type.isDate())
        return createAggregateFunctionSequenceNextNodeImpl<DataTypeDate::FieldType>(data_type, argument_types, descending_order);
    if (timestamp_type.isDateTime())
        return createAggregateFunctionSequenceNextNodeImpl<DataTypeDateTime::FieldType>(data_type, argument_types, descending_order);

    throw Exception{"Illegal type " + argument_types.front().get()->getName()
            + " of first argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionSequenceNextNode(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("sequenceNextNode", { createAggregateFunctionSequenceNextNode, properties });
}

}
