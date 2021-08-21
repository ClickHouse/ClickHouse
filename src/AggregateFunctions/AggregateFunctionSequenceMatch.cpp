#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>

#include <common/range.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <template <typename, typename> typename AggregateFunction, template <typename> typename Data>
AggregateFunctionPtr createAggregateFunctionSequenceBase(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (params.size() != 1)
        throw Exception{"Aggregate function " + name + " requires exactly one parameter.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    const auto arg_count = argument_types.size();

    if (arg_count < 3)
        throw Exception{"Aggregate function " + name + " requires at least 3 arguments.",
            ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION};

    if (arg_count - 1 > max_events)
        throw Exception{"Aggregate function " + name + " supports up to "
            + toString(max_events) + " event arguments.",
            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION};

    const auto * time_arg = argument_types.front().get();

    for (const auto i : collections::range(1, arg_count))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1)
                + " of aggregate function " + name + ", must be UInt8",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    String pattern = params.front().safeGet<std::string>();

    AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunction, Data>(*argument_types[0], argument_types, params, pattern));
    if (res)
        return res;

    WhichDataType which(argument_types.front().get());
    if (which.isDateTime())
        return std::make_shared<AggregateFunction<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>>>(argument_types, params, pattern);
    else if (which.isDate())
        return std::make_shared<AggregateFunction<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>>>(argument_types, params, pattern);

    throw Exception{"Illegal type " + time_arg->getName() + " of first argument of aggregate function "
            + name + ", must be DateTime",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sequenceMatch", createAggregateFunctionSequenceBase<AggregateFunctionSequenceMatch, AggregateFunctionSequenceMatchData>);
    factory.registerFunction("sequenceCount", createAggregateFunctionSequenceBase<AggregateFunctionSequenceCount, AggregateFunctionSequenceMatchData>);
}

}
