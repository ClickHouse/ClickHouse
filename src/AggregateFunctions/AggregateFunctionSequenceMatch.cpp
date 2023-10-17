#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>

#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>

#include <base/range.h>

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
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires exactly one parameter.",
            name);

    const auto arg_count = argument_types.size();

    if (arg_count < 3)
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} requires at least 3 arguments.",
            name);

    if (arg_count - 1 > max_events)
        throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION, "Aggregate function {} supports up to {} event arguments.", name, max_events);

    const auto * time_arg = argument_types.front().get();

    for (const auto i : collections::range(1, arg_count))
    {
        const auto * cond_arg = argument_types[i].get();
        if (!isUInt8(cond_arg))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Illegal type {} of argument {} of aggregate function {}, must be UInt8",
                            cond_arg->getName(), toString(i + 1), name);
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

    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type {} of first argument of aggregate function {}, must be DateTime",
                    time_arg->getName(), name);
}

}

void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sequenceMatch", createAggregateFunctionSequenceBase<AggregateFunctionSequenceMatch, AggregateFunctionSequenceMatchData>);
    factory.registerFunction("sequenceCount", createAggregateFunctionSequenceBase<AggregateFunctionSequenceCount, AggregateFunctionSequenceMatchData>);
}

}
