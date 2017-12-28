#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionSequenceCount(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    if (params.size() != 1)
        throw Exception{"Aggregate function " + name + " requires exactly one parameter.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String pattern = params.front().safeGet<std::string>();
    return std::make_shared<AggregateFunctionSequenceCount>(argument_types, pattern);
}

AggregateFunctionPtr createAggregateFunctionSequenceMatch(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    if (params.size() != 1)
        throw Exception{"Aggregate function " + name + " requires exactly one parameter.",
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String pattern = params.front().safeGet<std::string>();
    return std::make_shared<AggregateFunctionSequenceMatch>(argument_types, pattern);
}

}

void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sequenceMatch", createAggregateFunctionSequenceMatch);
    factory.registerFunction("sequenceCount", createAggregateFunctionSequenceCount);
}

}
