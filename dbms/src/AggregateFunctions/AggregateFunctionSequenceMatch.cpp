#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionSequenceMatch.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionSequenceCount(const std::string & name, const DataTypes & argument_types, const Array & /*parameters*/)
{
    if (!AggregateFunctionSequenceCount::sufficientArgs(argument_types.size()))
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionSequenceCount>();
}

AggregateFunctionPtr createAggregateFunctionSequenceMatch(const std::string & name, const DataTypes & argument_types, const Array & /*parameters*/)
{
    if (!AggregateFunctionSequenceMatch::sufficientArgs(argument_types.size()))
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionSequenceMatch>();
}

}

void registerAggregateFunctionsSequenceMatch(AggregateFunctionFactory & factory)
{
    factory.registerFunction("sequenceMatch", createAggregateFunctionSequenceMatch);
    factory.registerFunction("sequenceCount", createAggregateFunctionSequenceCount);
}

}
