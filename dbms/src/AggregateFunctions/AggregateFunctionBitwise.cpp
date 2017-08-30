#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitwise.h>
#include <AggregateFunctions/Helpers.h>

namespace DB
{

namespace
{

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitwise(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    if (argument_types.size() != 1)
        throw Exception("Incorrect number of arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionBitwise, Data>(*argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsBitwise(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBitOr", createAggregateFunctionBitwise<AggregateFunctionGroupBitOrData>);
    factory.registerFunction("groupBitAnd", createAggregateFunctionBitwise<AggregateFunctionGroupBitAndData>);
    factory.registerFunction("groupBitXor", createAggregateFunctionBitwise<AggregateFunctionGroupBitXorData>);

    /// Aliases for compatibility with MySQL.
    factory.registerFunction("BIT_OR", createAggregateFunctionBitwise<AggregateFunctionGroupBitOrData>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("BIT_AND", createAggregateFunctionBitwise<AggregateFunctionGroupBitAndData>, AggregateFunctionFactory::CaseInsensitive);
    factory.registerFunction("BIT_XOR", createAggregateFunctionBitwise<AggregateFunctionGroupBitXorData>, AggregateFunctionFactory::CaseInsensitive);
}

}
