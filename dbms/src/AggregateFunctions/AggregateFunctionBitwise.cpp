#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionBitwise.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitwise(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!argument_types[0]->canBeUsedInBitOperations())
        throw Exception("The type " + argument_types[0]->getName() + " of argument for aggregate function " + name
            + " is illegal, because it cannot be used in bitwise operations",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

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
