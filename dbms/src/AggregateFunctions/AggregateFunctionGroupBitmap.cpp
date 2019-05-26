#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmap.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

template <template <typename> class Data>
AggregateFunctionPtr createAggregateFunctionBitmap(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    if (!argument_types[0]->canBeUsedInBitOperations())
        throw Exception("The type " + argument_types[0]->getName() + " of argument for aggregate function " + name
            + " is illegal, because it cannot be used in Bitmap operations",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    AggregateFunctionPtr res(createWithUnsignedIntegerType<AggregateFunctionBitmap, Data>(*argument_types[0], argument_types[0]));

    if (!res)
        throw Exception("Illegal type " + argument_types[0]->getName() + " of argument for aggregate function " + name, ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return res;
}

}

void registerAggregateFunctionsBitmap(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupBitmap", createAggregateFunctionBitmap<AggregateFunctionGroupBitmapData>);

}

}
