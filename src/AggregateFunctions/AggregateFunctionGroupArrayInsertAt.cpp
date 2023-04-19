#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupArrayInsertAt.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionGroupArrayInsertAt(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertBinary(name, argument_types);

    if (argument_types.size() != 2)
        throw Exception("Aggregate function groupArrayInsertAt requires two arguments.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    return std::make_shared<AggregateFunctionGroupArrayInsertAtGeneric>(argument_types, parameters);
}

}

void registerAggregateFunctionGroupArrayInsertAt(AggregateFunctionFactory & factory)
{
    factory.registerFunction("groupArrayInsertAt", createAggregateFunctionGroupArrayInsertAt);
}

}
