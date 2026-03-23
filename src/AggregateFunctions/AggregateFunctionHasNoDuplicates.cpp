#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionHasNoDuplicates.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionHasNoDuplicates(
    const String & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires at least one argument",
            name);

    if (!parameters.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} does not accept parameters",
            name);

    return std::make_shared<AggregateFunctionHasNoDuplicates>(argument_types);
}

}

void registerAggregateFunctionHasNoDuplicates(AggregateFunctionFactory & factory)
{
    factory.registerFunction("__hasNoDuplicates", createAggregateFunctionHasNoDuplicates);
}

}
