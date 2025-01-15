#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionUniqHyperLogLogPlusPlus.h>
#include <AggregateFunctions/Helpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqHyperLogLogPlusPlus(
    const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    if (argument_types.size() != 1)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function '{}' requires exactly one argument: got {}",
            name,
            argument_types.size());

    WhichDataType which(argument_types[0]);
    if (!which.isUInt64())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Aggregate function '{}' requires an argument of type UInt64, got {}",
            name,
            argument_types[0]->getName());

    return std::make_shared<AggregateFunctionUniqHyperLogLogPlusPlus>(argument_types, params);
}

}

void registerAggregateFunctionUniqHyperLogLogPlusPlus(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true, .is_order_dependent = false};
    factory.registerFunction("uniqHLLPP", {createAggregateFunctionUniqHyperLogLogPlusPlus, properties});
}

}
