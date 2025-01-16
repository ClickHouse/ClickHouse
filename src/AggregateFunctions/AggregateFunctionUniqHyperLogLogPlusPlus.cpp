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

namespace
{

AggregateFunctionPtr createAggregateFunctionUniqHyperLogLogPlusPlus(
    const std::string & , const DataTypes & argument_types, const Array & params, const Settings *)
{
    return std::make_shared<AggregateFunctionUniqHyperLogLogPlusPlus>(argument_types, params);
}

}

void registerAggregateFunctionUniqHyperLogLogPlusPlus(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true, .is_order_dependent = false};
    factory.registerFunction("uniqHLLPP", {createAggregateFunctionUniqHyperLogLogPlusPlus, properties});
}

}
