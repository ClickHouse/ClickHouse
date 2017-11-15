#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionCount.h>

namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionCount(const std::string & name, const DataTypes & argument_types, const Array & parameters)
{
    return std::make_shared<AggregateFunctionCount>();
}

}

void registerAggregateFunctionCount(AggregateFunctionFactory & factory)
{
    factory.registerFunction("count", createAggregateFunctionCount, AggregateFunctionFactory::CaseInsensitive);
}

AggregateFunctionPtr createAggregateFunctionCountNotNull(const DataTypes & argument_types)
{
    if (argument_types.size() == 1)
        return std::make_shared<AggregateFunctionCountNotNullUnary>();
    return std::make_shared<AggregateFunctionCountNotNullVariadic>();
}

}
