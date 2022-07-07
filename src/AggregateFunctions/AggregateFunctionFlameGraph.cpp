#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFlameGraph.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionAllocationsFlameGraph(const std::string &, const DataTypes & argument_types, const Array &, const Settings *)
{
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, false);
}

AggregateFunctionPtr createAggregateFunctionAllocationsTree(const std::string &, const DataTypes & argument_types, const Array &, const Settings *)
{
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, true);
}

void registerAggregateFunctionFlameGraph(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("allocationsFlameGraph", { createAggregateFunctionAllocationsFlameGraph, properties });
    factory.registerFunction("allocationsTree", { createAggregateFunctionAllocationsTree, properties });
}

}
