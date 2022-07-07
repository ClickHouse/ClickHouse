#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionFlameGraph.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{

static void check(const std::string & name, const DataTypes & argument_types, const Array & params)
{
    assertNoParameters(name, params);

    if (argument_types.size() != 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 3 arguments : trace, size, ptr",
            name);

    auto ptr_type = std::make_shared<DataTypeUInt64>();
    auto trace_type = std::make_shared<DataTypeArray>(ptr_type);
    auto size_type = std::make_shared<DataTypeInt64>();

    if (!argument_types[0]->equals(*trace_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument (trace) for function {} must be Array(UInt64), but it has type {}",
            name, argument_types[0]->getName());

    if (!argument_types[1]->equals(*size_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument (size) for function {} must be Int64, but it has type {}",
            name, argument_types[1]->getName());

    if (!argument_types[2]->equals(*ptr_type))
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Third argument (ptr) for function {} must be UInt64, but it has type {}",
            name, argument_types[2]->getName());
}

AggregateFunctionPtr createAggregateFunctionAllocationsFlameGraph(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, false);
}

AggregateFunctionPtr createAggregateFunctionAllocationsTree(const std::string & name, const DataTypes & argument_types, const Array & params, const Settings *)
{
    check(name, argument_types, params);
    return std::make_shared<AggregateFunctionFlameGraph>(argument_types, true);
}

void registerAggregateFunctionFlameGraph(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true, .is_order_dependent = false };

    factory.registerFunction("allocationsFlameGraph", { createAggregateFunctionAllocationsFlameGraph, properties });
    factory.registerFunction("allocationsTree", { createAggregateFunctionAllocationsTree, properties });
}

}
