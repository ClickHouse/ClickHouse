#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionMerge.h>
#include <AggregateFunctions/Combinators/AggregateFunctionState.h>

#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace
{

class AggregateFunctionCombinatorState final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "State"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        return arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionState>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `uniqState`), it returns the intermediate aggregation state (an `AggregateFunction` value) instead of the final result, for use with materialized views and `AggregatingMergeTree`.",
        .syntax = "<aggregate_function>State",
        .related = {"Merge", "SimpleState"}});
}

}
