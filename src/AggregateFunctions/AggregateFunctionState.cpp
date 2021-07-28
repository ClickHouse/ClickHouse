#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include "Documentation/SimpleDocumentation.h"


namespace DB
{

namespace
{

namespace AgrStateDocs
{
const char * doc = R"(
If you apply this combinator, the aggregate function does not return the resulting value (such as the number of unique values for the [uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) function), but an intermediate state of the aggregation (for `uniq`, this is the hash table for calculating the number of unique values). This is an `AggregateFunction(...)` that can be used for further processing or stored in a table to finish aggregating later.

To work with these states, use:

-   [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) table engine.
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) function.
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#runningaccumulate) function.
-   [-Merge](#aggregate_functions_combinators-merge) combinator.
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinator.
)";
}

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
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>(), makeSimpleDocumentation(AgrStateDocs::doc));
}

}
