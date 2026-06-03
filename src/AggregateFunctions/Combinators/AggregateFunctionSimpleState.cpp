#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionSimpleState.h>

namespace DB
{
struct Settings;
namespace
{
    class AggregateFunctionCombinatorSimpleState final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "SimpleState"; }

        DataTypes transformArguments(const DataTypes & arguments) const override { return arguments; }

        AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function,
            const AggregateFunctionProperties &,
            const DataTypes & arguments,
            const Array & params) const override
        {
            return std::make_shared<AggregateFunctionSimpleState>(nested_function, arguments, params);
        }
    };

}

void registerAggregateFunctionCombinatorSimpleState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorSimpleState>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumSimpleState`), it returns the final aggregation result value, but typed as `SimpleAggregateFunction` instead of the plain result type, for storage in engines such as `AggregatingMergeTree`. Unlike the `State` combinator, it does not return a serialized intermediate state; it is only supported for functions whose partial results can be merged by reapplying the function itself.",
        .syntax = "<aggregate_function>SimpleState",
        .related = {"State"}});
}

}
