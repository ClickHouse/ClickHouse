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
        .description = "Like the `State` combinator, but returns the state as a `SimpleAggregateFunction` value where possible.",
        .syntax = "<aggregate_function>SimpleState",
        .related = {"State"}});
}

}
