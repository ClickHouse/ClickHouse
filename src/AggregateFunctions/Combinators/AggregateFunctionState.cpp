#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionMerge.h>
#include <AggregateFunctions/Combinators/AggregateFunctionState.h>

#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
}

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
        /// Disallow applying `-State` on top of an aggregate function that
        /// already returns a state (either via another `-State` directly, or
        /// transitively through combinators that propagate `isState()`, such
        /// as `-OrDefault`, `-OrNull`, `-If`, `-Array`, `-ForEach`, etc.).
        ///
        /// `AggregateFunctionState::insertResultInto` pushes the raw state
        /// pointer into a `ColumnAggregateFunction` and relies on the caller
        /// to keep the underlying memory alive. Stacking `-State` on a
        /// state-returning chain produces patterns whose result column is a
        /// column of states pointing at a *different* state's representation,
        /// which has no consistent memory-ownership story — and consumers such
        /// as `runningAccumulate`, where the accumulator lives on the stack,
        /// then dereference dangling pointers. The duplicate-combinator name
        /// check in `AggregateFunctionFactory::get` only catches the
        /// immediate `StateState` case, so this stricter rule is enforced
        /// here for any nesting depth.
        if (nested_function->isState())
            throw Exception(
                ErrorCodes::ILLEGAL_AGGREGATION,
                "`-State` combinator cannot be applied to aggregate function `{}` "
                "because it already returns a state (its underlying chain contains "
                "another `-State` combinator).",
                nested_function->getName());

        return std::make_shared<AggregateFunctionState>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>());
}

}
