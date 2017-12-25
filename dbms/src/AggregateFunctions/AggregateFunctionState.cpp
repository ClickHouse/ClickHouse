#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

class AggregateFunctionCombinatorState final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "State"; };

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        return arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params) const override
    {
        return std::make_shared<AggregateFunctionState>(nested_function, arguments, params);
    }
};

void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>());
}


DataTypePtr AggregateFunctionState::getReturnType() const
{
    auto ptr = std::make_shared<DataTypeAggregateFunction>(nested_func, arguments, params);

    /// Special case: it is -MergeState combinator.
    /// We must return AggregateFunction(agg, ...) instead of AggregateFunction(aggMerge, ...)
    if (typeid_cast<const AggregateFunctionMerge *>(ptr->getFunction().get()))
    {
        if (arguments.size() != 1)
            throw Exception("Combinator -MergeState expects only one argument", ErrorCodes::BAD_ARGUMENTS);

        if (!typeid_cast<const DataTypeAggregateFunction *>(arguments[0].get()))
            throw Exception("Combinator -MergeState expects argument with AggregateFunction type", ErrorCodes::BAD_ARGUMENTS);

        return arguments[0];
    }

    return ptr;
}

}
