#include "AggregateFunctionState.h"
#include "AggregateFunctionMerge.h"
#include "AggregateFunctionCombinatorFactory.h"

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
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>());
}

}
