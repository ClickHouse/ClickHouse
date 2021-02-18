#include <AggregateFunctions/AggregateFunctionOrFill.h>

#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"


namespace DB
{

template <bool UseNull>
class AggregateFunctionCombinatorOrFill final : public IAggregateFunctionCombinator
{
public:
    String getName() const override
    {
        if constexpr (UseNull)
            return "OrNull";
        else
            return "OrDefault";
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionOrFill<UseNull>>(
            nested_function,
            arguments,
            params);
    }
};

void registerAggregateFunctionCombinatorOrFill(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrFill<false>>());
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrFill<true>>());
}

}
