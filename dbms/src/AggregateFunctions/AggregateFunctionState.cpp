#include <AggregateFunctions/AggregateFunctionMerge.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <DataTypes/DataTypeAggregateFunction.h>


namespace DB
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
        const AggregateFunctionPtr & nested_function, const DataTypes &, const Array &) const override
    {
        auto ret = nested_function->getInstance();
        ret->setState(true);
        return ret;
    }
};

void registerAggregateFunctionCombinatorState(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorState>());
}

}
