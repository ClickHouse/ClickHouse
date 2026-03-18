#include "AggregateFunctionOrFill.h"
#include "AggregateFunctionCombinatorFactory.h"

namespace DB
{

namespace
{

enum class Kind : uint8_t
{
    OrNull,
    OrDefault
};

class AggregateFunctionCombinatorOrFill final : public IAggregateFunctionCombinator
{
private:
    Kind kind;

public:
    explicit AggregateFunctionCombinatorOrFill(Kind kind_) : kind(kind_) {}

    /// Due to aggregate_functions_null_for_empty
    bool supportsNesting() const override { return true; }

    String getName() const override
    {
        return kind == Kind::OrNull ? "OrNull" : "OrDefault";
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        if (kind == Kind::OrNull)
            return std::make_shared<AggregateFunctionOrFill<true>>(nested_function, arguments, params);
        return std::make_shared<AggregateFunctionOrFill<false>>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorOrFill(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrFill>(Kind::OrNull));
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrFill>(Kind::OrDefault));
}

}
