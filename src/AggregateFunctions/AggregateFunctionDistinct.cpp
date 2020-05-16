#include <AggregateFunctions/AggregateFunctionDistinct.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Common/typeid_cast.h>
#include "registerAggregateFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorDistinct final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Distinct"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_arguments;
        for (const auto & type : arguments)
        {
            nested_arguments.push_back(type);
        }

        return nested_arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
    {
        return std::make_shared<AggregateFunctionDistinct>(nested_function, arguments);
    }
};

void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinct>());
}

}
