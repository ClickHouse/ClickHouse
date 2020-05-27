#include <AggregateFunctions/AggregateFunctionOrderBy.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <Common/typeid_cast.h>
#include "registerAggregateFunctions.h"

namespace DB
{

    namespace ErrorCodes
    {
        extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    }

    class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
    {
    public:
        String getName() const override { return "OrderBy"; }

        DataTypes transformArguments(const DataTypes & arguments, const Array & parameters) const override
        {
            if (arguments.empty())
                throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            return DataTypes(arguments.begin(), arguments.end() - parameters.back().safeGet<UInt64>());
        }

        AggregateFunctionPtr transformAggregateFunction(
                const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & parameters) const override
        {
            return std::make_shared<AggregateFunctionOrderBy>(nested_function, arguments, parameters);
        }

        Array transformParameters(const Array & parameters) const override
        {
            return Array(parameters.begin(), parameters.end() - 1);
        }
    };

    void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
    {
        factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
    }

}
