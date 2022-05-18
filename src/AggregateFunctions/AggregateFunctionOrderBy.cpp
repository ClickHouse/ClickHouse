#include <AggregateFunctions/AggregateFunctionOrderBy.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/typeid_cast.h>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "OrderBy"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes newArguments(arguments.size());
        for (size_t i = 0; i < arguments.size(); i++) {
            newArguments[i] = arguments[i];
        }
        return newArguments;
    }

    Array transformParameters(const Array & params) const override 
    {
        if (params.empty())
            throw Exception("Incorrect number of parameters for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (params[0].getType() != Field::Types::Which::String)
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return Array(std::next(params.begin()), params.end());
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        return std::make_shared<AggregateFunctionOrderBy>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
}

}
