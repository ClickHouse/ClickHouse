#include <AggregateFunctions/AggregateFunctionOrderBy.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <memory>
#include <iterator>

namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class AggregateFunctionCombinatorOrderBy final : public IAggregateFunctionCombinator
{
private:
    mutable size_t number_of_args_to_sort = 0;

    size_t parse(const String& s) const
    {
        size_t res = 1;
        for (auto symb : s) {
            if (symb == ',') {
                res += 1;
            } 
        }
        return res;
    }

public:
    String getName() const override { return "OrderBy"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.size() < number_of_args_to_sort)
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        return DataTypes(arguments.begin(), std::prev(arguments.end(), number_of_args_to_sort));
    }

    Array transformParameters(const Array & parameters) const override 
    {
        if (parameters.empty())
            throw Exception("Incorrect number of parameters for aggregate function with " + getName() + " suffix",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        if (parameters[0].getType() != Field::Types::Which::String)
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        number_of_args_to_sort = parse(parameters[0].get<String>());
        return Array(std::next(parameters.begin()), parameters.end());
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


void registerAggregateFunctionCombinatorOrderBy(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorOrderBy>());
}

}
