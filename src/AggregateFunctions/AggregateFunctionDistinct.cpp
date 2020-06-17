#include <AggregateFunctions/AggregateFunctionDistinct.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <Common/typeid_cast.h>
#include "registerAggregateFunctions.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
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

        return arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
            const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
    {
        AggregateFunctionPtr res;
        if (arguments.size() == 1)
        {
            res = AggregateFunctionPtr(createWithNumericType<AggregateFunctionDistinctSingleNumericImpl>(*arguments[0], nested_function, arguments));
            if (res)
                return res;

            if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return std::make_shared<AggregateFunctionDistinctSingleGenericImpl<true>>(nested_function, arguments);
            else
                return std::make_shared<AggregateFunctionDistinctSingleGenericImpl<false>>(nested_function, arguments);
        }

        if (!res)
            throw Exception("Illegal type " /* + argument_type->getName() + */
                            " of argument for aggregate function " + nested_function->getName() + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return res;
    }
};

void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinct>());
}

}
