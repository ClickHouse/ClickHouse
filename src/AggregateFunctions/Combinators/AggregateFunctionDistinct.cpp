#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionDistinct.h>

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

class AggregateFunctionCombinatorDistinct final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Distinct"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Incorrect number of arguments for aggregate function with {} suffix", getName());

        return arguments;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {
        AggregateFunctionPtr res;
        if (arguments.size() == 1)
        {
            res.reset(createWithNumericType<
                AggregateFunctionDistinct,
                AggregateFunctionDistinctSingleNumericData>(*arguments[0], nested_function, arguments, params));

            if (res)
                return res;

            if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return std::make_shared<
                    AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<true>>>(nested_function, arguments, params);
            return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctSingleGenericData<false>>>(
                nested_function, arguments, params);
        }

        return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctMultipleGenericData>>(nested_function, arguments, params);
    }
};

}

void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinct>());
}

}
