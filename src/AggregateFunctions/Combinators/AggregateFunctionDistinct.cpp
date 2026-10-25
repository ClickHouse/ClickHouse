#include <AggregateFunctions/Combinators/AggregateFunctionCombinatorFactory.h>
#include <AggregateFunctions/Combinators/AggregateFunctionDistinct.h>

#include <AggregateFunctions/Helpers.h>

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
        const AggregateFunctionProperties & properties,
        const DataTypes & arguments,
        const Array & params) const override
    {
        AggregateFunctionPtr res;
        /// Combinator names retain the aggregate's base name. In particular,
        /// Tuple has multiple nested functions and no getNestedFunction().
        /// Only product needs an ordered distinct history here, because its
        /// Float64 multiplication may change when unique keys are replayed.
        const bool ordered_product = properties.is_order_dependent && nested_function->getName().starts_with("product");

        if (arguments.size() == 1)
        {
            if (ordered_product)
            {
                res.reset(createWithNumericType<
                    AggregateFunctionDistinct,
                    AggregateFunctionDistinctSingleNumericOrderedData>(
                        *arguments[0], nested_function, arguments, params, true));

                if (res)
                    return res;

                if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                    return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctSingleGenericOrderedData<true>>>(
                        nested_function, arguments, params, true);

                return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctSingleGenericOrderedData<false>>>(
                    nested_function, arguments, params, true);
            }

            res.reset(createWithNumericType<
                AggregateFunctionDistinct,
                AggregateFunctionDistinctSingleNumericData>(*arguments[0], nested_function, arguments, params, properties.is_order_dependent));

            if (res)
                return res;

            if (arguments[0]->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return std::make_shared<
                    AggregateFunctionDistinct<
                        AggregateFunctionDistinctSingleGenericData<true>>>(nested_function, arguments, params, properties.is_order_dependent);
            return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctSingleGenericData<false>>>(
                nested_function, arguments, params, properties.is_order_dependent);
        }

        if (ordered_product)
            return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctMultipleGenericOrderedData>>(
                nested_function, arguments, params, true);

        return std::make_shared<AggregateFunctionDistinct<AggregateFunctionDistinctMultipleGenericData>>(
            nested_function, arguments, params, properties.is_order_dependent);
    }
};

}

void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory & factory);
void registerAggregateFunctionCombinatorDistinct(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorDistinct>(), Documentation{
        .description = "Applied as a suffix to an aggregate function name (e.g. `sumDistinct`), it aggregates only distinct argument values.",
        .syntax = "<aggregate_function>Distinct",
        .related = {}});
}

}
