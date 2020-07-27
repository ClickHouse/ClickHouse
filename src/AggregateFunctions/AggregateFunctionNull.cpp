#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class AggregateFunctionCombinatorNull final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Null"; }

    bool isForInternalUsageOnly() const override { return true; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        DataTypes res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = removeNullable(arguments[i]);
        return res;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array & params) const override
    {
        bool has_nullable_types = false;
        bool has_null_types = false;
        for (const auto & arg_type : arguments)
        {
            if (arg_type->isNullable())
            {
                has_nullable_types = true;
                if (arg_type->onlyNull())
                {
                    has_null_types = true;
                    break;
                }
            }
        }

        if (!has_nullable_types)
            throw Exception("Aggregate function combinator 'Null' requires at least one argument to be Nullable", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        /// Special case for 'count' function. It could be called with Nullable arguments
        /// - that means - count number of calls, when all arguments are not NULL.
        if (nested_function && nested_function->getName() == "count")
            return std::make_shared<AggregateFunctionCountNotNullUnary>(arguments[0], params);

        if (has_null_types)
            return std::make_shared<AggregateFunctionNothing>(arguments, params);

        /// If applied to aggregate function with -State combinator, we apply -Null combinator to it's nested_function instead of itself.
        /// Because Nullable AggregateFunctionState does not make sense and ruins the logic of managing aggregate function states.

        if (const AggregateFunctionState * function_state = typeid_cast<const AggregateFunctionState *>(nested_function.get()))
        {
            auto transformed_nested_function = transformAggregateFunction(function_state->getNestedFunction(), properties, arguments, params);

            return std::make_shared<AggregateFunctionState>(
                transformed_nested_function,
                transformed_nested_function->getArgumentTypes(),
                transformed_nested_function->getParameters());
        }

        bool return_type_is_nullable = nested_function->getReturnType()->canBeInsideNullable();
        bool serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

        if (arguments.size() == 1)
        {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullUnary<true>>(nested_function, arguments, params);
            else
                return std::make_shared<AggregateFunctionNullUnary<false>>(nested_function, arguments, params);
        }
        else
        {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullVariadic<true>>(nested_function, arguments, params);
            else
                return std::make_shared<AggregateFunctionNullVariadic<false>>(nested_function, arguments, params);
        }
    }
};

void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
}

}
