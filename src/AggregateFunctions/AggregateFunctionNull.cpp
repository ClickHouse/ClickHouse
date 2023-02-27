#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionState.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

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
        {
            /// Nullable(Nothing) is processed separately, don't convert it to Nothing.
            if (arguments[i]->onlyNull())
                res[i] = arguments[i];
            else
                res[i] = removeNullable(arguments[i]);
        }
        return res;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties & properties,
        const DataTypes & arguments,
        const Array & params) const override
    {
        bool has_nullable_types = false;
        bool has_null_types = false;
        std::unordered_set<size_t> arguments_that_can_be_only_null;
        if (nested_function)
            arguments_that_can_be_only_null = nested_function->getArgumentsThatCanBeOnlyNull();

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i]->isNullable())
            {
                has_nullable_types = true;
                if (arguments[i]->onlyNull() && !arguments_that_can_be_only_null.contains(i))
                {
                    has_null_types = true;
                    break;
                }
            }
        }

        if (!has_nullable_types)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function combinator 'Null' "
                            "requires at least one argument to be Nullable");

        if (has_null_types)
        {
            /// Currently the only functions that returns not-NULL on all NULL arguments are count and uniq, and they returns UInt64.
            if (properties.returns_default_when_only_null)
                return std::make_shared<AggregateFunctionNothing>(arguments, params, std::make_shared<DataTypeUInt64>());
            else
                return std::make_shared<AggregateFunctionNothing>(arguments, params, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()));
        }

        assert(nested_function);

        if (auto adapter = nested_function->getOwnNullAdapter(nested_function, arguments, params, properties))
            return adapter;

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

        bool return_type_is_nullable = !properties.returns_default_when_only_null && nested_function->getResultType()->canBeInsideNullable();
        bool serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

        if (arguments.size() == 1)
        {
            if (return_type_is_nullable)
            {
                return std::make_shared<AggregateFunctionNullUnary<true, true>>(nested_function, arguments, params);
            }
            else
            {
                if (serialize_flag)
                    return std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, arguments, params);
                else
                    return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function, arguments, params);
            }
        }
        else
        {
            if (return_type_is_nullable)
            {
                return std::make_shared<AggregateFunctionNullVariadic<true, true>>(nested_function, arguments, params);
            }
            else
            {
                if (serialize_flag)
                    return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function, arguments, params);
                else
                    return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function, arguments, params);
            }
        }
    }
};

}

void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
}

}
