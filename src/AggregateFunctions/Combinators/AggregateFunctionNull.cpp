#include "AggregateFunctionNull.h"
#include "AggregateFunctionState.h"
#include "AggregateFunctionSimpleState.h"
#include "AggregateFunctionCombinatorFactory.h"

#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <DataTypes/DataTypeNullable.h>


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

    template <typename T>
    std::optional<AggregateFunctionPtr> tryTransformStateFunctionImpl(const AggregateFunctionPtr & nested_function,
                                                       const AggregateFunctionProperties & properties,
                                                       const DataTypes & arguments,
                                                       const Array & params) const
    {
        if (const T * function_state = typeid_cast<const T *>(nested_function.get()))
        {
            auto transformed_nested_function = transformAggregateFunction(function_state->getNestedFunction(), properties, arguments, params);

            return std::make_shared<T>(
                transformed_nested_function,
                transformed_nested_function->getArgumentTypes(),
                transformed_nested_function->getParameters());
        }
        return {};
    }

    AggregateFunctionPtr tryTransformStateFunction(const AggregateFunctionPtr & nested_function,
                                                   const AggregateFunctionProperties & properties,
                                                   const DataTypes & arguments,
                                                   const Array & params) const
    {
        return tryTransformStateFunctionImpl<AggregateFunctionState>(nested_function, properties, arguments, params)
            .or_else([&]() { return tryTransformStateFunctionImpl<AggregateFunctionSimpleState>(nested_function, properties, arguments, params); })
            .value_or(nullptr);
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
            /** Some functions, such as `count`, `uniq`, and others, return 0 :: UInt64 instead of NULL for a NULL argument.
              * These functions have the `returns_default_when_only_null` property, so we explicitly specify the result type
              * when replacing the function with `nothing`.
              *
              * Note: It's a bit dangerous to have the function result type depend on properties because we do not serialize properties in AST,
              * and we can lose this information. For example, when we have `count(NULL)` replaced with `nothing(NULL) as "count(NULL)"` and send it
              * to the remote server, the remote server will execute `nothing(NULL)` and return `NULL` while `0` is expected.
              *
              * To address this, we handle `nothing` in a special way in `FunctionNode::toASTImpl`.
              */
            if (properties.returns_default_when_only_null)
                return std::make_shared<AggregateFunctionNothingUInt64>(arguments, params);
            return std::make_shared<AggregateFunctionNothingNull>(arguments, params);
        }

        assert(nested_function);

        if (auto adapter = nested_function->getOwnNullAdapter(nested_function, arguments, params, properties))
            return adapter;

        /// If applied to aggregate function with either -State/-SimpleState combinator, we apply -Null combinator to it's nested_function instead of itself.
        /// Because Nullable AggregateFunctionState does not make sense and ruins the logic of managing aggregate function states.
        if (const AggregateFunctionPtr new_function = tryTransformStateFunction(nested_function, properties, arguments, params))
        {
            return new_function;
        }

        bool return_type_is_nullable = !properties.returns_default_when_only_null && nested_function->getResultType()->canBeInsideNullable();
        bool serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

        if (arguments.size() == 1)
        {
            if (return_type_is_nullable)
            {
                return std::make_shared<AggregateFunctionNullUnary<true, true>>(nested_function, arguments, params);
            }

            if (serialize_flag)
                return std::make_shared<AggregateFunctionNullUnary<false, true>>(nested_function, arguments, params);
            return std::make_shared<AggregateFunctionNullUnary<false, false>>(nested_function, arguments, params);
        }

        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionNullVariadic<true, true>>(nested_function, arguments, params);
        }

        return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function, arguments, params);
#if 0
                if (serialize_flag)
                    return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function, arguments, params);
                else
                    /// This should be <false, false> (no serialize flag) but it was initially added incorrectly and
                    /// changing it would break the binary compatibility of aggregation states using this method
                    // (such as AggregateFunction(argMaxOrNull, Nullable(Int64), UInt64)). The extra flag is harmless
                    return std::make_shared<AggregateFunctionNullVariadic<false, true>>(nested_function, arguments, params);
            }
#endif
    }
};

}

void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
}

}
