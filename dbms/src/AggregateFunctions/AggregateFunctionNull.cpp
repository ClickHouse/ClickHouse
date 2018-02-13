#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/AggregateFunctionNull.h>
#include <AggregateFunctions/AggregateFunctionNothing.h>
#include <AggregateFunctions/AggregateFunctionCount.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

class AggregateFunctionCombinatorNull final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Null"; };

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        size_t size = arguments.size();
        DataTypes res(size);
        for (size_t i = 0; i < size; ++i)
            res[i] = removeNullable(arguments[i]);
        return res;
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function, const DataTypes & arguments, const Array &) const override
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
        {
            if (arguments.size() == 1)
                return std::make_shared<AggregateFunctionCountNotNullUnary>(arguments[0]);
            else
                return std::make_shared<AggregateFunctionCountNotNullVariadic>(arguments);
        }

        if (has_null_types)
            return std::make_shared<AggregateFunctionNothing>();

        bool return_type_is_nullable = nested_function->getReturnType()->canBeInsideNullable();

        if (arguments.size() == 1)
        {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullUnary<true>>(nested_function);
            else
                return std::make_shared<AggregateFunctionNullUnary<false>>(nested_function);
        }
        else
        {
            if (return_type_is_nullable)
                return std::make_shared<AggregateFunctionNullVariadic<true>>(nested_function, arguments);
            else
                return std::make_shared<AggregateFunctionNullVariadic<false>>(nested_function, arguments);
        }
    }
};

void registerAggregateFunctionCombinatorNull(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorNull>());
}

}
