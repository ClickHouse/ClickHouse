#include <AggregateFunctions/AggregateFunctionIf.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"
#include "AggregateFunctionNull.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class AggregateFunctionCombinatorIf final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "If"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception("Incorrect number of arguments for aggregate function with " + getName() + " suffix",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (!isUInt8(arguments.back()))
            throw Exception("Illegal type " + arguments.back()->getName() + " of last argument for aggregate function with " + getName() + " suffix",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return DataTypes(arguments.begin(), std::prev(arguments.end()));
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array &) const override
    {
        return std::make_shared<AggregateFunctionIf>(nested_function, arguments);
    }
};

/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable, bool serialize_flag>
class AggregateFunctionIfNullUnary final
    : public AggregateFunctionNullUnaryBase<result_is_nullable, serialize_flag, true, AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>
{
private:
    size_t num_arguments;

    using Base = AggregateFunctionNullUnaryBase<result_is_nullable, serialize_flag, true,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>;
public:

    String getName() const override
    {
        return Base::getName() + "If";
    }

    AggregateFunctionIfNullUnary(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : AggregateFunctionNullUnaryBase<result_is_nullable, serialize_flag, true,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>(std::move(nested_function_), arguments, params),
        num_arguments(arguments.size())
    {
        if (num_arguments == 0)
            throw Exception("Aggregate function " + getName() + " require at least one argument",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    bool singleFilter(const IColumn ** columns, size_t row_num) const override
    {
        const IColumn * filter_column = columns[num_arguments - 1];
        if (const ColumnNullable * nullable_column = typeid_cast<const ColumnNullable *>(filter_column))
            filter_column = nullable_column->getNestedColumnPtr().get();

        return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num];
    }
};

template <bool result_is_nullable, bool serialize_flag, bool null_is_skipped>
class AggregateFunctionIfNullVariadic final
    : public AggregateFunctionNullVariadicBase<result_is_nullable, serialize_flag, null_is_skipped, true,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>
{
private:
    size_t num_arguments;

    using Base = AggregateFunctionNullVariadicBase<result_is_nullable, serialize_flag, null_is_skipped, true,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>;
public:

    String getName() const override
    {
        return Base::getName() + "If";
    }

    AggregateFunctionIfNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : AggregateFunctionNullVariadicBase<result_is_nullable, serialize_flag, null_is_skipped, true,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>(std::move(nested_function_), arguments, params),
          num_arguments(arguments.size())
    {
    }

    bool singleFilter(const IColumn ** columns, size_t row_num) const override
    {
        return assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num];
    }
};


AggregateFunctionPtr AggregateFunctionIf::getOwnNullAdapter(
    const AggregateFunctionPtr & nested_function, const DataTypes & arguments,
    const Array & params, const AggregateFunctionProperties & properties) const
{
    bool return_type_is_nullable = !properties.returns_default_when_only_null && getReturnType()->canBeInsideNullable();
    size_t nullable_size = std::count_if(arguments.begin(), arguments.end(), [](const auto & element) { return element->isNullable(); });
    return_type_is_nullable &= nullable_size != 1 || !arguments.back()->isNullable();   /// If only condition is nullable. we should non-nullable type.
    bool serialize_flag = return_type_is_nullable || properties.returns_default_when_only_null;

    if (arguments.size() <= 2 && arguments.front()->isNullable())
    {
        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionIfNullUnary<true, true>>(nested_func, arguments, params);
        }
        else
        {
            if (serialize_flag)
                return std::make_shared<AggregateFunctionIfNullUnary<false, true>>(nested_func, arguments, params);
            else
                return std::make_shared<AggregateFunctionIfNullUnary<false, false>>(nested_func, arguments, params);
        }
    }
    else
    {
        if (return_type_is_nullable)
        {
            return std::make_shared<AggregateFunctionIfNullVariadic<true, true, true>>(nested_function, arguments, params);
        }
        else
        {
            if (serialize_flag)
                return std::make_shared<AggregateFunctionIfNullVariadic<false, true, true>>(nested_function, arguments, params);
            else
                return std::make_shared<AggregateFunctionIfNullVariadic<false, true, false>>(nested_function, arguments, params);
        }
    }
}

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorIf>());
}

}
