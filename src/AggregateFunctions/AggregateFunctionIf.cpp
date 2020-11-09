#include <AggregateFunctions/AggregateFunctionIf.h>
#include <AggregateFunctions/AggregateFunctionCombinatorFactory.h>
#include "registerAggregateFunctions.h"
#include "AggregateFunctionNull.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
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
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>
{
private:
    size_t num_arguments;

    using Base = AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullUnary<result_is_nullable, serialize_flag>>;
public:

    String getName() const override
    {
        return Base::getName();
    }

    AggregateFunctionIfNullUnary(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params), num_arguments(arguments.size())
    {
        if (num_arguments == 0)
            throw Exception("Aggregate function " + getName() + " require at least one argument",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }

    static inline bool singleFilter(const IColumn ** columns, size_t row_num, size_t num_arguments)
    {
        const IColumn * filter_column = columns[num_arguments - 1];
        if (const ColumnNullable * nullable_column = typeid_cast<const ColumnNullable *>(filter_column))
            filter_column = nullable_column->getNestedColumnPtr().get();

        return assert_cast<const ColumnUInt8 &>(*filter_column).getData()[row_num];
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const ColumnNullable * column = assert_cast<const ColumnNullable *>(columns[0]);
        const IColumn * nested_column = &column->getNestedColumn();
        if (!column->isNullAt(row_num) && singleFilter(columns, row_num, num_arguments))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
        }
    }
};

template <bool result_is_nullable, bool serialize_flag, bool null_is_skipped>
class AggregateFunctionIfNullVariadic final
    : public AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>
{
public:

    String getName() const override
    {
        return Base::getName();
    }

    AggregateFunctionIfNullVariadic(AggregateFunctionPtr nested_function_, const DataTypes & arguments, const Array & params)
        : Base(std::move(nested_function_), arguments, params), number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionIfNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    static inline bool singleFilter(const IColumn ** columns, size_t row_num, size_t num_arguments)
    {
        return assert_cast<const ColumnUInt8 &>(*columns[num_arguments - 1]).getData()[row_num];
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = assert_cast<const ColumnNullable &>(*columns[i]);
                if (null_is_skipped && nullable_col.isNullAt(row_num))
                {
                    /// If at least one column has a null value in the current row,
                    /// we don't process this row.
                    return;
                }
                nested_columns[i] = &nullable_col.getNestedColumn();
            }
            else
                nested_columns[i] = columns[i];
        }

        if (singleFilter(nested_columns, row_num, number_of_arguments))
        {
            this->setFlag(place);
            this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
        }
    }

private:
    using Base = AggregateFunctionNullBase<result_is_nullable, serialize_flag,
        AggregateFunctionIfNullVariadic<result_is_nullable, serialize_flag, null_is_skipped>>;

    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
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
                return std::make_shared<AggregateFunctionIfNullVariadic<false, false, true>>(nested_function, arguments, params);
        }
    }
}

void registerAggregateFunctionCombinatorIf(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorIf>());
}

}
