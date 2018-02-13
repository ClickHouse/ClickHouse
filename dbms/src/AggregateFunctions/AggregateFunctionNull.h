#pragma once

#include <array>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// This class implements a wrapper around an aggregate function. Despite its name,
/// this is an adapter. It is used to handle aggregate functions that are called with
/// at least one nullable argument. It implements the logic according to which any
/// row that contains at least one NULL is skipped.

/// If all rows had NULL, the behaviour is determined by "result_is_nullable" template parameter.
///  true - return NULL; false - return value from empty aggregation state of nested function.

template <bool result_is_nullable, typename Derived>
class AggregateFunctionNullBase : public IAggregateFunctionHelper<Derived>
{
protected:
    AggregateFunctionPtr nested_function;

    /** In addition to data for nested aggregate function, we keep a flag
      *  indicating - was there at least one non-NULL value accumulated.
      * In case of no not-NULL values, the function will return NULL.
      */

    static AggregateDataPtr nestedPlace(AggregateDataPtr place) noexcept
    {
        return place + (result_is_nullable ? 1 : 0);
    }

    static ConstAggregateDataPtr nestedPlace(ConstAggregateDataPtr place) noexcept
    {
        return place + (result_is_nullable ? 1 : 0);
    }

    static void initFlag(AggregateDataPtr place) noexcept
    {
        if (result_is_nullable)
            place[0] = 0;
    }

    static void setFlag(AggregateDataPtr place) noexcept
    {
        if (result_is_nullable)
            place[0] = 1;
    }

    static bool getFlag(ConstAggregateDataPtr place) noexcept
    {
        return result_is_nullable ? place[0] : 1;
    }

public:
    AggregateFunctionNullBase(AggregateFunctionPtr nested_function_)
        : nested_function{nested_function_}
    {
    }

    String getName() const override
    {
        /// This is just a wrapper. The function for Nullable arguments is named the same as the nested function itself.
        return nested_function->getName();
    }

    DataTypePtr getReturnType() const override
    {
        return result_is_nullable
            ? makeNullable(nested_function->getReturnType())
            : nested_function->getReturnType();
    }

    void create(AggregateDataPtr place) const override
    {
        initFlag(place);
        nested_function->create(nestedPlace(place));
    }

    void destroy(AggregateDataPtr place) const noexcept override
    {
        nested_function->destroy(nestedPlace(place));
    }

    bool hasTrivialDestructor() const override
    {
        return nested_function->hasTrivialDestructor();
    }

    size_t sizeOfData() const override
    {
        return 1 + nested_function->sizeOfData();
    }

    size_t alignOfData() const override
    {
        return 1;    /// NOTE This works fine on x86_64 and ok on AArch64. Doesn't work under UBSan.
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        if (result_is_nullable && getFlag(rhs))
            setFlag(place);

        nested_function->merge(nestedPlace(place), nestedPlace(rhs), arena);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        bool flag = getFlag(place);
        if (result_is_nullable)
            writeBinary(flag, buf);
        if (flag)
            nested_function->serialize(nestedPlace(place), buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena * arena) const override
    {
        bool flag = 1;
        if (result_is_nullable)
            readBinary(flag, buf);
        if (flag)
        {
            setFlag(place);
            nested_function->deserialize(nestedPlace(place), buf, arena);
        }
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        if (result_is_nullable)
        {
            ColumnNullable & to_concrete = static_cast<ColumnNullable &>(to);
            if (getFlag(place))
            {
                nested_function->insertResultInto(nestedPlace(place), to_concrete.getNestedColumn());
                to_concrete.getNullMapData().push_back(0);
            }
            else
            {
                to_concrete.insertDefault();
            }
        }
        else
        {
            nested_function->insertResultInto(nestedPlace(place), to);
        }
    }

    bool allocatesMemoryInArena() const override
    {
        return nested_function->allocatesMemoryInArena();
    }

    bool isState() const override
    {
        return nested_function->isState();
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/** There are two cases: for single argument and variadic.
  * Code for single argument is much more efficient.
  */
template <bool result_is_nullable>
class AggregateFunctionNullUnary final : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullUnary<result_is_nullable>>
{
public:
    AggregateFunctionNullUnary(AggregateFunctionPtr nested_function)
        : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullUnary<result_is_nullable>>(nested_function)
    {
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const ColumnNullable * column = static_cast<const ColumnNullable *>(columns[0]);
        if (!column->isNullAt(row_num))
        {
            this->setFlag(place);
            const IColumn * nested_column = &column->getNestedColumn();
            this->nested_function->add(this->nestedPlace(place), &nested_column, row_num, arena);
        }
    }
};


template <bool result_is_nullable>
class AggregateFunctionNullVariadic final : public AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>
{
public:
    AggregateFunctionNullVariadic(AggregateFunctionPtr nested_function, const DataTypes & arguments)
        : AggregateFunctionNullBase<result_is_nullable, AggregateFunctionNullVariadic<result_is_nullable>>(nested_function),
        number_of_arguments(arguments.size())
    {
        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        /// This container stores the columns we really pass to the nested function.
        const IColumn * nested_columns[number_of_arguments];

        for (size_t i = 0; i < number_of_arguments; ++i)
        {
            if (is_nullable[i])
            {
                const ColumnNullable & nullable_col = static_cast<const ColumnNullable &>(*columns[i]);
                if (nullable_col.isNullAt(row_num))
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

        this->setFlag(place);
        this->nested_function->add(this->nestedPlace(place), nested_columns, row_num, arena);
    }

    bool allocatesMemoryInArena() const override
    {
        return this->nested_function->allocatesMemoryInArena();
    }

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

}
