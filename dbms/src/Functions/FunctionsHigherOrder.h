#pragma once

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeExpression.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ExpressionActions.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnExpression.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionsMiscellaneous.h>
#include <Functions/FunctionHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Higher-order functions for arrays:
  *
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  * arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  * arrayCount(x1,...,xn -> expression, array1,...,arrayn) - for how many elements of the array the expression is true.
  * arrayExists(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for at least one array element.
  * arrayAll(x1,...,xn -> expression, array1,...,arrayn) - is the expression true for all elements of the array.
  *
  * For functions arrayCount, arrayExists, arrayAll, an overload of the form f(array) is available, which works in the same way as f(x -> x, array).
  */

struct ArrayMapImpl
{
    /// true if the expression (for an overload of f(expression, arrays)) or an array (for f(array)) should be boolean.
    static bool needBoolean() { return false; }
    /// true if the f(array) overload is unavailable.
    static bool needExpression() { return true; }
    /// true if the array must be exactly one.
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeArray>(expression_return);
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        return mapped->isColumnConst()
            ? ColumnArray::create(mapped->convertToFullColumnIfConst(), array.getOffsetsPtr())
            : ColumnArray::create(mapped, array.getOffsetsPtr());
    }
};

struct ArrayFilterImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    /// If there are several arrays, the first one is passed here.
    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
                return array.clone();
            else
                return ColumnArray::create(
                    array.getDataPtr()->cloneEmpty(),
                    ColumnArray::ColumnOffsets::create(array.size(), 0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        ColumnPtr filtered = array.getData().filter(filter, -1);

        const IColumn::Offsets & in_offsets = array.getOffsets();
        auto column_offsets = ColumnArray::ColumnOffsets::create(in_offsets.size());
        IColumn::Offsets & out_offsets = column_offsets->getData();

        size_t in_pos = 0;
        size_t out_pos = 0;
        for (size_t i = 0; i < in_offsets.size(); ++i)
        {
            for (; in_pos < in_offsets[i]; ++in_pos)
            {
                if (filter[in_pos])
                    ++out_pos;
            }
            out_offsets[i] = out_pos;
        }

        return ColumnArray::create(filtered, std::move(column_offsets));
    }
};

struct ArrayCountImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                ColumnUInt32::Container & out_counts = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_counts[i] = offsets[i] - pos;
                    pos = offsets[i];
                }

                return std::move(out_column);
            }
            else
                return DataTypeUInt32().createColumnConst(array.size(), UInt64(0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt32::create(offsets.size());
        ColumnUInt32::Container & out_counts = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            size_t count = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                    ++count;
            }
            out_counts[i] = count;
        }

        return std::move(out_column);
    }
};

struct ArrayExistsImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt8::create(offsets.size());
                ColumnUInt8::Container & out_exists = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_exists[i] = offsets[i] - pos > 0;
                    pos = offsets[i];
                }

                return std::move(out_column);
            }
            else
                return DataTypeUInt8().createColumnConst(array.size(), UInt64(0));
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt8::create(offsets.size());
        ColumnUInt8::Container & out_exists = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt8 exists = 0;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                {
                    exists = 1;
                    pos = offsets[i];
                    break;
                }
            }
            out_exists[i] = exists;
        }

        return std::move(out_column);
    }
};

struct ArrayAllImpl
{
    static bool needBoolean() { return true; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt8>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnUInt8 * column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
                return DataTypeUInt8().createColumnConst(array.size(), UInt64(1));
            else
            {
                const IColumn::Offsets & offsets = array.getOffsets();
                auto out_column = ColumnUInt8::create(offsets.size());
                ColumnUInt8::Container & out_all = out_column->getData();

                size_t pos = 0;
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_all[i] = offsets[i] == pos;
                    pos = offsets[i];
                }

                return std::move(out_column);
            }
        }

        const IColumn::Filter & filter = column_filter->getData();
        const IColumn::Offsets & offsets = array.getOffsets();
        auto out_column = ColumnUInt8::create(offsets.size());
        ColumnUInt8::Container & out_all = out_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt8 all = 1;
            for (; pos < offsets[i]; ++pos)
            {
                if (!filter[pos])
                {
                    all = 0;
                    pos = offsets[i];
                    break;
                }
            }
            out_all[i] = all;
        }

        return std::move(out_column);
    }
};

struct ArraySumImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & expression_return, const DataTypePtr & /*array_element*/)
    {
        if (checkDataType<DataTypeUInt8>(&*expression_return) ||
            checkDataType<DataTypeUInt16>(&*expression_return) ||
            checkDataType<DataTypeUInt32>(&*expression_return) ||
            checkDataType<DataTypeUInt64>(&*expression_return))
            return std::make_shared<DataTypeUInt64>();

        if (checkDataType<DataTypeInt8>(&*expression_return) ||
            checkDataType<DataTypeInt16>(&*expression_return) ||
            checkDataType<DataTypeInt32>(&*expression_return) ||
            checkDataType<DataTypeInt64>(&*expression_return))
            return std::make_shared<DataTypeInt64>();

        if (checkDataType<DataTypeFloat32>(&*expression_return) ||
            checkDataType<DataTypeFloat64>(&*expression_return))
            return std::make_shared<DataTypeFloat64>();

        throw Exception("arraySum cannot add values of type " + expression_return->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    template <typename Element, typename Result>
    static bool executeType(const ColumnPtr & mapped, const ColumnArray::Offsets & offsets, ColumnPtr & res_ptr)
    {
        const ColumnVector<Element> * column = checkAndGetColumn<ColumnVector<Element>>(&*mapped);

        if (!column)
        {
            const ColumnConst * column_const = checkAndGetColumnConst<ColumnVector<Element>>(&*mapped);

            if (!column_const)
                return false;

            const Element x = column_const->template getValue<Element>();

            auto res_column = ColumnVector<Result>::create(offsets.size());
            typename ColumnVector<Result>::Container & res = res_column->getData();

            size_t pos = 0;
            for (size_t i = 0; i < offsets.size(); ++i)
            {
                res[i] = x * (offsets[i] - pos);
                pos = offsets[i];
            }

            res_ptr = std::move(res_column);
            return true;
        }

        const typename ColumnVector<Element>::Container & data = column->getData();
        auto res_column = ColumnVector<Result>::create(offsets.size());
        typename ColumnVector<Result>::Container & res = res_column->getData();

        size_t pos = 0;
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            Result s = 0;
            for (; pos < offsets[i]; ++pos)
            {
                s += data[pos];
            }
            res[i] = s;
        }

        res_ptr = std::move(res_column);
        return true;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const IColumn::Offsets & offsets = array.getOffsets();
        ColumnPtr res;

        if (executeType< UInt8 , UInt64>(mapped, offsets, res) ||
            executeType< UInt16, UInt64>(mapped, offsets, res) ||
            executeType< UInt32, UInt64>(mapped, offsets, res) ||
            executeType< UInt64, UInt64>(mapped, offsets, res) ||
            executeType<  Int8 ,  Int64>(mapped, offsets, res) ||
            executeType<  Int16,  Int64>(mapped, offsets, res) ||
            executeType<  Int32,  Int64>(mapped, offsets, res) ||
            executeType<  Int64,  Int64>(mapped, offsets, res) ||
            executeType<Float32,Float64>(mapped, offsets, res) ||
            executeType<Float64,Float64>(mapped, offsets, res))
            return res;
        else
            throw Exception("Unexpected column for arraySum: " + mapped->getName());
    }
};

struct ArrayFirstImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return array_element;
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        auto column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                const auto & data = array.getData();
                auto out = data.cloneEmpty();

                size_t pos{};
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    if (offsets[i] - pos > 0)
                        out->insert(data[pos]);
                    else
                        out->insertDefault();

                    pos = offsets[i];
                }

                return std::move(out);
            }
            else
            {
                auto out = array.getData().cloneEmpty();
                out->insertDefault();
                return out->replicate(IColumn::Offsets(1, array.size()));
            }
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        const auto & data = array.getData();
        auto out = data.cloneEmpty();

        size_t pos{};
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            auto exists = false;
            for (; pos < offsets[i]; ++pos)
            {
                if (filter[pos])
                {
                    out->insert(data[pos]);
                    exists = true;
                    pos = offsets[i];
                    break;
                }
            }

            if (!exists)
                out->insertDefault();
        }

        return std::move(out);
    }
};

struct ArrayFirstIndexImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return true; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & /*array_element*/)
    {
        return std::make_shared<DataTypeUInt32>();
    }

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        auto column_filter = typeid_cast<const ColumnUInt8 *>(&*mapped);

        if (!column_filter)
        {
            auto column_filter_const = checkAndGetColumnConst<ColumnUInt8>(&*mapped);

            if (!column_filter_const)
                throw Exception("Unexpected type of filter column", ErrorCodes::ILLEGAL_COLUMN);

            if (column_filter_const->getValue<UInt8>())
            {
                const auto & offsets = array.getOffsets();
                auto out_column = ColumnUInt32::create(offsets.size());
                auto & out_index = out_column->getData();

                size_t pos{};
                for (size_t i = 0; i < offsets.size(); ++i)
                {
                    out_index[i] = offsets[i] - pos > 0;
                    pos = offsets[i];
                }

                return std::move(out_column);
            }
            else
                return DataTypeUInt32().createColumnConst(array.size(), UInt64(0));
        }

        const auto & filter = column_filter->getData();
        const auto & offsets = array.getOffsets();
        auto out_column = ColumnUInt32::create(offsets.size());
        auto & out_index = out_column->getData();

        size_t pos{};
        for (size_t i = 0; i < offsets.size(); ++i)
        {
            UInt32 index{};
            for (size_t idx{1}; pos < offsets[i]; ++pos, ++idx)
            {
                if (filter[pos])
                {
                    index = idx;
                    pos = offsets[i];
                    break;
                }
            }

            out_index[i] = index;
        }

        return std::move(out_column);
    }
};


/** Sort arrays, by values of its elements, or by values of corresponding elements of calculated expression (known as "schwartzsort").
  */
template <bool positive>
struct ArraySortImpl
{
    static bool needBoolean() { return false; }
    static bool needExpression() { return false; }
    static bool needOneArray() { return false; }

    static DataTypePtr getReturnType(const DataTypePtr & /*expression_return*/, const DataTypePtr & array_element)
    {
        return std::make_shared<DataTypeArray>(array_element);
    }

    struct Less
    {
        const IColumn & column;

        Less(const IColumn & column) : column(column) {}

        bool operator()(size_t lhs, size_t rhs) const
        {
            if (positive)
                return column.compareAt(lhs, rhs, column, 1) < 0;
            else
                return column.compareAt(lhs, rhs, column, -1) > 0;
        }
    };

    static ColumnPtr execute(const ColumnArray & array, ColumnPtr mapped)
    {
        const ColumnArray::Offsets & offsets = array.getOffsets();

        size_t size = offsets.size();
        size_t nested_size = array.getData().size();
        IColumn::Permutation permutation(nested_size);

        for (size_t i = 0; i < nested_size; ++i)
            permutation[i] = i;

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            auto next_offset = offsets[i];
            std::sort(&permutation[current_offset], &permutation[next_offset], Less(*mapped));
            current_offset = next_offset;
        }

        return ColumnArray::create(array.getData().permute(permutation, 0), array.getOffsetsPtr());
    }
};


template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayMapped>(); };

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// Called if at least one function argument is a lambda expression.
    /// For argument-lambda expressions, it defines the types of arguments of these expressions.
    void getLambdaArgumentTypesImpl(DataTypes & arguments) const override
    {
        if (arguments.size() < 1)
            throw Exception("Function " + getName() + " needs at least one argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
            throw Exception("Function " + getName() + " needs at least one array argument.",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        DataTypes nested_types(arguments.size() - 1);
        for (size_t i = 0; i < nested_types.size(); ++i)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception("Argument " + toString(i + 2) + " of function " + getName() + " must be array. Found "
                                + arguments[i + 1]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            nested_types[i] = array_type->getNestedType();
        }

        const DataTypeExpression * expression_type = checkAndGetDataType<DataTypeExpression>(&*arguments[0]);
        if (!expression_type || expression_type->getArgumentTypes().size() != nested_types.size())
            throw Exception("First argument for this overload of " + getName() + " must be an expression with "
                            + toString(nested_types.size()) + " arguments. Found "
                            + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        arguments[0] = std::make_shared<DataTypeExpression>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        size_t min_args = Impl::needExpression() ? 2 : 1;
        if (arguments.size() < min_args)
            throw Exception("Function " + getName() + " needs at least "
                + toString(min_args) + " argument; passed "
                + toString(arguments.size()) + ".",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());

            if (!array_type)
                throw Exception("The only argument for function " + getName() + " must be array. Found "
                    + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            DataTypePtr nested_type = array_type->getNestedType();

            if (Impl::needBoolean() && !checkDataType<DataTypeUInt8>(&*nested_type))
                throw Exception("The only argument for function " + getName() + " must be array of UInt8. Found "
                    + arguments[0]->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            return Impl::getReturnType(nested_type, nested_type);
        }
        else
        {
            if (arguments.size() > 2 && Impl::needOneArray())
                throw Exception("Function " + getName() + " needs one array argument.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            const DataTypeExpression * expression = checkAndGetDataType<DataTypeExpression>(arguments[0].get());

            if (!expression)
                throw Exception("Type of first argument for function " + getName() + " must be an expression.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.

            DataTypePtr return_type = expression->getReturnType();
            if (Impl::needBoolean() && !checkDataType<DataTypeUInt8>(&*return_type))
                throw Exception("Expression for function " + getName() + " must return UInt8, found "
                    + return_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypeArray * first_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get());

            return Impl::getReturnType(return_type, first_array_type->getNestedType());
        }
    }

    void getReturnTypeAndPrerequisitesImpl(const ColumnsWithTypeAndName & arguments,
                                        DataTypePtr & out_return_type,
                                        ExpressionActions::Actions & out_prerequisites) override
    {
        size_t min_args = Impl::needExpression() ? 2 : 1;
        if (arguments.size() < min_args)
            throw Exception("Function " + getName() + " needs at least "
                            + toString(min_args) + " argument; passed "
                            + toString(arguments.size()) + ".",
                            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments.size() == 1)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[0].type);

            if (!array_type)
                throw Exception("The only argument for function " + getName() + " must be array. Found "
                                + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            DataTypePtr nested_type = array_type->getNestedType();

            if (Impl::needBoolean() && !checkDataType<DataTypeUInt8>(&*nested_type))
                throw Exception("The only argument for function " + getName() + " must be array of UInt8. Found "
                                + arguments[0].type->getName() + " instead.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            out_return_type = Impl::getReturnType(nested_type, nested_type);
        }
        else
        {
            if (arguments.size() > 2 && Impl::needOneArray())
                throw Exception("Function " + getName() + " needs one array argument.",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            if (!arguments[0].column)
                throw Exception("Type of first argument for function " + getName() + " must be an expression.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const ColumnExpression * column_expression = typeid_cast<const ColumnExpression *>(arguments[0].column.get());

            if (!column_expression)
                throw Exception("Column of first argument for function " + getName() + " must be an expression.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.

            /// Let's add to the block all the columns mentioned in the expression, multiplied into an array parallel to the one being processed.
            const ExpressionActions & expression = *column_expression->getExpression();
            const NamesAndTypesList & required_columns = expression.getRequiredColumnsWithTypes();

            const NamesAndTypesList expression_arguments = column_expression->getArguments();
            NameSet argument_names;
            for (const auto & expression_argument : expression_arguments)
                argument_names.emplace(expression_argument.name);

            for (const auto & required_column : required_columns)
            {
                if (argument_names.count(required_column.name))
                    continue;
                Names replicate_arguments;
                replicate_arguments.push_back(required_column.name);
                replicate_arguments.push_back(arguments[1].name);
                out_prerequisites.push_back(ExpressionAction::applyFunction(std::make_shared<FunctionReplicate>(), replicate_arguments));
            }

            DataTypePtr return_type = column_expression->getReturnType();
            if (Impl::needBoolean() && !checkDataType<DataTypeUInt8>(&*return_type))
                throw Exception("Expression for function " + getName() + " must return UInt8, found "
                                + return_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const DataTypeArray * first_array_type = checkAndGetDataType<DataTypeArray>(&*arguments[1].type);

            out_return_type = Impl::getReturnType(return_type, first_array_type->getNestedType());
        }
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        executeImpl(block, arguments, {}, result);
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result) override
    {
        if (arguments.size() == 1)
        {
            ColumnPtr column_array_ptr = block.getByPosition(arguments[0]).column;
            const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                column_array_ptr = column_const_array->convertToFullColumn();
                column_array = static_cast<const ColumnArray *>(column_array_ptr.get());
            }

            block.getByPosition(result).column = Impl::execute(*column_array, column_array->getDataPtr());
        }
        else
        {
            const auto & column_with_type_and_name = block.getByPosition(arguments[0]);

            if (!column_with_type_and_name.column)
                throw Exception("First argument for function " + getName() + " must be an expression.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const ColumnExpression * column_expression = typeid_cast<const ColumnExpression *>(column_with_type_and_name.column.get());

            ColumnPtr offsets_column;

            Block temp_block;
            const ExpressionActions & expression = *column_expression->getExpression();
            const NamesAndTypesList & expression_arguments = column_expression->getArguments();
            NameSet argument_names;

            ColumnPtr column_first_array_ptr;
            const ColumnArray * column_first_array = nullptr;

            /// Put the expression arguments in the block.

            size_t i = 0;
            for (const auto expression_argument : expression_arguments)
            {
                ColumnPtr column_array_ptr = block.getByPosition(arguments[i + 1]).column;
                const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

                if (!column_array)
                {
                    const ColumnConst * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                    if (!column_const_array)
                        throw Exception("Expected array column, found " + column_array_ptr->getName(), ErrorCodes::ILLEGAL_COLUMN);
                    column_array_ptr = column_const_array->convertToFullColumn();
                    column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());
                }

                if (!offsets_column)
                {
                    offsets_column = column_array->getOffsetsPtr();
                }
                else
                {
                    /// The first condition is optimization: do not compare data if the pointers are equal.
                    if (column_array->getOffsetsPtr() != offsets_column
                        && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                        throw Exception("Arrays passed to " + getName() + " must have equal size", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
                }

                if (i == 0)
                {
                    column_first_array_ptr = column_array_ptr;
                    column_first_array = column_array;
                }

                temp_block.insert({
                    column_array->getDataPtr(),
                    expression_argument.type,
                    expression_argument.name});

                argument_names.insert(expression_argument.name);
                ++i;
            }

            /// Put all the necessary columns multiplied by the sizes of arrays into the block.

            Names required_columns = expression.getRequiredColumns();
            size_t prerequisite_index = 0;

            for (size_t i = 0; i < required_columns.size(); ++i)
            {
                const String & name = required_columns[i];

                if (argument_names.count(name))
                    continue;

                ColumnWithTypeAndName replicated_column = block.getByPosition(prerequisites[prerequisite_index]);

                replicated_column.name = name;
                replicated_column.column = typeid_cast<const ColumnArray &>(*replicated_column.column).getDataPtr();
                replicated_column.type = typeid_cast<const DataTypeArray &>(*replicated_column.type).getNestedType(),
                temp_block.insert(std::move(replicated_column));

                ++prerequisite_index;
            }

            expression.execute(temp_block);

            block.getByPosition(result).column = Impl::execute(*column_first_array, temp_block.getByName(column_expression->getReturnName()).column);
        }
    }
};


struct NameArrayMap         { static constexpr auto name = "arrayMap"; };
struct NameArrayFilter      { static constexpr auto name = "arrayFilter"; };
struct NameArrayCount       { static constexpr auto name = "arrayCount"; };
struct NameArrayExists      { static constexpr auto name = "arrayExists"; };
struct NameArrayAll         { static constexpr auto name = "arrayAll"; };
struct NameArraySum         { static constexpr auto name = "arraySum"; };
struct NameArrayFirst       { static constexpr auto name = "arrayFirst"; };
struct NameArrayFirstIndex  { static constexpr auto name = "arrayFirstIndex"; };
struct NameArraySort        { static constexpr auto name = "arraySort"; };
struct NameArrayReverseSort { static constexpr auto name = "arrayReverseSort"; };

using FunctionArrayMap = FunctionArrayMapped<ArrayMapImpl, NameArrayMap>;
using FunctionArrayFilter = FunctionArrayMapped<ArrayFilterImpl, NameArrayFilter>;
using FunctionArrayCount = FunctionArrayMapped<ArrayCountImpl, NameArrayCount>;
using FunctionArrayExists = FunctionArrayMapped<ArrayExistsImpl, NameArrayExists>;
using FunctionArrayAll = FunctionArrayMapped<ArrayAllImpl, NameArrayAll>;
using FunctionArraySum = FunctionArrayMapped<ArraySumImpl, NameArraySum>;
using FunctionArrayFirst = FunctionArrayMapped<ArrayFirstImpl, NameArrayFirst>;
using FunctionArrayFirstIndex = FunctionArrayMapped<ArrayFirstIndexImpl, NameArrayFirstIndex>;
using FunctionArraySort = FunctionArrayMapped<ArraySortImpl<true>, NameArraySort>;
using FunctionArrayReverseSort = FunctionArrayMapped<ArraySortImpl<false>, NameArrayReverseSort>;

}
