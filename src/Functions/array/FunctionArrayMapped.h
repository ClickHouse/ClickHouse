#pragma once

#include <type_traits>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/IColumn.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/castColumn.h>

#include <IO/WriteHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


template <typename T>
ColumnPtr getOffsetsPtr(const T & column)
{
    if constexpr (std::is_same_v<T, ColumnArray>)
    {
        return column.getOffsetsPtr();
    }
    else // ColumnMap
    {
        return column.getNestedColumn().getOffsetsPtr();
    }
}

template <typename T>
const IColumn::Offsets & getOffsets(const T & column)
{
    if constexpr (std::is_same_v<T, ColumnArray>)
    {
        return column.getOffsets();
    }
    else // ColumnMap
    {
        return column.getNestedColumn().getOffsets();
    }
}

/** Higher-order functions for arrays.
  * These functions optionally apply a map (transform) to array (or multiple arrays of identical size) by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  * arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  *
  * For some functions arrayCount, arrayExists, arrayAll, an overload of the form f(array) is available,
  *  which works in the same way as f(x -> x, array).
  *
  * See the example of Impl template parameter in arrayMap.cpp
  */
template <typename Impl, typename Name>
class FunctionArrayMapped : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static constexpr bool is_argument_type_map = std::is_same_v<typename Impl::data_type, DataTypeMap>;
    static constexpr bool is_argument_type_array = std::is_same_v<typename Impl::data_type, DataTypeArray>;
    static constexpr auto argument_type_name = is_argument_type_map ? "Map" : "Array";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayMapped>(); }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    /// Called if at least one function argument is a lambda expression.
    /// For argument-lambda expressions, it defines the types of arguments of these expressions.
    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} needs at least one argument, passed {}", getName(), arguments.size());

        if (arguments.size() == 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} needs at least one argument with data", getName());

        if (arguments.size() > 2 && Impl::needOneArray())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} needs one argument with data", getName());

        size_t nested_types_count = is_argument_type_map ? (arguments.size() - 1) * 2 : (arguments.size() - 1);
        DataTypes nested_types(nested_types_count);
        for (size_t i = 0; i < arguments.size() - 1; ++i)
        {
            const auto * array_type = checkAndGetDataType<typename Impl::data_type>(&*arguments[i + 1]);
            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be {}. Found {} instead",
                    toString(i + 2),
                    getName(),
                    argument_type_name,
                    arguments[i + 1]->getName());
            if constexpr (is_argument_type_map)
            {
                nested_types[2 * i] = recursiveRemoveLowCardinality(array_type->getKeyType());
                nested_types[2 * i + 1] = recursiveRemoveLowCardinality(array_type->getValueType());
            }
            else if constexpr (is_argument_type_array)
            {
                nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
            }
        }

        const DataTypeFunction * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type || function_type->getArgumentTypes().size() != nested_types.size())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for this overload of {} must be a function with {} arguments, found {} instead",
                getName(), nested_types.size(), arguments[0]->getName());

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t min_args = Impl::needExpression() ? 2 : 1;
        if (arguments.size() < min_args)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                            "Function {} needs at least {} argument, passed {}",
                            getName(), min_args, arguments.size());

        if ((arguments.size() == 1) && is_argument_type_array)
        {
            const auto * data_type = checkAndGetDataType<typename Impl::data_type>(arguments[0].type.get());

            if (!data_type)
                throw Exception("The only argument for function " + getName() + " must be array. Found "
                                + arguments[0].type->getName() + " instead", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            DataTypePtr nested_type = data_type->getNestedType();

            if (Impl::needBoolean() && !isUInt8(nested_type))
                throw Exception("The only argument for function " + getName() + " must be array of UInt8. Found "
                                + arguments[0].type->getName() + " instead", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            if constexpr (is_argument_type_array)
                return Impl::getReturnType(nested_type, nested_type);
            else
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code reached");
        }
        else
        {
            if (arguments.size() > 2 && Impl::needOneArray())
                throw Exception("Function " + getName() + " needs one argument with data",
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

            const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());

            if (!data_type_function)
                throw Exception("First argument for function " + getName() + " must be a function",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.

            DataTypePtr return_type = removeLowCardinality(data_type_function->getReturnType());

            /// Special cases when we need boolean lambda result:
            ///  - lambda may return Nullable(UInt8) column, in this case after lambda execution we will
            ///    replace all NULLs with 0 and return nested UInt8 column.
            ///  - lambda may return Nothing or Nullable(Nothing) because of default implementation of functions
            ///    for these types. In this case we will just create UInt8 const column full of 0.
            if (Impl::needBoolean() && !isUInt8(removeNullable(return_type)) && !isNothing(removeNullable(return_type)))
                throw Exception("Expression for function " + getName() + " must return UInt8 or Nullable(UInt8), found "
                                + return_type->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            static_assert(is_argument_type_map || is_argument_type_array, "unsupported type");

            if (arguments.size() < 2)
            {
                throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "{}", arguments.size());
            }

            const auto * first_array_type = checkAndGetDataType<typename Impl::data_type>(arguments[1].type.get());

            if (!first_array_type)
                throw DB::Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported type {}", arguments[1].type->getName());

            if constexpr (is_argument_type_array)
                return Impl::getReturnType(return_type, first_array_type->getNestedType());

            if constexpr (is_argument_type_map)
                return Impl::getReturnType(return_type, first_array_type->getKeyValueTypes());

            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Unreachable code reached");
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (arguments.size() == 1)
        {
            ColumnPtr column_array_ptr = arguments[0].column;
            const auto * column_array = checkAndGetColumn<typename Impl::column_type>(column_array_ptr.get());

            if (!column_array)
            {
                const ColumnConst * column_const_array = checkAndGetColumnConst<typename Impl::column_type>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "Expected {} column, found {}",
                        argument_type_name,
                        column_array_ptr->getName());
                column_array_ptr = column_const_array->convertToFullColumn();
                column_array = assert_cast<const typename Impl::column_type *>(column_array_ptr.get());
            }

            if constexpr (std::is_same_v<typename Impl::column_type, ColumnMap>)
            {
                return Impl::execute(*column_array, column_array->getNestedColumn().getDataPtr());
            }
            else
            {
                return Impl::execute(*column_array, column_array->getDataPtr());
            }
        }
        else
        {
            const auto & column_with_type_and_name = arguments[0];

            if (!column_with_type_and_name.column)
                throw Exception("First argument for function " + getName() + " must be a function.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            const auto * column_function = typeid_cast<const ColumnFunction *>(column_with_type_and_name.column.get());

            if (!column_function)
                throw Exception("First argument for function " + getName() + " must be a function.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            ColumnPtr offsets_column;

            ColumnPtr column_first_array_ptr;
            const typename Impl::column_type * column_first_array = nullptr;

            ColumnsWithTypeAndName arrays;
            arrays.reserve(arguments.size() - 1);

            for (size_t i = 1; i < arguments.size(); ++i)
            {
                const auto & array_with_type_and_name = arguments[i];

                ColumnPtr column_array_ptr = array_with_type_and_name.column;
                const auto * column_array = checkAndGetColumn<typename Impl::column_type>(column_array_ptr.get());

                const DataTypePtr & array_type_ptr = array_with_type_and_name.type;
                const auto * array_type = checkAndGetDataType<typename Impl::data_type>(array_type_ptr.get());

                if (!column_array)
                {
                    const ColumnConst * column_const_array = checkAndGetColumnConst<typename Impl::column_type>(column_array_ptr.get());
                    if (!column_const_array)
                        throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN, "Expected {} column, found {}", argument_type_name, column_array_ptr->getName());
                    column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                    column_array = checkAndGetColumn<typename Impl::column_type>(column_array_ptr.get());
                }

                if (!array_type)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected {} type, found {}", argument_type_name, array_type_ptr->getName());

                if (!offsets_column)
                {
                    offsets_column = getOffsetsPtr(*column_array);
                }
                else
                {
                    /// The first condition is optimization: do not compare data if the pointers are equal.
                    if (getOffsetsPtr(*column_array) != offsets_column
                        && getOffsets(*column_array) != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                        throw Exception(
                            ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH,
                            "{}s passed to {} must have equal size",
                            argument_type_name,
                            getName());
                }

                if (i == 1)
                {
                    column_first_array_ptr = column_array_ptr;
                    column_first_array = column_array;
                }

                if constexpr (is_argument_type_map)
                {
                    arrays.emplace_back(ColumnWithTypeAndName(
                        column_array->getNestedData().getColumnPtr(0), recursiveRemoveLowCardinality(array_type->getKeyType()), array_with_type_and_name.name+".key"));
                    arrays.emplace_back(ColumnWithTypeAndName(
                        column_array->getNestedData().getColumnPtr(1), recursiveRemoveLowCardinality(array_type->getValueType()), array_with_type_and_name.name+".value"));
                }
                else
                {
                    arrays.emplace_back(ColumnWithTypeAndName(column_array->getDataPtr(),
                                                            recursiveRemoveLowCardinality(array_type->getNestedType()),
                                                            array_with_type_and_name.name));
                }
            }

            /// Put all the necessary columns multiplied by the sizes of arrays into the columns.
            auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(getOffsets(*column_first_array)));
            auto * replicated_column_function = typeid_cast<ColumnFunction *>(replicated_column_function_ptr.get());
            replicated_column_function->appendArguments(arrays);

            auto lambda_result = replicated_column_function->reduce();
            if (lambda_result.column->lowCardinality())
                lambda_result.column = lambda_result.column->convertToFullColumnIfLowCardinality();

            if (Impl::needBoolean())
            {
                /// If result column is Nothing or Nullable(Nothing), just create const UInt8 column with 0 value.
                if (isNothing(removeNullable(lambda_result.type)))
                {
                    auto result_type = std::make_shared<DataTypeUInt8>();
                    lambda_result.column = result_type->createColumnConst(lambda_result.column->size(), 0);
                }
                /// If result column is Nullable(UInt8), then extract nested column and write 0 in all rows
                /// when we have NULL.
                else if (lambda_result.column->isNullable())
                {
                    auto result_column = IColumn::mutate(std::move(lambda_result.column));

                    if (isColumnConst(*result_column))
                    {
                        UInt8 value = result_column->empty() ? 0 : result_column->getBool(0);
                        auto result_type = std::make_shared<DataTypeUInt8>();
                        lambda_result.column = result_type->createColumnConst(result_column->size(), value);
                    }
                    else
                    {
                        auto * column_nullable = assert_cast<ColumnNullable *>(result_column.get());
                        auto & null_map = column_nullable->getNullMapData();
                        auto nested_column = IColumn::mutate(std::move(column_nullable->getNestedColumnPtr()));
                        auto & nested_data = assert_cast<ColumnUInt8 *>(nested_column.get())->getData();
                        for (size_t i = 0; i != nested_data.size(); ++i)
                        {
                            if (null_map[i])
                                nested_data[i] = 0;
                        }
                        lambda_result.column = std::move(nested_column);
                    }
                }
            }

            return Impl::execute(*column_first_array, lambda_result.column);
        }
    }
};

}
