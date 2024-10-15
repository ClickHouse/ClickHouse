#pragma once

#include <type_traits>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/IColumn.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

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
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Higher-order functions for arrays.
  * These functions optionally apply a map (transform) to array (or multiple arrays of identical size) by lambda function,
  *  and return some result based on that transformation.
  *
  * Examples:
  * arrayMap(x1,...,xn -> expression, array1,...,arrayn) - apply the expression to each element of the array (or set of parallel arrays).
  * arrayFilter(x -> predicate, array) - leave in the array only the elements for which the expression is true.
  *
  * It is possible for the functions to require fixed number of positional arguments:
  * arrayPartialSort(limit, arr)
  * arrayPartialSort(x -> predicate, limit, arr)
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
    static constexpr size_t num_fixed_params = []{ if constexpr (requires { Impl::num_fixed_params; }) return Impl::num_fixed_params; else return 0; }();

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayMapped>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Called if at least one function argument is a lambda expression.
    /// For argument-lambda expressions, it defines the types of arguments of these expressions.
    void getLambdaArgumentTypes(DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs at least one argument, passed {}",
                getName(),
                arguments.size());

        if (arguments.size() <= 1 + num_fixed_params)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs at least {} argument{} with data",
                getName(),
                num_fixed_params + 1,
                (num_fixed_params + 1 == 1) ? "" : "s");

        if (arguments.size() > 2 + num_fixed_params && Impl::needOneArray())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs {} argument{} with data",
                getName(),
                num_fixed_params + 1,
                (num_fixed_params + 1 == 1) ? "" : "s");

        bool is_single_array_argument = arguments.size() == num_fixed_params + 2;
        size_t tuple_argument_size = 0;

        size_t num_nested_types = arguments.size() - num_fixed_params - 1;
        DataTypes nested_types(num_nested_types);

        for (size_t i = 0; i < num_nested_types; ++i)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(&*arguments[i + 1 + num_fixed_params]);
            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Argument {} of function {} must be Array. Found {} instead",
                    i + 2 + num_fixed_params,
                    getName(),
                    arguments[i + 1 + num_fixed_params]->getName());

            if (const auto * tuple_type = checkAndGetDataType<DataTypeTuple>(array_type->getNestedType().get()))
                tuple_argument_size = tuple_type->getElements().size();

            nested_types[i] = recursiveRemoveLowCardinality(array_type->getNestedType());
        }

        const auto * function_type = checkAndGetDataType<DataTypeFunction>(arguments[0].get());
        if (!function_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for this overload of {} must be a function with {} arguments, found {} instead",
                getName(),
                nested_types.size(),
                arguments[0]->getName());

        size_t num_function_arguments = function_type->getArgumentTypes().size();
        if (is_single_array_argument
            && tuple_argument_size > 1
            && tuple_argument_size == num_function_arguments)
        {
            assert(nested_types.size() == 1);

            auto argument_type = nested_types[0];
            const auto & tuple_type = assert_cast<const DataTypeTuple &>(*argument_type);

            nested_types.clear();
            nested_types.reserve(tuple_argument_size);

            for (const auto & element : tuple_type.getElements())
                nested_types.push_back(element);
        }

        if (num_function_arguments != nested_types.size())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for this overload of {} must be a function with {} arguments, found {} instead",
                getName(),
                nested_types.size(),
                arguments[0]->getName());

        arguments[0] = std::make_shared<DataTypeFunction>(nested_types);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t min_args = (Impl::needExpression() ? 2 : 1) + num_fixed_params ;
        if (arguments.size() < min_args)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} needs at least {} argument{}, passed {}",
                getName(),
                min_args,
                (min_args > 1 ? "s" : ""),
                arguments.size());

        if (arguments.size() == 1 + num_fixed_params)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[num_fixed_params].type.get());

            if (!array_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {}{}{} argument for function {} must be array. Found {} instead",
                    num_fixed_params + 1,
                    getOrdinalSuffix(num_fixed_params + 1),
                    (num_fixed_params == 0 ? " and only" : ""),
                    getName(),
                    arguments[num_fixed_params].type->getName());

            if constexpr (num_fixed_params)
                Impl::checkArguments(getName(), arguments.data());

            DataTypePtr nested_type = array_type->getNestedType();

            if (Impl::needBoolean() && !isUInt8(nested_type))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The {}{}{} argument for function {} must be array of UInt8. Found {} instead",
                    num_fixed_params + 1,
                    getOrdinalSuffix(num_fixed_params + 1),
                    (num_fixed_params == 0 ? " and only" : ""),
                    getName(),
                    arguments[num_fixed_params].type->getName());

            return Impl::getReturnType(nested_type, nested_type);
        }

        if (arguments.size() > 2 + num_fixed_params && Impl::needOneArray())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} needs one argument with data", getName());

        const auto * data_type_function = checkAndGetDataType<DataTypeFunction>(arguments[0].type.get());

        if (!data_type_function)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be a function. Actual {}",
                getName(),
                arguments[0].type->getName());

        if constexpr (num_fixed_params)
            Impl::checkArguments(getName(), arguments.data() + 1);

        /// The types of the remaining arguments are already checked in getLambdaArgumentTypes.

        DataTypePtr return_type = removeLowCardinality(data_type_function->getReturnType());

        /// Special cases when we need boolean lambda result:
        ///  - lambda may return Nullable(UInt8) column, in this case after lambda execution we will
        ///    replace all NULLs with 0 and return nested UInt8 column.
        ///  - lambda may return Nothing or Nullable(Nothing) because of default implementation of functions
        ///    for these types. In this case we will just create UInt8 const column full of 0.
        if (Impl::needBoolean() && !isUInt8(removeNullable(return_type)) && !isNothing(removeNullable(return_type)))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Expression for function {} must return UInt8 or Nullable(UInt8), found {}",
                getName(),
                return_type->getName());

        if (arguments.size() < 2 + num_fixed_params)
            throw DB::Exception(ErrorCodes::LOGICAL_ERROR, "Incorrect number of arguments: {}", arguments.size());

        const auto * first_array_type = checkAndGetDataType<DataTypeArray>(arguments[1 + num_fixed_params].type.get());
        if (!first_array_type)
            throw DB::Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported type {}", arguments[1 + num_fixed_params].type->getName());

        return Impl::getReturnType(return_type, first_array_type->getNestedType());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        if (arguments.size() == 1 + num_fixed_params)
        {
            ColumnPtr column_array_ptr = arguments[num_fixed_params].column;
            const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

            if (!column_array)
            {
                const auto * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                if (!column_const_array)
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN, "Expected Array column, found {}", column_array_ptr->getName());

                column_array_ptr = column_const_array->convertToFullColumn();
                column_array = assert_cast<const ColumnArray *>(column_array_ptr.get());
            }

            if constexpr (num_fixed_params)
                return Impl::execute(
                    *column_array,
                    column_array->getDataPtr(),
                    arguments.data());
            else
                return Impl::execute(*column_array, column_array->getDataPtr());
        }
        else
        {
            const auto & column_with_type_and_name = arguments[0];

            if (!column_with_type_and_name.column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

            const auto * column_function = typeid_cast<const ColumnFunction *>(column_with_type_and_name.column.get());
            if (!column_function)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

            const auto & type_function = assert_cast<const DataTypeFunction &>(*arguments[0].type);
            size_t num_function_arguments = type_function.getArgumentTypes().size();

            ColumnPtr offsets_column;
            ColumnPtr column_first_array_ptr;
            const ColumnArray * column_first_array = nullptr;

            ColumnsWithTypeAndName arrays;
            arrays.reserve(arguments.size() - 1 - num_fixed_params);

            bool is_single_array_argument = arguments.size() == num_fixed_params + 2;
            for (size_t i = 1 + num_fixed_params; i < arguments.size(); ++i)
            {
                const auto & array_with_type_and_name = arguments[i];

                auto column_array_ptr = array_with_type_and_name.column;
                const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

                const auto & array_type_ptr = array_with_type_and_name.type;
                const auto * array_type = checkAndGetDataType<DataTypeArray>(array_type_ptr.get());

                if (!column_array)
                {
                    const auto * column_const_array = checkAndGetColumnConst<ColumnArray>(column_array_ptr.get());
                    if (!column_const_array)
                        throw Exception(
                            ErrorCodes::ILLEGAL_COLUMN, "Expected Array column, found {}", column_array_ptr->getName());

                    column_array_ptr = recursiveRemoveLowCardinality(column_const_array->convertToFullColumn());
                    column_array = &checkAndGetColumn<ColumnArray>(*column_array_ptr);
                }

                if (!array_type)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Expected Array type, found {}", array_type_ptr->getName());

                if (!offsets_column)
                {
                    offsets_column = column_array->getOffsetsPtr();
                }
                else
                {
                    /// The first condition is optimization: do not compare data if the pointers are equal.
                    if (column_array->getOffsetsPtr() != offsets_column
                        && column_array->getOffsets() != typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData())
                        throw Exception(
                            ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH,
                            "Arrays passed to {} must have equal size. Argument {} has size {} which differs with the size of another argument, {}",
                            getName(),
                            i + 1,
                            column_array->getOffsets().back(),  /// By the way, PODArray supports addressing -1th element.
                            typeid_cast<const ColumnArray::ColumnOffsets &>(*offsets_column).getData().back());
                }

                const auto * column_tuple = checkAndGetColumn<ColumnTuple>(&column_array->getData());
                size_t tuple_size = column_tuple ? column_tuple->getColumns().size() : 0;

                if (is_single_array_argument && tuple_size > 1 && tuple_size == num_function_arguments)
                {
                    const auto & type_tuple = assert_cast<const DataTypeTuple &>(*array_type->getNestedType());
                    const auto & tuple_names = type_tuple.getElementNames();

                    arrays.reserve(column_tuple->getColumns().size());
                    for (size_t j = 0; j < tuple_size; ++j)
                    {
                        arrays.emplace_back(
                            column_tuple->getColumnPtr(j),
                            type_tuple.getElement(j),
                            array_with_type_and_name.name + "." + tuple_names[j]);
                    }
                }
                else
                {
                    arrays.emplace_back(
                        column_array->getDataPtr(),
                        array_type->getNestedType(),
                        array_with_type_and_name.name);
                }

                if (i == 1 + num_fixed_params)
                {
                    column_first_array_ptr = column_array_ptr;
                    column_first_array = column_array;
                }
            }

            /// Put all the necessary columns multiplied by the sizes of arrays into the columns.
            auto replicated_column_function_ptr = IColumn::mutate(column_function->replicate(column_first_array->getOffsets()));
            auto & replicated_column_function = typeid_cast<ColumnFunction &>(*replicated_column_function_ptr);
            replicated_column_function.appendArguments(arrays);

            auto lambda_result = replicated_column_function.reduce();

            /// Convert LowCardinality(T) -> T and Const(LowCardinality(T)) -> Const(T),
            /// because we removed LowCardinality from return type of lambda expression.
            if (lambda_result.column->lowCardinality())
                lambda_result.column = lambda_result.column->convertToFullColumnIfLowCardinality();

            if (const auto * const_column = checkAndGetColumnConst<ColumnLowCardinality>(lambda_result.column.get()))
                lambda_result.column = const_column->removeLowCardinality();

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

            if constexpr (num_fixed_params)
                return Impl::execute(
                    *column_first_array,
                    lambda_result.column,
                    arguments.data() + 1);
            else
                return Impl::execute(*column_first_array, lambda_result.column);
        }
    }
};

}
