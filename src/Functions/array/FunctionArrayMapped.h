#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>

#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

#include <algorithm>

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

namespace detail
{

struct NullableArrayArgument
{
    ColumnPtr column;
    ColumnPtr null_map;
};

inline const ColumnUInt8 & getNullMapColumnUInt8(const ColumnPtr & null_map_column)
{
    if (const auto * null_map_const = checkAndGetColumnConst<ColumnUInt8>(null_map_column.get()))
        return assert_cast<const ColumnUInt8 &>(null_map_const->getDataColumn());
    return assert_cast<const ColumnUInt8 &>(*null_map_column);
}

inline ColumnPtr materializeNullMapToRowCount(const ColumnPtr & null_map_column, size_t num_rows)
{
    const auto & null_map = getNullMapColumnUInt8(null_map_column);

    if (null_map.size() == num_rows && !isColumnConst(*null_map_column))
        return null_map_column;

    if (null_map.size() != 1 && null_map.size() != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Null map size {} does not match row count {} and is not a single constant value",
            null_map.size(), num_rows);

    auto result = ColumnUInt8::create();
    auto & result_data = assert_cast<ColumnUInt8 &>(*result).getData();
    if (null_map.size() == num_rows)
        result_data.assign(null_map.getData().begin(), null_map.getData().end());
    else
        result_data.resize_fill(num_rows, null_map.getData()[0]);
    return result;
}

inline void mergeRowNullMap(ColumnPtr & accumulated, const ColumnPtr & new_null_map, size_t num_rows)
{
    if (!new_null_map)
        return;

    ColumnPtr materialized_new = materializeNullMapToRowCount(new_null_map, num_rows);

    if (!accumulated)
    {
        accumulated = std::move(materialized_new);
        return;
    }

    accumulated = materializeNullMapToRowCount(accumulated, num_rows);
    auto mutable_accumulated = IColumn::mutate(accumulated);
    auto & acc_data = assert_cast<ColumnUInt8 &>(*mutable_accumulated).getData();
    const auto & new_data = assert_cast<const ColumnUInt8 &>(*materialized_new).getData();

    for (size_t i = 0; i < num_rows; ++i)
        acc_data[i] |= new_data[i];

    accumulated = std::move(mutable_accumulated);
}

inline NullableArrayArgument unwrapNullableArrayArgumentColumn(
    const ColumnPtr & column, const DataTypePtr & type, VectorWithMemoryTracking<ColumnPtr> & materialized_columns)
{
    NullableArrayArgument result{.column = column, .null_map = nullptr};

    ColumnPtr unwrapped_column = column;
    auto col_no_lowcardinality = recursiveRemoveLowCardinality(unwrapped_column);
    if (col_no_lowcardinality.get() != unwrapped_column.get())
    {
        materialized_columns.emplace_back(col_no_lowcardinality);
        unwrapped_column = col_no_lowcardinality;
    }

    const ColumnConst * col_const = checkAndGetColumnConst<ColumnConst>(unwrapped_column.get());
    const IColumn * data_column = col_const ? &col_const->getDataColumn() : unwrapped_column.get();

    if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(data_column))
    {
        result.null_map = nullable_array_column->getNullMapColumnPtr();

        auto nested_array_ptr = nullable_array_column->getNestedColumnPtr();
        if (checkAndGetColumn<ColumnArray>(nested_array_ptr.get())
            || checkAndGetColumnConst<ColumnArray>(nested_array_ptr.get()))
        {
            result.column = col_const
                ? ColumnConst::create(std::move(nested_array_ptr), col_const->size())
                : nested_array_ptr;
        }
    }
    else if (type->isNullable())
    {
        if (!result.null_map)
        {
            auto null_map = ColumnUInt8::create();
            assert_cast<ColumnUInt8 &>(*null_map).getData().resize_fill(unwrapped_column->size(), 0);
            result.null_map = std::move(null_map);
        }
    }

    return result;
}

inline bool nullMapHasAnyNull(const PaddedPODArray<UInt8> & data)
{
    return std::find(data.begin(), data.end(), static_cast<UInt8>(1)) != data.end();
}

inline DataTypePtr applyNullableArrayReturnType(
    const DataTypePtr & return_type, const ColumnsWithTypeAndName & arguments, size_t first_array_argument_index)
{
    for (size_t i = first_array_argument_index; i < arguments.size(); ++i)
    {
        if (arguments[i].type->isNullable())
            return makeNullable(return_type);
    }
    return return_type;
}

inline ColumnPtr wrapNullableArrayResultIfNeeded(
    ColumnPtr result, const ColumnPtr & array_null_map, size_t input_rows_count, const DataTypePtr & result_type)
{
    if (!result_type->isNullable())
    {
        if (!array_null_map)
            return result;

        const auto & source_null_map = assert_cast<const ColumnUInt8 &>(
            *materializeNullMapToRowCount(array_null_map, input_rows_count));
        if (!nullMapHasAnyNull(source_null_map.getData()))
            return result;

        auto mutable_result = IColumn::mutate(result->convertToFullColumnIfConst());
        MutableColumnPtr null_map_col = ColumnUInt8::create();
        assert_cast<ColumnUInt8 &>(*null_map_col).getData().assign(
            source_null_map.getData().begin(), source_null_map.getData().end());
        return ColumnNullable::create(std::move(mutable_result), std::move(null_map_col));
    }

    const size_t num_rows = result->size();

    ColumnPtr null_map_col;
    if (array_null_map)
        null_map_col = materializeNullMapToRowCount(array_null_map, num_rows);
    else
    {
        auto null_map_mut = ColumnUInt8::create();
        assert_cast<ColumnUInt8 &>(*null_map_mut).getData().resize_fill(num_rows, 0);
        null_map_col = std::move(null_map_mut);
    }

    ColumnPtr nested_result = result;
    if (const auto * nullable_result = checkAndGetColumn<ColumnNullable>(result.get()))
    {
        mergeRowNullMap(null_map_col, nullable_result->getNullMapColumnPtr(), num_rows);
        nested_result = nullable_result->getNestedColumnPtr();
    }
    else
        nested_result = IColumn::mutate(result->convertToFullColumnIfConst());

    return ColumnNullable::create(nested_result, null_map_col);
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
  * It is possible for the functions to require fixed number of positional arguments:
  * arrayPartialSort(limit, arr)
  * arrayPartialSort(x -> predicate, limit, arr)
  *
  * For some functions arrayCount, arrayExists, arrayAll, an overload of the form f(array) is available,
  *  which works in the same way as f(x -> x, array).
  *
  * See the example of Impl template parameter in arrayMap.cpp
  */
template <typename Impl, typename Name, bool IsDeterministic = true>
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
    bool isDeterministic() const override { return IsDeterministic; }
    bool isDeterministicInScopeOfQuery() const override { return IsDeterministic; }
    bool isHigherOrderFunction() const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }
    /// Nullable(Array) null maps are merged and applied in executeImpl.
    bool useDefaultImplementationForNulls() const override { return false; }

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
            const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[i + 1 + num_fixed_params]).get());
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
            const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[num_fixed_params].type).get());

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

            return detail::applyNullableArrayReturnType(
                Impl::getReturnType(nested_type, nested_type), arguments, num_fixed_params);
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

        const auto * first_array_type = checkAndGetDataType<DataTypeArray>(removeNullable(arguments[1 + num_fixed_params].type).get());
        if (!first_array_type)
            throw DB::Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Unsupported type {}", arguments[1 + num_fixed_params].type->getName());

        return detail::applyNullableArrayReturnType(
            Impl::getReturnType(return_type, first_array_type->getNestedType()), arguments, 1 + num_fixed_params);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        VectorWithMemoryTracking<ColumnPtr> materialized_columns;

        if (arguments.size() == 1 + num_fixed_params)
        {
            ColumnPtr array_null_map;
            auto unwrapped_array = detail::unwrapNullableArrayArgumentColumn(
                arguments[num_fixed_params].column, arguments[num_fixed_params].type, materialized_columns);
            detail::mergeRowNullMap(array_null_map, unwrapped_array.null_map, input_rows_count);

            ColumnPtr column_array_ptr = unwrapped_array.column;
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

            ColumnPtr result;
            if constexpr (num_fixed_params)
                result = Impl::execute(
                    *column_array,
                    column_array->getDataPtr(),
                    arguments.data());
            else
                result = Impl::execute(*column_array, column_array->getDataPtr());

            return detail::wrapNullableArrayResultIfNeeded(std::move(result), array_null_map, input_rows_count, result_type);
        }
        else
        {
            const auto & column_with_type_and_name = arguments[0];

            if (!column_with_type_and_name.column)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument for function {} must be a function.", getName());

            auto column_function_materialized = column_with_type_and_name.column->convertToFullColumnIfConst();

            const auto * column_function = typeid_cast<const ColumnFunction *>(column_function_materialized.get());
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
            ColumnPtr array_null_map;
            for (size_t i = 1 + num_fixed_params; i < arguments.size(); ++i)
            {
                const auto & array_with_type_and_name = arguments[i];

                auto unwrapped_array = detail::unwrapNullableArrayArgumentColumn(
                    array_with_type_and_name.column, array_with_type_and_name.type, materialized_columns);
                detail::mergeRowNullMap(array_null_map, unwrapped_array.null_map, input_rows_count);

                auto column_array_ptr = unwrapped_array.column;
                const auto * column_array = checkAndGetColumn<ColumnArray>(column_array_ptr.get());

                const auto & array_type_ptr = array_with_type_and_name.type;
                const auto * array_type = checkAndGetDataType<DataTypeArray>(removeNullable(array_type_ptr).get());

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

            lambda_result.column = lambda_result.column->convertToFullColumnIfLowCardinality();

            if (Impl::needBoolean())
            {
                /// If result column is Nothing or Nullable(Nothing), just create const UInt8 column with 0 value.
                if (isNothing(removeNullable(lambda_result.type)))
                {
                    auto bool_result_type = std::make_shared<DataTypeUInt8>();
                    lambda_result.column = bool_result_type->createColumnConst(lambda_result.column->size(), 0);
                }
                /// If result column is Nullable(UInt8), then extract nested column and write 0 in all rows
                /// when we have NULL.
                else if (lambda_result.column->isNullable())
                {
                    auto result_column = IColumn::mutate(std::move(lambda_result.column));

                    if (isColumnConst(*result_column))
                    {
                        UInt8 value = result_column->empty() ? 0 : result_column->getBool(0);
                        auto bool_result_type = std::make_shared<DataTypeUInt8>();
                        lambda_result.column = bool_result_type->createColumnConst(result_column->size(), value);
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

            ColumnPtr result;
            if constexpr (num_fixed_params)
                result = Impl::execute(
                    *column_first_array,
                    lambda_result.column,
                    arguments.data() + 1);
            else
                result = Impl::execute(*column_first_array, lambda_result.column);

            return detail::wrapNullableArrayResultIfNeeded(std::move(result), array_null_map, input_rows_count, result_type);
        }
    }
};

}
