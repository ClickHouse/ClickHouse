#pragma once
#include <Functions/IFunctionImpl.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include "Columns/ColumnLowCardinality.h"
#include "DataTypes/DataTypeLowCardinality.h"
#include "Interpreters/castColumn.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

using NullMap = PaddedPODArray<UInt8>;

/// ConcreteActions -- what to do when the index was found.

struct HasAction
{
    using ResultType = UInt8;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t) noexcept { current = 1; }
};

/// The index is returned starting from 1.
struct IndexOfAction
{
    using ResultType = UInt64;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType& current, size_t j) noexcept { current = j + 1; }
};

struct CountEqualAction
{
    using ResultType = UInt64;
    static constexpr const bool resume_execution = true;
    static constexpr void apply(ResultType & current, size_t) noexcept { ++current; }
};

/// How to perform the search depending on the arguments data types.
namespace Impl
{
template <
    class ConcreteAction,
    bool RightArgIsConstant = false,
    class IntegralInitial = UInt64,
    class IntegralResult = UInt64>
struct Main
{
private:
    using Initial = IntegralInitial;
    using Result = IntegralResult;

    using ResultType = typename ConcreteAction::ResultType;
    using ResultArr = PaddedPODArray<ResultType>;

    using ArrOffset = ColumnArray::Offset;
    using ArrOffsets = ColumnArray::Offsets;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsign-compare"

    static constexpr bool compare(const Initial & left, const PaddedPODArray<Result> & right, size_t, size_t i) noexcept
    {
        return left == right[i];
    }

    static constexpr bool compare(const PaddedPODArray<Initial> & left, const Result & right, size_t i, size_t) noexcept
    {
        return left[i] == right;
    }

    static constexpr bool compare(
            const PaddedPODArray<Initial> & left, const PaddedPODArray<Result> & right, size_t i, size_t j) noexcept
    {
        return left[i] == right[j];
    }

    /// LowCardinality
    static bool compare(const IColumn & left, const Result& right, size_t i, size_t)
    {
        return left.getUInt(i) == right;
    }

    /// Generic
    static bool compare(const IColumn& left, const IColumn& right, size_t i, size_t j)
    {
        return 0 == left.compareAt(i, RightArgIsConstant ? 0 : j, right, 1);
    }

#pragma GCC diagnostic pop

    static constexpr bool hasNull(const NullMap * const null_map, size_t i) noexcept { return (*null_map)[i]; }

    template <size_t Case, class Data, class Target>
    static void process(
        const Data & data, const ArrOffsets & offsets, const Target & target, ResultArr & result,
        [[maybe_unused]] const NullMap * const null_map_data,
        [[maybe_unused]] const NullMap * const null_map_item)
    {
        const size_t size = offsets.size();

        result.resize(size);

        ArrOffset current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const size_t array_size = offsets[i] - current_offset;
            ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                if constexpr (Case == 2) /// Right arg is Nullable
                     if (hasNull(null_map_item, i))
                        continue;

                if constexpr (Case == 3) /// Left arg is an array of Nullables
                    if (hasNull(null_map_data, current_offset + j))
                        continue;

                if constexpr (Case == 4) /// Both args are nullable
                {
                    const bool right_is_null = hasNull(null_map_data, current_offset + j);
                    const bool left_is_null = hasNull(null_map_item, i);

                    if (right_is_null != left_is_null)
                        continue;

                    if (!right_is_null && !compare(data, target, current_offset + j, i))
                        continue;
                }
                else if (!compare(data, target, current_offset + j, i))
                        continue;

                ConcreteAction::apply(current, j);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

public:
    template <class Data, class Target>
    static void vector(
        const Data & data,
        const ArrOffsets & offsets,
        const Target & value,
        ResultArr & result,
        const NullMap * const null_map_data,
        const NullMap * const null_map_item)
    {
        if (!null_map_data && !null_map_item)
            process<1>(data, offsets, value, result, null_map_data, null_map_item);
        else if (!null_map_data && null_map_item)
            process<2>(data, offsets, value, result, null_map_data, null_map_item);
        else if (null_map_data && !null_map_item)
            process<3>(data, offsets, value, result, null_map_data, null_map_item);
        else
            process<4>(data, offsets, value, result, null_map_data, null_map_item);
    }
};

/// When the 2nd function argument is a NULL value.
template <class ConcreteAction>
struct Null
{
    using ResultType = typename ConcreteAction::ResultType;

    static void process(
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<ResultType> & result,
        [[maybe_unused]] const NullMap * null_map_data)
    {
        const size_t size = offsets.size();

        if (!null_map_data)
        {
            result.resize_fill(size);
            return;
        }

        result.resize(size);

        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            ResultType current = 0;
            const size_t array_size = offsets[i] - current_offset;

            for (size_t j = 0; j < array_size; ++j)
            {
                if (!(*null_map_data)[current_offset + j])
                    continue;

                ConcreteAction::apply(current, j);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

template <class ConcreteAction>
struct String
{
private:
    using Offset = ColumnString::Offset;
    template <bool IsConst> using OffsetT = std::conditional_t<IsConst, Offset, const ColumnString::Offsets &>;
    using ArrayOffset = ColumnArray::Offset;
    using ResultType = typename ConcreteAction::ResultType;

    template <bool IsConst, bool HasNullMapData, bool HasNullMapItem>
    static void processImpl(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & item_values,
        OffsetT<IsConst> item_offsets,
        PaddedPODArray<ResultType> & result,
        [[maybe_unused]] const NullMap * data_map,
        [[maybe_unused]] const NullMap * item_map)
    {
        const size_t size = offsets.size();

        result.resize(size);

        ArrayOffset current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const ArrayOffset array_size = offsets[i] - current_offset;

            [[maybe_unused]] Offset value_pos = 0;
            [[maybe_unused]] Offset value_size = 0;

            if constexpr (!IsConst) // workaround because ?: ternary operator is not constexpr
            {
                if (0 != i) value_pos = item_offsets[i - 1];
                value_size = item_offsets[i] - value_pos;
            }

            ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                const ArrayOffset string_pos = current_offset + j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                const ArrayOffset string_size = string_offsets[current_offset + j] - string_pos - IsConst * 1;

                if constexpr (IsConst)
                {
                    if constexpr (HasNullMapData)
                        if ((*data_map)[current_offset + j])
                            continue;

                    if (!memequalSmallAllowOverflow15(item_values.data(), item_offsets, &data[string_pos], string_size))
                        continue;
                }
                else if constexpr (HasNullMapData)
                {
                    if ((*data_map)[current_offset + j])
                    {
                        if constexpr (!HasNullMapItem)
                            continue;

                        if (!(*item_map)[i])
                            continue;
                    }
                    else if (!memequalSmallAllowOverflow15(&item_values[value_pos], value_size, &data[string_pos], string_size))
                        continue;
                }
                else if (!memequalSmallAllowOverflow15(&item_values[value_pos], value_size, &data[string_pos], string_size))
                    continue;

                ConcreteAction::apply(current, j);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    template <bool IsConst>
    static inline void invokeCheckNullMaps(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & str_offsets, const ColumnString::Chars & values,
        OffsetT<IsConst> item_offsets,
        PaddedPODArray<ResultType> & result, const NullMap * data_map, const NullMap * item_map)
    {
        if (data_map && item_map)
            processImpl<IsConst, true, true>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else if (data_map)
            processImpl<IsConst, true, false>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else if (item_map)
            processImpl<IsConst, false, true>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else
            processImpl<IsConst, false, false>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
    }

public:
    static inline void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        Offset item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map)
    {
        invokeCheckNullMaps<true>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }

    static inline void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        const ColumnString::Offsets & item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map)
    {
        invokeCheckNullMaps<false>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }
};
}

template <class ConcreteAction, class Name>
class FunctionArrayIndex : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());

        if (!array_type)
            throw Exception("First argument for function " + getName() + " must be an array.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (!arguments[1]->onlyNull() && !allowArguments(array_type->getNestedType(), arguments[1]))
            throw Exception("Types of array and 2nd argument of function \""
                + getName() + "\" must be identical up to nullability, cardinality, "
                "numeric types, or Enum and numeric type. Passed: "
                + arguments[0]->getName() + " and " + arguments[1]->getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    /**
      * If one or both arguments passed to this function are nullable,
      * we create a new column that contains non-nullable arguments:
      *
      * - if the 1st argument is a non-constant array of nullable values,
      * it is turned into a non-constant array of ordinary values + a null
      * byte map;
      * - if the 2nd argument is a nullable value, it is turned into an
      * ordinary value + a null byte map.
      *
      * Note that since constant arrays have quite a specific structure
      * (they are vectors of Fields, which may represent the NULL value),
      * they do not require any preprocessing.
      */
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const ColumnPtr & ptr = arguments[0].column;

        /**
         * The columns here have two general cases, either being Array(T) or Const(Array(T)).
         * The last type will return nullptr after casting to ColumnArray, so we leave the casting
         * to execute* functions.
         */
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(ptr.get());
        const ColumnNullable * nullable = nullptr;

        if (col_array)
            nullable = checkAndGetColumn<ColumnNullable>(col_array->getData());

        auto & arg_column = arguments[1].column;
        const ColumnNullable * arg_nullable = checkAndGetColumn<ColumnNullable>(*arg_column);

        if (!nullable && !arg_nullable)
            return executeOnNonNullable(arguments, result_type);
        else
        {
            /**
             * To correctly process the Nullable values (either #col_array, #arg_column or both) we create a new columns
             * and operate on it. The columns structure follows:
             * {0, 1, 2, 3, 4}
             * {data (array) argument, "value" argument, data null map, "value" null map, function result}.
             */
            ColumnsWithTypeAndName source_columns(4);

            if (nullable)
            {
                const auto & nested_col = nullable->getNestedColumnPtr();

                auto & data = source_columns[0];

                data.column = ColumnArray::create(nested_col, col_array->getOffsetsPtr());
                data.type = std::make_shared<DataTypeArray>(
                    static_cast<const DataTypeNullable &>(
                        *static_cast<const DataTypeArray &>(
                            *arguments[0].type
                        ).getNestedType()
                    ).getNestedType());

                auto & null_map = source_columns[2];

                null_map.column = nullable->getNullMapColumnPtr();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & data = source_columns[0];
                data = arguments[0];
            }

            if (arg_nullable)
            {
                auto & arg = source_columns[1];
                arg.column = arg_nullable->getNestedColumnPtr();
                arg.type =
                    static_cast<const DataTypeNullable &>(
                        *arguments[1].type
                    ).getNestedType();

                auto & null_map = source_columns[3];
                null_map.column = arg_nullable->getNullMapColumnPtr();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & arg = source_columns[1];
                arg = arguments[1];
            }

            /// Now perform the function.
            return executeOnNonNullable(source_columns, result_type);
        }
    }

private:
    using ResultType = typename ConcreteAction::ResultType;
    using ResultColumnType = ColumnVector<ResultType>;
    using ResultColumnPtr = decltype(ResultColumnType::create());

    using NullMaps = std::pair<const NullMap *, const NullMap *>;

    struct ExecutionData
    {
        const IColumn& left;
        const IColumn& right;
        const ColumnArray::Offsets& offsets;
        ColumnPtr result_column;
        NullMaps maps;
        ResultColumnPtr result { ResultColumnType::create() };

        inline void moveResult() { result_column = std::move(result); }
    };

    static inline bool allowNested(const DataTypePtr & left, const DataTypePtr & right)
    {
        return ((isNativeNumber(left) || isEnum(left)) && isNativeNumber(right)) || left->equals(*right);
    }

    static inline bool allowArguments(const DataTypePtr & array_inner_type, const DataTypePtr & arg)
    {
        if (allowNested(array_inner_type, arg))
            return true;

        /// Nullable

        const bool array_is_nullable = array_inner_type->isNullable();
        const bool arg_is_nullable = arg->isNullable();

        const DataTypePtr arg_or_arg_nullable_nested = arg_is_nullable
            ? checkAndGetDataType<DataTypeNullable>(arg.get())->getNestedType()
            : arg;

        if (array_is_nullable) // comparing Array(Nullable(T)) elem and U
        {
            const DataTypePtr array_nullable_nested =
                checkAndGetDataType<DataTypeNullable>(array_inner_type.get())->getNestedType();

            // We also allow Nullable(T) and LC(U) if the Nullable(T) and U are allowed,
            // the LC(U) will be converted to U.
            return allowNested(
                    array_nullable_nested,
                    recursiveRemoveLowCardinality(arg_or_arg_nullable_nested));
        }
        else if (arg_is_nullable) // cannot compare Array(T) elem (namely, T) and Nullable(T)
            return false;

        /// LowCardinality

        const auto * const array_lc_ptr = checkAndGetDataType<DataTypeLowCardinality>(array_inner_type.get());
        const auto * const arg_lc_ptr = checkAndGetDataType<DataTypeLowCardinality>(arg.get());

        const DataTypePtr array_lc_inner_type = recursiveRemoveLowCardinality(array_inner_type);
        const DataTypePtr arg_lc_inner_type = recursiveRemoveLowCardinality(arg);

        const bool array_is_lc = nullptr != array_lc_ptr;
        const bool arg_is_lc = nullptr != arg_lc_ptr;

        const bool array_lc_inner_type_is_nullable = array_is_lc && array_lc_inner_type->isNullable();
        const bool arg_lc_inner_type_is_nullable = arg_is_lc && arg_lc_inner_type->isNullable();

        if (array_is_lc) // comparing LC(T) and U
        {
            const DataTypePtr array_lc_nested_or_lc_nullable_nested = array_lc_inner_type_is_nullable
                ? checkAndGetDataType<DataTypeNullable>(array_lc_inner_type.get())->getNestedType()
                : array_lc_inner_type;

            if (arg_is_lc) // comparing LC(T) and LC(U)
            {
                const DataTypePtr arg_lc_nested_or_lc_nullable_nested = arg_lc_inner_type_is_nullable
                    ? checkAndGetDataType<DataTypeNullable>(arg_lc_inner_type.get())->getNestedType()
                    : arg_lc_inner_type;

                return allowNested(
                        array_lc_nested_or_lc_nullable_nested,
                        arg_lc_nested_or_lc_nullable_nested);
            }
            else if (arg_is_nullable) // Comparing LC(T) and Nullable(U)
            {
                if (!array_lc_inner_type_is_nullable)
                    return false; // Can't compare Array(LC(U)) elem and Nullable(T);

                return allowNested(
                        array_lc_nested_or_lc_nullable_nested,
                        arg_or_arg_nullable_nested);
            }
            else // Comparing LC(T) and U (U neither Nullable nor LC)
                return allowNested(array_lc_nested_or_lc_nullable_nested, arg);
        }

        if (arg_is_lc) // Allow T and LC(U) if U and T are allowed (the low cardinality column will be converted).
            return allowNested(array_inner_type, arg_lc_inner_type);

        return false;
    }

#define INTEGRAL_TPL_PACK UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64

    ColumnPtr executeOnNonNullable(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        if (const auto* const left_arr = checkAndGetColumn<ColumnArray>(arguments[0].column.get()))
        {
            if (checkAndGetColumn<ColumnLowCardinality>(&left_arr->getData()))
            {
                if (auto res = executeLowCardinality(arguments))
                    return res;

                throw Exception(
                    "Illegal internal type of first argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
            }
        }

        ColumnPtr res;
        if (!((res = executeIntegral<INTEGRAL_TPL_PACK>(arguments))
              || (res = executeConst(arguments, result_type))
              || (res = executeString(arguments))
              || (res = executeGeneric(arguments))))
            throw Exception(
                "Illegal internal type of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);

        return res;
    }

    /**
     * The Array's internal data type may be quite tricky (containing a Nullable type somewhere). To process the
     * Nullable types correctly, for each data type specialisation we provide two null maps (one for the data and one
     * for the items). By convention they are passed as the third and the fourth argument, respectively
     * (counting from 1).
     *
     * @return {nullptr, nullptr} if there are less than 3 arguments.
     * @return {null_map_data, nullptr} if there are three arguments
     * @return {nullptr, null_map_item} if there are four arguments but the third is missing.
     * @return {null_map_data, null_map_item} if there are four arguments.
     */
    static NullMaps getNullMaps(const ColumnsWithTypeAndName & arguments) noexcept
    {
        if (arguments.size() < 3)
            return {nullptr, nullptr};

        const NullMap * null_map_data = nullptr;
        const NullMap * null_map_item = nullptr;

        if (const auto & data_map = arguments[2].column; data_map)
            null_map_data = &assert_cast<const ColumnUInt8 &>(*data_map).getData();

        if (const auto & item_map = arguments[3].column; item_map)
            null_map_item = &assert_cast<const ColumnUInt8 &>(*item_map).getData();

        return {null_map_data, null_map_item};
    }

    /**
     * Given a variadic pack #Integral, apply executeIntegralExpanded with such parameters:
     * Integral s = {s1, s2, ...}
     * (s1, s1, s2, ...), (s2, s1, s2, ...), (s3, s1, s2, ...)
     */
    template <class ...Integral>
    static inline ColumnPtr executeIntegral(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnArray * const left = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!left)
            return nullptr;

        const ColumnPtr right_converted_ptr = arguments[1].column->convertToFullColumnIfLowCardinality();
        const IColumn& right = *right_converted_ptr.get();

        ExecutionData data = {
            left->getData(),
            right,
            left->getOffsets(),
            nullptr,
            getNullMaps(arguments)
        };

        if (executeIntegral<Integral...>(data))
            return data.result_column;

        return nullptr;
    }

    template <class ...Integral>
    static inline bool executeIntegral(ExecutionData& data)
    {
        return (executeIntegralExpanded<Integral, Integral...>(data) || ...);
    }

    /// Invoke executeIntegralImpl with such parameters: (A, other1), (A, other2), ...
    template <class A, class ...Other>
    static inline bool executeIntegralExpanded(ExecutionData& data)
    {
        return (executeIntegralImpl<A, Other>(data) || ...);
    }

    /**
     * The internal data type of the first argument (target array), if it's integral, like UInt8, may differ from the
     * second argument, namely, the @e value, so it's possible to invoke the <tt>has(Array(Int8), UInt64)</tt> e.g.
     * so we have to check all possible variants for #Initial and #Resulting types.
     */
    template <class Initial, class Resulting>
    static bool executeIntegralImpl(ExecutionData& data)
    {
        const ColumnVector<Initial> * col_nested = checkAndGetColumn<ColumnVector<Initial>>(&data.left);

        if (!col_nested)
            return false;

        const auto [null_map_data, null_map_item] = data.maps;

        if (data.right.onlyNull())
            Impl::Null<ConcreteAction>::process(
                data.offsets,
                data.result->getData(),
                null_map_data);
        else if (const auto item_arg_const = checkAndGetColumnConst<ColumnVector<Resulting>>(&data.right))
            Impl::Main<ConcreteAction, true, Initial, Resulting>::vector(
                col_nested->getData(),
                data.offsets,
                item_arg_const->template getValue<Resulting>(),
                data.result->getData(),
                null_map_data,
                nullptr);
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnVector<Resulting>>(&data.right))
            Impl::Main<ConcreteAction, false, Initial, Resulting>::vector(
                col_nested->getData(),
                data.offsets,
                item_arg_vector->getData(),
                data.result->getData(),
                null_map_data,
                null_map_item);
        else
            return false;

        data.moveResult();
        return true;
    }

    /**
     * Catches arguments of type LC(T) (left) and U (right).
     *
     * The perftests
     * https://clickhouse-test-reports.s3.yandex.net/12550/2d27fa0fa8c198a82bf1fe3625050ccf56695976/integration_tests_(release).html
     * showed that the amount of action needed to convert the non-constant right argument to the index column
     * (similar to the left one's) is significantly higher than converting the array itself to an ordinary column.
     *
     * So, in terms of performance it's more optimal to fall back to default implementation and catch only constant
     * right arguments.
     *
     * Tips and tricks tried can be found at https://github.com/ClickHouse/ClickHouse/pull/12550 .
     */
    static ColumnPtr executeLowCardinality(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnArray * const col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!col_array)
            return nullptr;

        const ColumnLowCardinality * const col_lc = checkAndGetColumn<ColumnLowCardinality>(&col_array->getData());

        if (!col_lc)
            return nullptr;

        const auto [null_map_data, null_map_item] = getNullMaps(arguments);

        const IColumn& col_arg = *arguments[1].column.get();

        if (const ColumnConst * const col_arg_const = checkAndGetColumn<ColumnConst>(col_arg))
        {
            const IColumnUnique& col_lc_dict = col_lc->getDictionary();

            const bool different_inner_types = col_lc_dict.isNullable()
                ? !col_arg_const->structureEquals(*col_lc_dict.getNestedColumn().get())
                : true; // Can't compare so ignore this check

            const bool use_cloned_arg = col_arg_const->isNumeric()
                // outer types do not match
                && !col_arg_const->structureEquals(col_lc_dict)
                // inner types do not match (like A and Nullable(B) or A and Const(B));
                && different_inner_types;

            const DataTypeArray * const array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
            const DataTypePtr target_type_ptr = recursiveRemoveLowCardinality(array_type->getNestedType());

            const ColumnPtr col_arg_cloned = use_cloned_arg
                ? castColumn(arguments[1], target_type_ptr)
                : col_arg_const->getPtr();

            const StringRef elem = col_arg_cloned->getDataAt(0);
            ResultColumnPtr col_result = ResultColumnType::create();

            UInt64 index = 0;

            if (elem != EMPTY_STRING_REF)
            {
                if (std::optional<UInt64> maybe_index = col_lc_dict.getOrFindValueIndex(elem); maybe_index)
                    index = *maybe_index;
                else
                {
                    const size_t offsets_size = col_array->getOffsets().size();
                    auto& data = col_result->getData();

                    data.resize_fill(offsets_size);

                    return col_result;
                }
            }

            Impl::Main<ConcreteAction, true>::vector(
                col_lc->getIndexes(),
                col_array->getOffsets(),
                index,
                col_result->getData(),
                null_map_data,
                null_map_item);

            return col_result;
        }
        else if (col_lc->nestedIsNullable()) // LC(Nullable(T)) and U
        {
            const ColumnPtr left_casted = col_lc->convertToFullColumnIfLowCardinality(); // Nullable(T)
            const ColumnNullable& left_nullable = *checkAndGetColumn<ColumnNullable>(left_casted.get());

            const NullMap * const null_map_left_casted = &left_nullable.getNullMapColumn().getData();

            const IColumn& left_ptr = left_nullable.getNestedColumn();

            const ColumnPtr right_casted = col_arg.convertToFullColumnIfLowCardinality();
            const ColumnNullable * const right_nullable = checkAndGetColumn<ColumnNullable>(right_casted.get());

            const NullMap * const null_map_right_casted = right_nullable
                ? &right_nullable->getNullMapColumn().getData()
                : null_map_item;

            const IColumn& right_ptr = right_nullable
                ? right_nullable->getNestedColumn()
                : *right_casted.get();

            ExecutionData data = {
                left_ptr, right_ptr,
                col_array->getOffsets(),
                nullptr,
                {null_map_left_casted, null_map_right_casted}};

            if (dispatchConvertedLCColumns(data))
                return data.result_column;
        }
        else // LC(T) and U, T not Nullable
        {
            if (col_arg.isNullable())
                return nullptr;

            if (const auto* const arg_lc = checkAndGetColumn<ColumnLowCardinality>(&col_arg);
                arg_lc && arg_lc->isNullable())
                return nullptr;

            // LC(T) and U (possibly LC(V))

            const ColumnPtr left_casted = col_lc->convertToFullColumnIfLowCardinality();
            const ColumnPtr right_casted = col_arg.convertToFullColumnIfLowCardinality();

            ExecutionData data = {
                *left_casted.get(), *right_casted.get(), col_array->getOffsets(),
                nullptr, {null_map_data, null_map_item}
            };

            if (dispatchConvertedLCColumns(data))
                return data.result_column;
        }

        return nullptr;
    }

    static bool dispatchConvertedLCColumns(ExecutionData& data)
    {
        if (data.left.isNumeric() && data.right.isNumeric()) // ColumnArrays
            return executeIntegral<INTEGRAL_TPL_PACK>(data);

        if (checkAndGetColumn<ColumnString>(&data.left))
            return executeStringImpl(data);

        Impl::Main<ConcreteAction, true>::vector(
            data.left,
            data.offsets, data.right,
            data.result->getData(),
            data.maps.first, data.maps.second);

        data.moveResult();
        return true;
    }

#undef INTEGRAL_TPL_PACK

    static ColumnPtr executeString(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnArray * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!array)
            return nullptr;

        const ColumnString * left = checkAndGetColumn<ColumnString>(&array->getData());

        if (!left)
            return nullptr;

        const ColumnPtr right_ptr = arguments[1].column->convertToFullColumnIfLowCardinality();
        const IColumn & right = *right_ptr.get();

        ExecutionData data = {
            *left, right, array->getOffsets(),
            nullptr, getNullMaps(arguments),
            std::move(ResultColumnType::create())
        };

        if (executeStringImpl(data))
            return data.result_column;

        return nullptr;
    }

    static bool executeStringImpl(ExecutionData& data)
    {
        const auto [null_map_data, null_map_item] = data.maps;
        const ColumnString& left = *typeid_cast<const ColumnString* const>(&data.left);

        if (data.right.onlyNull())
            Impl::Null<ConcreteAction>::process(
                data.offsets,
                data.result->getData(),
                null_map_data);
        else if (const auto *const item_arg_const = checkAndGetColumnConstStringOrFixedString(&data.right))
        {
            const ColumnString * item_const_string =
                checkAndGetColumn<ColumnString>(&item_arg_const->getDataColumn());

            const ColumnFixedString * item_const_fixedstring =
                checkAndGetColumn<ColumnFixedString>(&item_arg_const->getDataColumn());

            if (item_const_string)
                Impl::String<ConcreteAction>::process(
                    left.getChars(),
                    data.offsets,
                    left.getOffsets(),
                    item_const_string->getChars(),
                    item_const_string->getDataAt(0).size,
                    data.result->getData(),
                    null_map_data,
                    null_map_item);
            else if (item_const_fixedstring)
                Impl::String<ConcreteAction>::process(
                    left.getChars(),
                    data.offsets,
                    left.getOffsets(),
                    item_const_fixedstring->getChars(),
                    item_const_fixedstring->getN(),
                    data.result->getData(),
                    null_map_data,
                    null_map_item);
            else
                throw Exception(
                    "Logical error: ColumnConst contains not String nor FixedString column",
                        ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto *const item_arg_vector = checkAndGetColumn<ColumnString>(&data.right))
        {
            Impl::String<ConcreteAction>::process(
                left.getChars(),
                data.offsets,
                left.getOffsets(),
                item_arg_vector->getChars(),
                item_arg_vector->getOffsets(),
                data.result->getData(),
                null_map_data,
                null_map_item);
        }
        else
            return false;

        data.moveResult();
        return true;
    }

    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());

        if (!col_array)
            return nullptr;

        Array arr = col_array->getValue<Array>();

        const ColumnPtr right_ptr = arguments[1].column->convertToFullColumnIfLowCardinality();
        const IColumn * item_arg = right_ptr.get();

        if (isColumnConst(*item_arg))
        {
            ResultType current = 0;
            const auto & value = (*item_arg)[0];

            for (size_t i = 0, size = arr.size(); i < size; ++i)
            {
                if (!applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                    continue;

                ConcreteAction::apply(current, i);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            return result_type->createColumnConst(item_arg->size(), static_cast<ResultType>(current));
        }
        else
        {
            /// Null map of the 2nd function argument, if it applies.
            const NullMap * null_map = nullptr;

            if (arguments.size() > 2)
                if (const auto & col = arguments[3].column; col)
                    null_map = &assert_cast<const ColumnUInt8 &>(*col).getData();

            const size_t size = item_arg->size();
            auto col_res = ResultColumnType::create(size);

            auto & data = col_res->getData();

            for (size_t row = 0; row < size; ++row)
            {
                const auto & value = (*item_arg)[row];

                data[row] = 0;

                for (size_t i = 0, arr_size = arr.size(); i < arr_size; ++i)
                {
                    if (arr[i].isNull())
                    {
                        if (!null_map)
                            continue;

                        if (!(*null_map)[row])
                            continue;
                    }
                    else if (!applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                        continue;

                    ConcreteAction::apply(data[row], i);

                    if constexpr (!ConcreteAction::resume_execution)
                        break;
                }
            }

            return col_res;
        }
    }

    static ColumnPtr executeGeneric(const ColumnsWithTypeAndName & arguments)
    {
        const ColumnArray * col = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

        if (!col)
            return nullptr;

        const IColumn & col_nested = col->getData();

        const ColumnPtr right_ptr = arguments[1].column->convertToFullColumnIfLowCardinality();
        const IColumn & item_arg = *right_ptr.get();

        auto col_res = ResultColumnType::create();

        auto [null_map_data, null_map_item] = getNullMaps(arguments);

        if (item_arg.onlyNull())
            Impl::Null<ConcreteAction>::process(
                col->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (isColumnConst(item_arg))
            Impl::Main<ConcreteAction, true>::vector(
                col_nested,
                col->getOffsets(),
                typeid_cast<const ColumnConst &>(item_arg).getDataColumn(),
                col_res->getData(), /// TODO This is wrong.
                null_map_data,
                nullptr);
        else
            Impl::Main<ConcreteAction>::vector(
                col_nested,
                col->getOffsets(),
                item_arg,
                col_res->getData(),
                null_map_data,
                null_map_item);

        return col_res;
    }
};
}
