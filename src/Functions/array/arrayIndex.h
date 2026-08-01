#pragma once

#include <cstddef>
#include <type_traits>

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/LowCardinalityExecutionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeObject.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_CONVERT_TYPE;
    extern const int CANNOT_PARSE_BOOL;
    extern const int CANNOT_PARSE_DATE;
    extern const int CANNOT_PARSE_DATETIME;
    extern const int CANNOT_PARSE_IPV4;
    extern const int CANNOT_PARSE_IPV6;
    extern const int CANNOT_PARSE_NUMBER;
    extern const int CANNOT_PARSE_TEXT;
    extern const int CANNOT_PARSE_UUID;
    extern const int DECIMAL_OVERFLOW;
    extern const int NOT_IMPLEMENTED;
    extern const int TOO_LARGE_STRING_SIZE;
    extern const int UNKNOWN_ELEMENT_OF_ENUM;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
}

using NullMap = PaddedPODArray<UInt8>;

/// ConcreteActions -- what to do when the index was found.

struct HasAction
{
    using ResultType = UInt8;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType & current, size_t) noexcept { current = 1; }
};

/// The index is returned starting from 1.
struct IndexOfAction
{
    using ResultType = UInt64;
    static constexpr const bool resume_execution = false;
    static constexpr void apply(ResultType & current, size_t j) noexcept { current = j + 1; }
};

struct IndexOfAssumeSorted : public IndexOfAction
{
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
    typename ConcreteAction,
    bool RightArgIsConstant = false,
    typename IntegralInitial = UInt64,
    typename IntegralResult = UInt64>
struct Main
{
private:
    using Initial = IntegralInitial;
    using Result = IntegralResult;

    using ResultType = typename ConcreteAction::ResultType;
    using ResultArr = PaddedPODArray<ResultType>;

    using ArrOffset = ColumnArray::Offset;
    using ArrOffsets = ColumnArray::Offsets;

    static constexpr bool compare(const Initial & left, const PaddedPODArray<Result> & right, size_t, size_t i)
    {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
        return left == right[i];
#pragma clang diagnostic pop
    }

    static constexpr bool compare(const PaddedPODArray<Initial> & left, const Result & right, size_t i, size_t)
    {
        if constexpr (std::is_floating_point_v<Initial> && !std::is_floating_point_v<Result>)
        {
            return left[i] == static_cast<Initial>(right);
        }
        else if constexpr (!std::is_floating_point_v<Initial> && std::is_floating_point_v<Result>)
        {
            return static_cast<Result>(left[i]) == right;
        }
        else
        {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wdouble-promotion"
            return left[i] == right;
#pragma clang diagnostic pop
        }
    }

    static constexpr bool compare(
            const PaddedPODArray<Initial> & left, const PaddedPODArray<Result> & right, size_t i, size_t j)
    {
        if constexpr (std::is_floating_point_v<Initial> && !std::is_floating_point_v<Result>)
        {
            return left[i] == static_cast<Initial>(right[j]);
        }
        else if constexpr (!std::is_floating_point_v<Initial> && std::is_floating_point_v<Result>)
        {
            return static_cast<Result>(left[i]) == right[j];
        }
        else
        {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wdouble-promotion"
            return left[i] == right[j];
#pragma clang diagnostic pop
        }
    }

    /// LowCardinality
    static bool compare(const IColumn & left, const Result & right, size_t i, size_t)
    {
        return left.getUInt(i) == right;
    }

    /// Generic
    static bool compare(const IColumn & left, const IColumn & right, size_t i, size_t j)
    {
        return 0 == left.compareAt(i, RightArgIsConstant ? 0 : j, right, 1);
    }

    static bool compare(const Array & arr, const Field& rhs, size_t pos, size_t)
    {
        return accurateEquals(arr[pos], rhs);
    }

    static constexpr bool lessOrEqual(const PaddedPODArray<Initial> & left, const Result & right, size_t i, size_t)
    {
        if constexpr (std::is_floating_point_v<Initial> && !std::is_floating_point_v<Result>)
        {
            return left[i] >= static_cast<Initial>(right);
        }
        else if constexpr (!std::is_floating_point_v<Initial> && std::is_floating_point_v<Result>)
        {
            return static_cast<Result>(left[i]) >= right;
        }
        else
        {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"
#pragma clang diagnostic ignored "-Wdouble-promotion"
            return left[i] >= right;
#pragma clang diagnostic pop
        }
    }

    static bool lessOrEqual(const IColumn & left, const Result & right, size_t i, size_t) { return left[i] >= right; }

    static bool lessOrEqual(const Array & arr, const Field & rhs, size_t pos, size_t)
    {
        return accurateLessOrEqual(rhs, arr[pos]);
    }

public:
    /** Assuming that the array is sorted, use a binary search */
    template <typename Data, typename Target>
    static constexpr ResultType lowerBound(const Data & data, const Target & target, size_t array_size, ArrOffset current_offset)
    {
        ResultType current = 0;
        size_t low = 0;
        size_t high = array_size;
        while (high - low > 0)
        {
            auto middle = low + ((high - low) >> 1);
            auto compare_result = lessOrEqual(data, target, current_offset + middle, 0);
            /// avoid conditional branching
            high = compare_result ? middle : high;
            low = compare_result ? low : middle + 1;
        }
        if (low < array_size && compare(data, target, current_offset + low, 0))
        {
            ConcreteAction::apply(current, low);
        }
        return current;
    }

    template <size_t Case, typename Data, typename Target>
    static constexpr ResultType linearSearch(
        const Data & data,
        const Target & target,
        size_t array_size,
        const NullMap * const null_map_data,
        const NullMap * const null_map_item,
        size_t row_index,
        ArrOffset current_offset)
    {
        ResultType current = 0;
        for (size_t j = 0; j < array_size; ++j)
        {
            if constexpr (Case == 2) /// Right arg is Nullable
                if (hasNull(null_map_item, row_index))
                    continue;

            if constexpr (Case == 3) /// Left arg is an array of Nullables
                if (hasNull(null_map_data, current_offset + j))
                    continue;

            if constexpr (Case == 4) /// Both args are nullable
            {
                const bool right_is_null = hasNull(null_map_data, current_offset + j);
                const bool left_is_null = hasNull(null_map_item, row_index);

                if (right_is_null != left_is_null)
                    continue;

                if (!right_is_null && !compare(data, target, current_offset + j, row_index))
                    continue;
            }
            else if (!compare(data, target, current_offset + j, row_index))
                continue;

            ConcreteAction::apply(current, j);

            if constexpr (!ConcreteAction::resume_execution)
                break;
        }
        return current;
    }

    static ResultType linearSearchConst(const Array & arr, const Field & value)
    {
        ResultType current = 0;
        for (size_t i = 0, size = arr.size(); i < size; ++i)
        {
            if (!accurateEquals(arr[i], value))
                continue;

            ConcreteAction::apply(current, i);

            if constexpr (!ConcreteAction::resume_execution)
                break;
        }
        return current;
    }

private:
    /** Looking for the target element index in the data (array) */
    template <size_t Case, typename Data, typename Target>
    static constexpr ResultType getIndex(
        const Data & data,
        const Target & target,
        size_t array_size,
        const NullMap * const null_map_data,
        const NullMap * const null_map_item,
        size_t row_index,
        ArrOffset current_offset)
    {
        /** Use binary search if the following conditions are met.
          *   1. The array type is not nullable. (Case = 1)
          *   2. Target is not a column or an array.
          */
        if constexpr (
            std::is_same_v<ConcreteAction, IndexOfAssumeSorted> && !std::is_same_v<Target, PaddedPODArray<Result>>
            && !std::is_same_v<Target, IColumn> && Case == 1)
        {
            return lowerBound(data, target, array_size, current_offset);
        }
        return linearSearch<Case>(data, target, array_size, null_map_data, null_map_item, row_index, current_offset);
    }

    static constexpr bool hasNull(const NullMap * const null_map, size_t i) noexcept { return (*null_map)[i]; }

    template <size_t Case, typename Data, typename Target>
    static void process(
        const Data & data,
        const ArrOffsets & offsets,
        const Target & target,
        ResultArr & result,
        [[maybe_unused]] const NullMap * const null_map_data,
        [[maybe_unused]] const NullMap * const null_map_item)
    {
        if constexpr (std::is_same_v<Data, IColumn> && std::is_same_v<Target, IColumn>)
        {
            /// Generic variant is using IColumn::compare function that only allows to compare columns of identical types.
            if (typeid(data) != typeid(target))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Columns {} and {} cannot be compared", data.getName(), target.getName());
        }

        const size_t size = offsets.size();
        result.resize(size);

        ArrOffset current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const size_t array_size = offsets[i] - current_offset;
            result[i] = getIndex<Case>(data, target, array_size, null_map_data, null_map_item, i, current_offset);
            current_offset = offsets[i];
        }
    }

public:
    template <typename Data, typename Target>
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
template <typename ConcreteAction>
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

template <typename ConcreteAction>
struct String
{
private:
    using Offset = ColumnString::Offset;
    template <bool IsConst> using OffsetT = std::conditional_t<IsConst, Offset, const ColumnString::Offsets &>;
    using ArrayOffset = ColumnArray::Offset;
    using ResultType = typename ConcreteAction::ResultType;

    /// Both operands live in PaddedPODArray, so the AllowOverflow15 primitives may over-read safely.
    template <bool ZeroPadded>
    static bool needleEquals(const UInt8 * needle, size_t needle_size, const UInt8 * elem, size_t elem_size)
    {
        if constexpr (ZeroPadded)
            return memequalSmallLikeZeroPaddedAllowOverflow15(needle, needle_size, elem, elem_size);
        else
            return memequalSmallAllowOverflow15(needle, needle_size, elem, elem_size);
    }

    template <bool IsConst, bool HasNullMapData, bool HasNullMapItem, bool ZeroPadded>
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
                value_pos = item_offsets[i - 1];
                value_size = item_offsets[i] - value_pos;
            }

            ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                const ArrayOffset string_pos = string_offsets[current_offset + j - 1];
                const ArrayOffset string_size = string_offsets[current_offset + j] - string_pos;

                if constexpr (IsConst)
                {
                    if constexpr (HasNullMapData)
                        if ((*data_map)[current_offset + j])
                            continue;

                    if (!needleEquals<ZeroPadded>(item_values.data(), item_offsets, &data[string_pos], string_size))
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
                    else if (!needleEquals<ZeroPadded>(&item_values[value_pos], value_size, &data[string_pos], string_size))
                        continue;
                }
                else if (!needleEquals<ZeroPadded>(&item_values[value_pos], value_size, &data[string_pos], string_size))
                    continue;

                ConcreteAction::apply(current, j);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    template <bool IsConst, bool ZeroPadded>
    static void invokeCheckNullMaps(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & str_offsets, const ColumnString::Chars & values,
        OffsetT<IsConst> item_offsets,
        PaddedPODArray<ResultType> & result, const NullMap * data_map, const NullMap * item_map)
    {
        if (data_map && item_map)
            processImpl<IsConst, true, true, ZeroPadded>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else if (data_map)
            processImpl<IsConst, true, false, ZeroPadded>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else if (item_map)
            processImpl<IsConst, false, true, ZeroPadded>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
        else
            processImpl<IsConst, false, false, ZeroPadded>(data, offsets, str_offsets, values, item_offsets, result, data_map, item_map);
    }

public:
    /// [zero_padded] must be set only for a constant `FixedString` needle: its trailing NUL padding
    /// is not part of the value, so equality has to ignore it to agree with `=`.
    static void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        Offset item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map, bool zero_padded = false)
    {
        if (zero_padded)
            invokeCheckNullMaps<true, true>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
        else
            invokeCheckNullMaps<true, false>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }

    static void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        const ColumnString::Offsets & item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map)
    {
        invokeCheckNullMaps<false, false>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }
};
}

template <typename ConcreteAction, typename Name>
class FunctionArrayIndex final : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String getName() const override { return name; }

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto first_argument_type = arguments[0].type;
        auto second_argument_type = arguments[1].type;

        const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(first_argument_type.get());
        const DataTypeMap * map_type = checkAndGetDataType<DataTypeMap>(first_argument_type.get());

        DataTypePtr inner_type;

        const DataTypeObject * object_type = checkAndGetDataType<DataTypeObject>(first_argument_type.get());
        if constexpr (std::is_same_v<ConcreteAction, HasAction>)
        {
            if (!array_type && !map_type && !object_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be an array, map or JSON. Actual {}",
                    getName(),
                    first_argument_type->getName());

            if (object_type)
            {
                if (!isStringOrFixedString(second_argument_type))
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Second argument for function {} must be String when the first argument is JSON. Actual {}",
                        getName(), second_argument_type->getName());

                return std::make_shared<DataTypeUInt8>();
            }

            inner_type = map_type ? map_type->getKeyType() : array_type->getNestedType();
        }
        else
        {
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be an array. Actual {}",
                    getName(),
                    first_argument_type->getName());

            inner_type = array_type->getNestedType();
        }

        if (!second_argument_type->onlyNull() && !allowArguments(inner_type, second_argument_type))
        {
            const char * first_argument_type_name = map_type ? "map" : "array";
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Types of {} and 2nd argument of function `{}` must be identical up to nullability, cardinality, "
                "numeric types, or Enum and numeric type. Passed: {} and {}.",
                first_argument_type_name,
                getName(),
                first_argument_type->getName(),
                second_argument_type->getName());
        }

        return std::make_shared<DataTypeNumber<ResultType>>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        if (auto res = executeMap(arguments, result_type))
            return res;

        if (auto res = executeObject(arguments, result_type))
            return res;

        if (auto res = executeArrayLowCardinality(arguments))
            return res;

        auto new_arguments = arguments;

        for (auto & argument : new_arguments)
        {
            argument.column = recursiveRemoveLowCardinality(argument.column);
            argument.type = recursiveRemoveLowCardinality(argument.type);
        }

        return executeArrayImpl(new_arguments, result_type);
    }

private:
    using ResultType = typename ConcreteAction::ResultType;
    using ResultColumnType = ColumnVector<ResultType>;
    using ResultColumnPtr = decltype(ResultColumnType::create());

    using NullMaps = std::pair<const NullMap *, const NullMap *>;

    static bool allowArguments(const DataTypePtr & inner_type, const DataTypePtr & arg)
    {
        auto inner_type_decayed = removeNullable(removeLowCardinality(inner_type));
        auto arg_decayed = removeNullable(removeLowCardinality(arg));

        return ((isNativeNumber(inner_type_decayed) || isEnum(inner_type_decayed)) && isNativeNumber(arg_decayed))
            || getLeastSupertype(DataTypes{inner_type_decayed, arg_decayed});
    }

    /** If one or both arguments passed to this function are nullable,
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
    ColumnPtr executeArrayImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        const ColumnPtr & ptr = arguments[0].column;

        /** The columns here have two general cases, either being Array(T) or Const(Array(T)).
          * The last type will return nullptr after casting to ColumnArray, so we leave the casting
          * to execute* functions.
          */
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(ptr.get());
        const ColumnNullable * nullable = nullptr;

        if (col_array)
            nullable = checkAndGetColumn<ColumnNullable>(&col_array->getData());

        const auto & arg_column = arguments[1].column;
        const ColumnNullable * arg_nullable = checkAndGetColumn<ColumnNullable>(&*arg_column);

        /** A constant nullable needle arrives as `Const(Nullable(T))`, which the check above does not
          * see through, so the handlers below would receive a shape none of them recognizes and the
          * value would fall through to `executeGeneric`, losing the `FixedString` layout. Peel the
          * `Nullable` off the constant so it becomes the `Const(T)` the handlers already treat
          * correctly. An all-NULL needle is left alone: `executeNothing` answers it.
          *
          * Only string-family needles are peeled. That is the set whose layout the padded comparison
          * is about, and it is what `needsZeroPaddedComparison` is defined over. A numeric needle must
          * keep reaching `executeGeneric`, which casts both sides to a common supertype; peeling it
          * would route it to `executeIntegral`, whose raw comparison equates a negative signed value
          * with its unsigned bit-pattern twin.
          */
        if (!arg_nullable && !arg_column->onlyNull()
            && WhichDataType(removeNullable(recursiveRemoveLowCardinality(arguments[1].type))).isStringOrFixedString())
        {
            if (const auto * arg_const = checkAndGetColumnConst<ColumnNullable>(&*arg_column))
            {
                auto unwrapped = arguments;
                unwrapped[1].column = ColumnConst::create(
                    assert_cast<const ColumnNullable &>(arg_const->getDataColumn()).getNestedColumnPtr(),
                    arg_const->size());
                unwrapped[1].type = removeNullable(arguments[1].type);

                return executeArrayImpl(unwrapped, result_type);
            }
        }

        if (!nullable && !arg_nullable)
        {
            return executeOnNonNullable(arguments, result_type);
        }

        /** To correctly process the Nullable values (either #col_array, #arg_column or both) we create a new columns
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
                static_cast<const DataTypeNullable &>(*static_cast<const DataTypeArray &>(*arguments[0].type).getNestedType())
                    .getNestedType());

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
            arg.type = static_cast<const DataTypeNullable &>(*arguments[1].type).getNestedType();

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

#define INTEGRAL_PACK UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64

    ColumnPtr executeOnNonNullable(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        ColumnPtr res;
        if (!((res = executeNothing(arguments))
              || (res = executeIntegral<INTEGRAL_PACK>(arguments))
              || (res = executeConst(arguments, result_type))
              || (res = executeString(arguments))
              || (res = executeGeneric(arguments))))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal internal type of first argument of function {}", getName());

        return res;
    }

#undef INTEGRAL_PACK

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
    static NullMaps getNullMaps(const ColumnsWithTypeAndName & arguments)
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

    struct ExecutionData
    {
        const IColumn & left;
        const IColumn & right;
        const ColumnArray::Offsets & offsets;
        NullMaps null_maps;
    };

    /**
     * Given a variadic pack #Integral, apply executeIntegralExpanded with such parameters:
     * Integral s = {s1, s2, ...}
     * (s1, s1, s2, ...), (s2, s1, s2, ...), (s3, s1, s2, ...)
     */
    template <typename... Integral>
    static ColumnPtr executeIntegral(const ColumnsWithTypeAndName & arguments)
    {
        const auto * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!array)
            return nullptr;

        ExecutionData data
        {
            .left = array->getData(),
            .right = *arguments[1].column,
            .offsets = array->getOffsets(),
            .null_maps = getNullMaps(arguments),
        };

        auto result = ResultColumnType::create();
        return executeIntegral<Integral...>(data, *result) ? std::move(result) : nullptr;
    }

    template <typename... Integral>
    static bool executeIntegral(const ExecutionData & data, ResultColumnType & result)
    {
        return (executeIntegralExpanded<Integral, Integral...>(data, result) || ...);
    }

    /// Invoke executeIntegralImpl with such parameters: (A, other1), (A, other2), ...
    template <typename A, typename... Other>
    static bool executeIntegralExpanded(const ExecutionData & data, ResultColumnType & result)
    {
        return (executeIntegralImpl<A, Other>(data, result) || ...);
    }

    /**
     * The internal data type of the first argument (target array), if it's integral, like UInt8, may differ from the
     * second argument, namely, the @e value, so it's possible to invoke the <tt>has(Array(Int8), UInt64)</tt> e.g.
     * so we have to check all possible variants for #Initial and #Resulting types.
     */
    template <typename Initial, typename Resulting>
    static bool executeIntegralImpl(const ExecutionData & data, ResultColumnType & result)
    {
        const auto * left_typed = checkAndGetColumn<ColumnVector<Initial>>(&data.left);
        if (!left_typed)
            return false;

        if (const auto * item_arg_const = checkAndGetColumnConst<ColumnVector<Resulting>>(&data.right))
            Impl::Main<ConcreteAction, true, Initial, Resulting>::vector(
                left_typed->getData(),
                data.offsets,
                item_arg_const->template getValue<Resulting>(),
                result.getData(),
                data.null_maps.first,
                nullptr);
        else if (const auto * item_arg_vector = checkAndGetColumn<ColumnVector<Resulting>>(&data.right))
            Impl::Main<ConcreteAction, false, Initial, Resulting>::vector(
                left_typed->getData(),
                data.offsets,
                item_arg_vector->getData(),
                result.getData(),
                data.null_maps.first,
                data.null_maps.second);
        else
            return false;

        return true;
    }

    /**
     * Catches arguments of type LowCardinality(T) (left) and U (right).
     *
     * The perftests showed that the amount of action needed to convert the non-constant right argument to the index column
     * (similar to the left one's) is significantly higher than converting the array itself to an ordinary column.
     *
     * So, in terms of performance it's more optimal to fall back to default implementation and catch only constant
     * right arguments.
     *
     * Tips and tricks tried can be found at https://github.com/ClickHouse/ClickHouse/pull/12550 .
     */

    /** Does casting [needle] into [element_type] preserve its value exactly?
      *
      * The dictionary lookup casts the needle down to the element type and then matches by bytes, so a
      * cast that changes the value makes the lookup ask about a value the needle does not denote. The
      * test is a ROUND TRIP rather than a single cast, because a one-way cast cannot report that it
      * lost anything: casting a non-midnight `DateTime` to `Date` truncates the time of day and
      * succeeds, so only converting back and comparing distinguishes it from an exact midnight.
      */
    /** Is [code] a way for the probe's own cast to say "this value does not convert"?
      *
      * Only these may be read as an answer about the VALUE. Anything else -- a memory limit, a
      * logical error, a cancellation -- is a failure of the probe itself and must propagate, or the
      * guard would silently decline on an unrelated fault and hide it.
      *
      * Each entry was raised by an actual `CAST` of a plausible needle: `TOO_LARGE_STRING_SIZE` by
      * `String -> FixedString` that does not fit, `CANNOT_PARSE_TEXT`/`_NUMBER` by a
      * `String -> UInt8`/`Decimal`, the dated/UUID/IP/Bool ones by a `String` to each of those,
      * `UNKNOWN_ELEMENT_OF_ENUM` by a `String -> Enum8`, `CANNOT_CONVERT_TYPE` by `nan -> Int64`,
      * `DECIMAL_OVERFLOW` by an out-of-range `Float -> Decimal64`, `ILLEGAL_TYPE_OF_ARGUMENT` by a
      * composite (`Array`, `Tuple`) needle against a scalar element, and
      * `VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE`/`NOT_IMPLEMENTED` by the accurate cast back and by pairs
      * that have no conversion at all.
      */
    static bool isNeedleConversionRefusal(int code)
    {
        return code == ErrorCodes::CANNOT_CONVERT_TYPE
            || code == ErrorCodes::CANNOT_PARSE_BOOL
            || code == ErrorCodes::CANNOT_PARSE_DATE
            || code == ErrorCodes::CANNOT_PARSE_DATETIME
            || code == ErrorCodes::CANNOT_PARSE_IPV4
            || code == ErrorCodes::CANNOT_PARSE_IPV6
            || code == ErrorCodes::CANNOT_PARSE_NUMBER
            || code == ErrorCodes::CANNOT_PARSE_TEXT
            || code == ErrorCodes::CANNOT_PARSE_UUID
            || code == ErrorCodes::DECIMAL_OVERFLOW
            || code == ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            || code == ErrorCodes::NOT_IMPLEMENTED
            || code == ErrorCodes::TOO_LARGE_STRING_SIZE
            || code == ErrorCodes::UNKNOWN_ELEMENT_OF_ENUM
            || code == ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    }

    static bool castPreservesNeedleValue(
        const ColumnPtr & needle_column, const DataTypePtr & needle_type, const DataTypePtr & element_type)
    {
        try
        {
            /// The down-cast is the very one `dictionaryIndexForConstant` performs, so it must be the
            /// same `castColumn`; the way back only has to report loss, so it may reject with a NULL.
            const auto narrowed = castColumn({needle_column, needle_type, ""}, element_type);
            if (narrowed->empty() || narrowed->isNullAt(0))
                return false;

            const auto restored
                = castColumnAccurateOrNull({narrowed, element_type, ""}, makeNullable(needle_type));
            if (restored->empty() || restored->isNullAt(0))
                return false;

            return accurateEquals((*restored)[0], (*needle_column)[0]);
        }
        catch (const Exception & e)
        {
            /// Ok: a needle the cast REFUSES cannot denote an element value. Any other failure is
            /// the probe's own, not an answer about the value, so let it propagate.
            if (!isNeedleConversionRefusal(e.code()))
                throw;

            return false;
        }
    }

    /** Why the dictionary shortcut may not be taken for a given needle.
      *
      * The two reasons are NOT interchangeable. [Shape] says only "this lookup cannot answer it", so
      * the value must be computed by the general path. [NoElementCanEqual] is a positive result about
      * the VALUE: the needle lies outside the image of the element type, so no element can equal it
      * and the answer is a zero-filled column. Collapsing the second into the first hands the value
      * to `executeIntegral`, whose comparison is by RAW PHYSICAL NUMBER, and a `Date` day number then
      * matches a `DateTime` epoch second that it does not equal.
      */
    enum class DictionaryShortcut : uint8_t
    {
        Admit,
        Shape,
        NoElementCanEqual,
    };

    /** The LowCardinality fast path resolves the needle to ONE dictionary index and then compares
      * indices, so it is sound only when the needle identifies exactly one element value AND the
      * dictionary's lookup, which is by BYTE identity, coincides with that type's equality.
      *
      * The string-family clauses are an allow-list on the TYPES because their equality is
      * width-sensitive in a way a value round trip does not see. Everything else is decided by
      * [castPreservesNeedleValue] on the actual constant. A FLOAT ELEMENT is refused outright: byte
      * identity is not equality there even for a same-type needle, since `+0.0 == -0.0` with
      * different bytes and `NaN != NaN` with equal bytes, so the dictionary both misses rows a `0.0`
      * needle must match and invents a `NaN` match. A float NEEDLE against a non-float element is
      * fine as long as it converts exactly.
      */
    static DictionaryShortcut needleMapsToSingleDictionaryValue(
        const DataTypePtr & element_type,
        const DataTypePtr & needle_type,
        std::optional<size_t> needle_constant_size,
        const ColumnPtr & needle_column)
    {
        const auto element = removeNullable(recursiveRemoveLowCardinality(element_type));
        const auto needle = removeNullable(recursiveRemoveLowCardinality(needle_type));

        const WhichDataType which_element(element);
        const WhichDataType which_needle(needle);

        /// See the float paragraph above: the dictionary is looked up by bytes, so a float ELEMENT
        /// cannot be matched by an equality-correct lookup at all. This says nothing about the value,
        /// so the general path must still compute it.
        if (which_element.isFloat())
            return DictionaryShortcut::Shape;

        /// A `FixedString` needle is zero-padded to the element width, so it stays one value only
        /// while the element type is at least as wide; a `String` element can hold several members
        /// of the needle's equivalence class, of which the dictionary would find at most one.
        if (which_needle.isFixedString())
            return which_element.isFixedString()
                    && assert_cast<const DataTypeFixedString &>(*element).getN()
                        >= assert_cast<const DataTypeFixedString &>(*needle).getN()
                ? DictionaryShortcut::Admit
                : DictionaryShortcut::Shape;

        if (which_element.isFixedString())
        {
            /// A `String` needle is padded up to the element width by the cast the dictionary lookup
            /// performs, which is exact while it fits, so it still denotes one value. A needle LONGER
            /// than the element width does not: the cast would throw, and its equivalence class can
            /// hold values the element type cannot store.
            const size_t element_n = assert_cast<const DataTypeFixedString &>(*element).getN();
            return needle_constant_size.has_value() && *needle_constant_size <= element_n
                ? DictionaryShortcut::Admit
                : DictionaryShortcut::Shape;
        }

        if (element->equals(*needle))
            return DictionaryShortcut::Admit;

        /// Mirror what the lookup does with the operands before casting: `recursiveRemoveLowCardinality`
        /// on the column, then peel `Nullable`, so the column handed to the cast matches [needle].
        ColumnPtr value = recursiveRemoveLowCardinality(needle_column);
        if (const auto * value_nullable = checkAndGetColumn<ColumnNullable>(value.get()))
            value = value_nullable->getNestedColumnPtr();

        /// The round trip failing is a fact about the VALUE, not about the lookup: the needle is not
        /// in the image of the element type, so no element of that type equals it and the answer is
        /// "no match" for every row. Reporting it as a shape decline instead would send the pair to
        /// `executeIntegral`, which compares raw physical numbers and would match a `Date` day number
        /// against an equal `DateTime` epoch second.
        return castPreservesNeedleValue(value, needle, element)
            ? DictionaryShortcut::Admit
            : DictionaryShortcut::NoElementCanEqual;
    }

    static ColumnPtr executeArrayLowCardinality(const ColumnsWithTypeAndName & arguments)
    {
        /// The LowCardinality optimization compares dictionary indices instead of actual values.
        /// This is correct for linear scan (indexOf, has, countEqual) where only equality is checked,
        /// but incorrect for binary search (indexOfAssumeSorted) where ordering matters --
        /// dictionary indices are assigned in insertion order, not in sorted order of values.
        if constexpr (std::is_same_v<ConcreteAction, IndexOfAssumeSorted>)
            return nullptr;

        const auto * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        const auto * col_array_const = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());

        if (!col_array && !col_array_const)
            return nullptr;

        if (col_array_const)
            col_array = col_array_const;

        const auto * left_lc = checkAndGetColumn<ColumnLowCardinality>(&col_array->getData());
        if (!left_lc)
            return nullptr;

        const auto * right_const = checkAndGetColumn<ColumnConst>(arguments[1].column.get());
        if (!right_const)
            return nullptr;

        const auto & array_type  = assert_cast<const DataTypeArray &>(*arguments[0].type);
        const auto target_type = recursiveRemoveLowCardinality(array_type.getNestedType());

        /// `dictionaryIndexForConstant` answers a NULL needle with index 0, which is the NULL slot
        /// only for a nullable dictionary; for a non-nullable one index 0 is the type's DEFAULT
        /// value (`ColumnUnique::getDefaultValueIndex`), so a NULL needle would match every row
        /// holding that default. Let such a needle fall through: `executeNothing` answers it.
        if (right_const->isNullAt(0) && !isNullableOrLowCardinalityNullable(array_type.getNestedType()))
            return nullptr;

        /// The needle's own byte length, which decides whether a `String` needle fits a `FixedString`
        /// element. `getDataAt` returns `sizeAt` for a `ColumnString` (no terminator) and `N` for a
        /// `ColumnFixedString`, so it is the real length either way; it is unavailable for a NULL,
        /// where it throws, and for a column that does not store contiguous bytes.
        /// `useDefaultImplementationForLowCardinalityColumns` is false for this function, so the
        /// needle keeps its own wrappers. Peel them the same way the TYPE side is normalized in
        /// [needleMapsToSingleDictionaryValue], or the very same bytes would be admitted as a
        /// `String` and declined as a `LowCardinality(String)`. The `isNullAt` guard stays FIRST
        /// because `ColumnNullable::getDataAt` throws.
        std::optional<size_t> needle_constant_size;
        if (!right_const->isNullAt(0))
        {
            /// `recursiveRemoveLowCardinality` resolves the single value through the dictionary and
            /// preserves row order, so row 0 stays row 0. Peeling by hand via `getDictionary` would
            /// NOT: dictionary position 0 is the type's default, not this constant's value.
            ColumnPtr needle_holder = recursiveRemoveLowCardinality(right_const->getDataColumnPtr());
            const IColumn * needle_data = needle_holder.get();
            if (const auto * needle_nullable = checkAndGetColumn<ColumnNullable>(needle_data))
                needle_data = &needle_nullable->getNestedColumn();

            if (checkAndGetColumn<ColumnString>(needle_data) || checkAndGetColumn<ColumnFixedString>(needle_data))
                needle_constant_size = needle_data->getDataAt(0).size();
        }

        auto shortcut = needleMapsToSingleDictionaryValue(
            array_type.getNestedType(), arguments[1].type, needle_constant_size, right_const->getDataColumnPtr());

        /// A NULL needle carries no value, so the round trip inside the guard inspects the nested
        /// column's arbitrary placeholder rather than the needle. Only the reached-here case is left
        /// (a NULL needle on a NULLABLE dictionary), where index 0 IS the NULL slot and the lookup is
        /// the right answer, so the verdict must never be read as "no element can equal it".
        if (right_const->isNullAt(0) && shortcut == DictionaryShortcut::NoElementCanEqual)
            shortcut = DictionaryShortcut::Admit;

        if (shortcut == DictionaryShortcut::Shape)
            return nullptr;

        UInt64 index = 0;
        UInt64 left_size = arguments[0].column->size();
        ResultColumnPtr col_result = ResultColumnType::create();

        /// A needle outside the image of the element type equals no element, and the dictionary miss
        /// says the same thing, so both answer every row with the action's "not found" value.
        if (shortcut == DictionaryShortcut::NoElementCanEqual
            || !LowCardinalityExecutionHelpers::dictionaryIndexForConstant(
                *left_lc, right_const->getDataColumnPtr(), arguments[1].type, target_type, index))
        {
            col_result->getData().resize_fill(col_array->size());

            if (col_array_const)
                return ColumnConst::create(std::move(col_result), left_size);

            return col_result;
        }

        Impl::Main<ConcreteAction, true>::vector(
            left_lc->getIndexes(),
            col_array->getOffsets(),
            index, /** Assuming LowCardinality has index of NULL always as zero. */
            col_result->getData(),
            nullptr,
            nullptr);

        if (col_array_const)
            return ColumnConst::create(std::move(col_result), left_size);

        return col_result;
    }

    ColumnPtr executeMap(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        if constexpr (!std::is_same_v<ConcreteAction, HasAction>)
            return nullptr;

        if (!isMap(arguments[0].type))
            return nullptr;

        auto non_const_map_column = arguments[0].column->convertToFullColumnIfConst();

        const auto & map_column = assert_cast<const ColumnMap &>(*non_const_map_column);
        const auto & map_array_column = map_column.getNestedColumn();
        auto offsets = map_array_column.getOffsetsPtr();
        auto keys = map_column.getNestedData().getColumnPtr(0);
        auto array_column = ColumnArray::create(keys, offsets);

        const auto & type_map = assert_cast<const DataTypeMap &>(*arguments[0].type);
        auto array_type = std::make_shared<DataTypeArray>(type_map.getKeyType());

        auto arguments_copy = arguments;
        arguments_copy[0].column = std::move(array_column);
        arguments_copy[0].type = std::move(array_type);
        arguments_copy[0].name = arguments[0].name;

        /// executeImpl strips LowCardinality before executeArrayImpl, but the Map path bypasses it.
        /// Strip here too so executeArrayImpl sees a ColumnNullable lookup column and fills null_map_item,
        /// keeping null-needle semantics identical to the plain array path.
        for (auto & argument : arguments_copy)
        {
            argument.column = recursiveRemoveLowCardinality(argument.column);
            argument.type = recursiveRemoveLowCardinality(argument.type);
        }

        return executeArrayImpl(arguments_copy, result_type);
    }

    /**
     * Helper function to check if a path or its prefix exists in shared data.
     */
     static bool hasPathInSharedData(
        const String & path,
        const String & prefix,
        const ColumnString * shared_paths,
        const ColumnVector<UInt64>::Container & shared_offsets,
        size_t row)
    {
        if (!shared_paths)
            return false;

        size_t start = shared_offsets[static_cast<ssize_t>(row) - 1];
        size_t end = shared_offsets[row];

        if (start == end)
            return false;

        /// Check for exact match
        size_t pos = ColumnObject::findPathLowerBoundInSharedData(path, *shared_paths, start, end);
        if (pos < end && shared_paths->getDataAt(pos) == path)
            return true;

        /// Check for prefix match (path + ".")
        pos = ColumnObject::findPathLowerBoundInSharedData(prefix, *shared_paths, start, end);
        if (pos < end && shared_paths->getDataAt(pos).starts_with(prefix))
            return true;

        return false;
    }

    /**
     * Check if a path exists in JSON object for a specific row.
     * Returns true if the path (or any path with this prefix) exists.
     */
    static bool hasPathInObjectRow(
        const ColumnObject & object_column,
        size_t row,
        const String & path,
        const String & prefix,
        const ColumnString * shared_paths,
        const ColumnVector<UInt64>::Container & shared_offsets)
    {
        /// First, check for the requested path in typed paths.
        /// Typed paths are always considered to be present in each row (even if null).
        const auto & typed_paths = object_column.getTypedPaths();
        if (typed_paths.contains(path))
            return true;

        for (const auto & [key, col] : typed_paths)
        {
            if (key.starts_with(prefix))
                return true;
        }

        /// Second, check for the requested path in dynamic paths.
        /// For dynamic paths, we consider null equivalent to absence of the value.
        const auto & dynamic_paths = object_column.getDynamicPathsPtrs();
        if (auto it = dynamic_paths.find(path); it != dynamic_paths.end() && !it->second->isNullAt(row))
            return true;

        for (const auto & [key, col] : dynamic_paths)
        {
            if (key.starts_with(prefix) && !col->isNullAt(row))
                return true;
        }

        /// Third, check for the requested path in shared data.
        return hasPathInSharedData(path, prefix, shared_paths, shared_offsets, row);
    }

    /**
     * Execute has() function for JSON/Object type.
     * Checks if a path exists in the JSON object.
     *
     * The function checks three storage tiers in order:
     * 1. Typed paths - explicitly declared paths that are always present (even if null)
     * 2. Dynamic paths - paths inferred at runtime, null means absence
     * 3. Shared data - overflow storage for rare paths, uses binary search
     *
     * Optimizations:
     * - For constant path: if found in typed paths, returns 1 for all rows immediately
     * - For constant path: pre-collects relevant dynamic columns to avoid repeated lookups
     * - For non-constant path: uses hasPathInObjectRow() helper with early returns
     *
     * @param arguments - [0] JSON column (or const JSON), [1] path column
     * @return ColumnUInt8 with 1 if path exists, 0 otherwise
    */
    ColumnPtr executeObject(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/) const
    {
        if constexpr (!std::is_same_v<ConcreteAction, HasAction>)
            return nullptr;

        const auto * object_type = checkAndGetDataType<DataTypeObject>(arguments[0].type.get());
        if (!object_type)
            return nullptr;

        auto non_const_object_column = arguments[0].column->convertToFullColumnIfConst();
        const auto & object_column = assert_cast<const ColumnObject &>(*non_const_object_column);
        const auto & path_column = *arguments[1].column;
        const size_t input_rows_count = object_column.size();

        if (input_rows_count == 0)
            return ColumnUInt8::create();

        auto res_col = ColumnUInt8::create(input_rows_count);
        auto & res_data = res_col->getData();

        const auto [shared_paths, shared_values] = object_column.getSharedDataPathsAndValues();
        const auto & shared_offsets = object_column.getSharedDataOffsets();

        if (isColumnConst(path_column))
        {
            const String path(path_column.getDataAt(0));
            const String prefix = path + ".";

            /// Optimization: if path or its prefix exists in typed paths,
            /// we can return 1 for all rows since typed paths are always present.
            const auto & typed_paths = object_column.getTypedPaths();
            bool found_in_typed = typed_paths.contains(path);
            if (!found_in_typed)
            {
                for (const auto & [key, col] : typed_paths)
                {
                    if (key.starts_with(prefix))
                    {
                        found_in_typed = true;
                        break;
                    }
                }
            }

            if (found_in_typed)
            {
                /// Path exists in typed paths - return 1 for all rows
                std::fill(res_data.begin(), res_data.end(), 1);
                return res_col;
            }

            /// Collect columns from dynamic paths that match exact path or prefix.
            /// These columns need to be checked for non-null values per row.
            VectorWithMemoryTracking<const IColumn *> relevant_dynamic_columns;
            const auto & dynamic_paths = object_column.getDynamicPathsPtrs();

            if (auto it = dynamic_paths.find(path); it != dynamic_paths.end())
                relevant_dynamic_columns.push_back(it->second);

            for (const auto & [key, col] : dynamic_paths)
            {
                if (key.starts_with(prefix))
                    relevant_dynamic_columns.push_back(col);
            }

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                bool found = false;

                /// Check dynamic paths - need to verify non-null for each row
                for (const auto * col : relevant_dynamic_columns)
                {
                    if (!col->isNullAt(i))
                    {
                        found = true;
                        break;
                    }
                }

                /// Check shared data if not found in dynamic paths
                if (!found)
                {
                    found = hasPathInSharedData(path, prefix, shared_paths, shared_offsets, i);
                }
                res_data[i] = found;
            }
        }
        else
        {
            /// Non-constant path: check each row individually
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                const String path(path_column.getDataAt(i));
                const String prefix = path + ".";

                res_data[i] = hasPathInObjectRow(object_column, i, path, prefix, shared_paths, shared_offsets);
            }
        }

        return res_col;
    }

    static ColumnPtr executeString(const ColumnsWithTypeAndName & arguments)
    {
        const auto * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!array)
            return nullptr;

        const auto * left = checkAndGetColumn<ColumnString>(&array->getData());
        if (!left)
            return nullptr;

        const auto & right = *arguments[1].column;
        const auto [null_map_data, null_map_item] = getNullMaps(arguments);

        auto result = ResultColumnType::create();

        if (const auto * item_arg_const = checkAndGetColumnConstStringOrFixedString(&right))
        {
            const auto * item_const_string = checkAndGetColumn<ColumnString>(&item_arg_const->getDataColumn());
            const auto * item_const_fixedstring = checkAndGetColumn<ColumnFixedString>(&item_arg_const->getDataColumn());

            if (item_const_string)
                Impl::String<ConcreteAction>::process(
                    left->getChars(),
                    array->getOffsets(),
                    left->getOffsets(),
                    item_const_string->getChars(),
                    item_const_string->getDataAt(0).size(),
                    result->getData(),
                    null_map_data,
                    null_map_item);
            else if (item_const_fixedstring)
                Impl::String<ConcreteAction>::process(
                    left->getChars(),
                    array->getOffsets(),
                    left->getOffsets(),
                    item_const_fixedstring->getChars(),
                    item_const_fixedstring->getN(),
                    result->getData(),
                    null_map_data,
                    null_map_item,
                    /*zero_padded=*/ true);
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnConst contains not String nor FixedString column");
        }
        else if (const auto * item_arg_vector = checkAndGetColumn<ColumnString>(&right))
        {
            Impl::String<ConcreteAction>::process(
                left->getChars(),
                array->getOffsets(),
                left->getOffsets(),
                item_arg_vector->getChars(),
                item_arg_vector->getOffsets(),
                result->getData(),
                null_map_data,
                null_map_item);
        }
        else
        {
            return nullptr;
        }

        return result;
    }

    /** String-family equality is zero-padded exactly when one side is a `FixedString`
      * (`toFixedString('abc', 5) = 'abc'`), while `String` vs `String` stays exact
      * (`'V0' = 'V0\0'` is false).
      */
    static bool needsZeroPaddedComparison(const DataTypePtr & element_type, const DataTypePtr & needle_type)
    {
        const auto element = removeNullable(recursiveRemoveLowCardinality(element_type));
        const auto needle = removeNullable(recursiveRemoveLowCardinality(needle_type));

        const WhichDataType which_element(element);
        const WhichDataType which_needle(needle);

        return (which_element.isFixedString() && which_needle.isStringOrFixedString())
            || (which_needle.isFixedString() && which_element.isStringOrFixedString());
    }

    /// Compares two string `Field`s as if the shorter one were padded with zero bytes.
    /// Unlike the `AllowOverflow15` primitives, this may not over-read: a `Field` holds a
    /// `std::string`, which carries no SIMD padding.
    static bool zeroPaddedFieldEquals(const Field & lhs, const Field & rhs)
    {
        if (lhs.getType() != Field::Types::String || rhs.getType() != Field::Types::String)
            return accurateEquals(lhs, rhs);

        const auto & a = lhs.safeGet<String>();
        const auto & b = rhs.safeGet<String>();

        const size_t min_size = std::min(a.size(), b.size());
        if (0 != memcmp(a.data(), b.data(), min_size))
            return false;

        const auto & longest = a.size() > b.size() ? a : b;
        for (size_t i = min_size; i < longest.size(); ++i)
            if (longest[i] != 0)
                return false;

        return true;
    }

    static ResultType linearSearchConstZeroPadded(const Array & arr, const Field & value)
    {
        ResultType current = 0;
        for (size_t i = 0, size = arr.size(); i < size; ++i)
        {
            if (!zeroPaddedFieldEquals(arr[i], value))
                continue;

            ConcreteAction::apply(current, i);

            if constexpr (!ConcreteAction::resume_execution)
                break;
        }
        return current;
    }

    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());

        if (!col_array)
            return nullptr;

        Array arr = col_array->getValue<Array>();
        const IColumn * item_arg = arguments[1].column.get();

        /// `Field` has no `FixedString`, so a `FixedString` value arrives as a NUL-padded `String`
        /// and exact `Field` equality compares that padding as data.
        const bool zero_padded = needsZeroPaddedComparison(
            assert_cast<const DataTypeArray &>(*arguments[0].type).getNestedType(), arguments[1].type);

        if (isColumnConst(*item_arg))
        {
            ResultType current = 0;
            const auto & value = (*item_arg)[0];
            /// Ordering under zero-padded comparison need not match the dictionary/insertion order
            /// the binary search relies on, so use the linear scan, which is correct either way.
            if (zero_padded)
                current = linearSearchConstZeroPadded(arr, value);
            else if constexpr (std::is_same_v<ConcreteAction, IndexOfAssumeSorted>)
            {
                if (isColumnNullableOrLowCardinalityNullable(
                        assert_cast<const ColumnArray &>(col_array->getDataColumn()).getData()))
                    current = Impl::Main<ConcreteAction, true>::linearSearchConst(arr, value);
                else
                    current = Impl::Main<ConcreteAction, true>::lowerBound(arr, value, arr.size(), 0);
            }
            else
            {
                current = Impl::Main<ConcreteAction, true>::linearSearchConst(arr, value);
            }

            return result_type->createColumnConst(item_arg->size(), current);
        }

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
                else
                {
                    if (null_map && (*null_map)[row])
                        continue;
                    if (zero_padded ? !zeroPaddedFieldEquals(arr[i], value) : !accurateEquals(arr[i], value))
                        continue;
                }

                ConcreteAction::apply(data[row], i);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }
        }

        return col_res;
    }

    static ColumnPtr executeNothing(const ColumnsWithTypeAndName & arguments)
    {
        const auto * array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!array)
            return nullptr;

        if (arguments[1].column->onlyNull())
        {
            auto result = ResultColumnType::create();
            Impl::Null<ConcreteAction>::process(array->getOffsets(), result->getData(), getNullMaps(arguments).first);
            return result;
        }

        return nullptr;
    }

    static ColumnPtr executeGeneric(const ColumnsWithTypeAndName & arguments)
    {
        const auto * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            return nullptr;

        DataTypePtr array_elements_type = assert_cast<const DataTypeArray &>(*arguments[0].type).getNestedType();
        const DataTypePtr & index_type = arguments[1].type;

        DataTypePtr common_type = getLeastSupertype(DataTypes{array_elements_type, arguments[1].type});
        ColumnPtr col_nested = castColumn({ col_array->getDataPtr(), array_elements_type, "" }, common_type);
        ColumnPtr item_arg = castColumn({ arguments[1].column, removeLowCardinality(index_type), "" }, common_type);

        auto col_res = ResultColumnType::create();

        auto [null_map_data, null_map_item] = getNullMaps(arguments);

        if (const auto * item_arg_const = checkAndGetColumn<ColumnConst>(item_arg.get()))
        {
            Impl::Main<ConcreteAction, true>::vector(
                *col_nested,
                col_array->getOffsets(),
                item_arg_const->getDataColumn(),
                col_res->getData(), /// TODO This is wrong.
                null_map_data,
                nullptr);
        }
        else
        {
            Impl::Main<ConcreteAction>::vector(
                *col_nested,
                col_array->getOffsets(),
                *item_arg,
                col_res->getData(),
                null_map_data,
                null_map_item);
        }

        return col_res;
    }
};
}
