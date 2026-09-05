#pragma once

#include <cstddef>
#include <limits>
#include <optional>
#include <type_traits>

#include <Core/Defines.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/LowCardinalityExecutionHelpers.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsCommon.h>
#include <Common/FieldAccurateComparison.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/checkStackSize.h>
#include <base/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnDynamic.h>
#include <DataTypes/DataTypeObject.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool type_json_skip_null_typed_paths;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NO_COMMON_TYPE;
    extern const int NOT_IMPLEMENTED;
}

using NullMap = PaddedPODArray<UInt8>;

/// True when `type` is Dynamic or Variant, at the top level or inside any chain of Tuple wrappers,
/// each of which may be Nullable. Stops at Array/Map on purpose: equality for identical Array/Map
/// types is itself compareAt-based, so descending there could not change the answer. forEachChild
/// cannot express that barrier.
inline bool hasTypeErasingElement(const IDataType & type)
{
    checkStackSize();

    if (isDynamic(type) || isVariant(type))
        return true;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(type.getPtr()).get()))
    {
        for (const auto & element : tuple_type->getElements())
            if (hasTypeErasingElement(*element))
                return true;
    }

    return false;
}

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
    static void invokeCheckNullMaps(
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
    static void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        Offset item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map)
    {
        invokeCheckNullMaps<true>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }

    static void process(
        const ColumnString::Chars & data, const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets, const ColumnString::Chars & item_values,
        const ColumnString::Offsets & item_offsets, PaddedPODArray<ResultType> & result,
        const NullMap * data_map, const NullMap * item_map)
    {
        invokeCheckNullMaps<false>(data, offsets, string_offsets, item_values, item_offsets, result, data_map, item_map);
    }
};
}

template <typename ConcreteAction, typename Name>
class FunctionArrayIndex final : public IFunction
{
public:
    static constexpr auto name = Name::name;
    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionArrayIndex>(context); }

    FunctionArrayIndex() = default;

    /// A null context is benign: `equals` then resolves with default `ComparisonParams`, so the
    /// relation stays correct and only query settings go unapplied.
    explicit FunctionArrayIndex(ContextPtr context)
        : equals_resolver(FunctionFactory::instance().get("equals", context))
        , skip_null_typed_paths(context && context->getSettingsRef()[Setting::type_json_skip_null_typed_paths])
    {
    }

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

        return executeArray(arguments, result_type);
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

    /// Sole owner of the array-shaped dispatch, so both entry points (executeImpl and executeMap)
    /// see the same arms in the same order. The Map path normalises cardinality before calling this,
    /// because a Map key cannot be nullable and so cannot use the arm below that assumes it can.
    ColumnPtr executeArray(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        if (auto res = executeErasedEquality(arguments, result_type))
            return res;

        return executeArrayAfterErasedEquality(arguments, result_type);
    }

    /// The arms that answer whenever the erased-equality one declines. Kept separate so that path can
    /// ask them for the rows it leaves undecided without re-entering itself.
    ColumnPtr executeArrayAfterErasedEquality(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
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

    /// The common type of the array element and the needle, with the wrappers that do not affect
    /// which equality relation applies removed.
    static DataTypePtr tryGetDecayedCommonType(const ColumnsWithTypeAndName & arguments)
    {
        const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get());
        if (!array_type)
            return nullptr;

        auto element_type = removeNullable(recursiveRemoveLowCardinality(array_type->getNestedType()));
        auto needle_type = removeNullable(recursiveRemoveLowCardinality(arguments[1].type));

        return tryGetLeastSupertype(DataTypes{element_type, needle_type});
    }

    /// Fold a per-element `equals` result plus both operands' nullness into one match bitmap.
    /// Two NULLs count as equal, per the documented has([NULL], NULL) -> 1 contract; an
    /// indeterminate (NULL) comparison counts as no match.
    static ColumnUInt8::MutablePtr foldEqualityResult(
        const ColumnPtr & equality_result, const IColumn & elements, const IColumn & needles, size_t size)
    {
        auto matches = ColumnUInt8::create(size);
        auto & matches_data = matches->getData();

        auto flat_result = equality_result->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
        const auto * nullable_result = checkAndGetColumn<ColumnNullable>(flat_result.get());
        const IColumn & result_values = nullable_result ? nullable_result->getNestedColumn() : *flat_result;

        /// `equals` yields Nullable(Nothing) when no comparison is possible at all (an erased operand
        /// with no compatible alternative and the matching *_throw_on_type_mismatch setting disabled).
        const bool result_is_nothing = result_values.getDataType() == TypeIndex::Nothing;

        for (size_t i = 0; i < size; ++i)
        {
            const bool element_is_null = elements.isNullAt(i);
            const bool needle_is_null = needles.isNullAt(i);

            if (element_is_null || needle_is_null)
                matches_data[i] = element_is_null && needle_is_null;
            else if (result_is_nothing || (nullable_result && nullable_result->isNullAt(i)))
                matches_data[i] = 0;
            else
                matches_data[i] = result_values.getBool(i) ? 1 : 0;
        }

        return matches;
    }

    /// Strip a Nullable wrapper, handing back the null map column so the caller can fold the outer
    /// nullness itself: a wrapper's isNullAt answers about the wrapper, not about a nested NULL.
    /// The caller must keep the returned null map column alive while reading its data.
    static ColumnWithTypeAndName unwrapNullable(const ColumnWithTypeAndName & operand, ColumnPtr & null_map_column)
    {
        auto full_column = operand.column->convertToFullColumnIfConst();
        const auto * nullable_column = checkAndGetColumn<ColumnNullable>(full_column.get());
        if (!nullable_column)
            return operand;

        null_map_column = nullable_column->getNullMapColumnPtr();
        return {nullable_column->getNestedColumnPtr(), removeNullable(operand.type), operand.name};
    }

    /// A column positionally aligned with the original rows, plus a null map marking the rows holding
    /// no value. A null `operand.column` means every row is NULL.
    struct PeeledOperand
    {
        ColumnWithTypeAndName operand;
        ColumnPtr null_map_column;
    };

    /// Replace a Dynamic/Variant operand by the one concrete alternative it holds, or return nothing
    /// so the caller declines. Mirrors the two fast paths of FunctionVariantAdaptor.
    static std::optional<PeeledOperand> tryPeelErased(const ColumnWithTypeAndName & operand)
    {
        if (!isDynamic(operand.type) && !isVariant(operand.type))
            return {};

        auto full_column = operand.column->convertToFullColumnIfConst();

        const ColumnVariant * variant_column = nullptr;
        DataTypes alternative_types;
        std::optional<ColumnVariant::Discriminator> shared_variant_discriminator;

        if (const auto * dynamic_column = checkAndGetColumn<ColumnDynamic>(full_column.get()))
        {
            variant_column = &dynamic_column->getVariantColumn();
            alternative_types = assert_cast<const DataTypeVariant &>(*dynamic_column->getVariantInfo().variant_type).getVariants();
            shared_variant_discriminator = dynamic_column->getSharedVariantDiscriminator();
        }
        else if (const auto * as_variant = checkAndGetColumn<ColumnVariant>(full_column.get()))
        {
            variant_column = as_variant;
            alternative_types = assert_cast<const DataTypeVariant &>(*operand.type).getVariants();
        }

        if (!variant_column)
            return {};

        /// Every row is NULL: there is no alternative to peel, so the answer comes from the null maps
        /// alone. Comparing anything here would fail, because a Nothing-typed argument is rejected
        /// unless the result is Nothing too.
        if (variant_column->hasOnlyNulls())
            return PeeledOperand{{nullptr, operand.type, operand.name}, ColumnUInt8::create(variant_column->size(), UInt8(1))};

        auto discriminator = variant_column->getGlobalDiscriminatorOfOneNoneEmptyVariant();
        if (!discriminator || discriminator == shared_variant_discriminator)
            return {};

        const auto & alternative_type = alternative_types[*discriminator];

        /// Equality for identical Array/Map types is compareAt-based, the very relation the guard
        /// stops at, so peeling into one would reintroduce it behind the guard's back.
        if (hasContainer(*alternative_type))
            return {};

        auto alternative_column = variant_column->getVariantPtrByGlobalDiscriminator(*discriminator);

        if (alternative_column->size() == variant_column->size())
            return PeeledOperand{{alternative_column, alternative_type, operand.name}, nullptr};

        /// The alternative was reported non-empty above, so index 0 below is always in range.
        if (alternative_column->empty())
            return {};

        /// The alternative holds only the rows that selected it, each at its own offset. Indexing by
        /// that offset rather than expanding by a mask avoids assuming the offsets ascend with the
        /// superproject row order, which ColumnVariant::validateState does not check.
        const size_t rows = variant_column->size();
        const auto local_discriminator = variant_column->localDiscriminatorByGlobal(*discriminator);
        const auto & local_discriminators = variant_column->getLocalDiscriminators();
        const auto & alternative_offsets = variant_column->getOffsets();

        auto selector = ColumnUInt64::create(rows);
        auto & selector_data = selector->getData();
        auto null_map = ColumnUInt8::create(rows);
        auto & null_map_data = null_map->getData();

        for (size_t row = 0; row < rows; ++row)
        {
            const bool selected = local_discriminators[row] == local_discriminator;
            null_map_data[row] = !selected;
            /// An unselected row reads no value, so any in-range index does; 0 keeps index() in bounds.
            selector_data[row] = selected ? alternative_offsets[row] : 0;
        }

        auto expanded = alternative_column->index(*selector, rows);

        return PeeledOperand{{std::move(expanded), alternative_type, operand.name}, std::move(null_map)};
    }

    /// The erased column reached by descending `elements` through the Tuple wrappers named by
    /// `path`, or nothing when the column shape does not match the type. Used to read a row's
    /// discriminators at the site the peel will act on. `holder` keeps the returned column alive: a
    /// level of the descent may materialise a temporary, which the caller must outlive.
    static const ColumnVariant * findVariantAtPath(
        const IColumn & elements, const VectorWithMemoryTracking<size_t> & path, ColumnPtr & holder)
    {
        const IColumn * current = &elements;
        auto held = current->getPtr();

        for (size_t position : path)
        {
            held = current->convertToFullColumnIfConst();
            const auto * as_nullable = checkAndGetColumn<ColumnNullable>(held.get());
            if (as_nullable)
                held = as_nullable->getNestedColumnPtr();

            const auto * as_tuple = checkAndGetColumn<ColumnTuple>(held.get());
            if (!as_tuple || position >= as_tuple->tupleSize())
                return nullptr;

            held = as_tuple->getColumnPtr(position);
            current = held.get();
        }

        held = current->convertToFullColumnIfConst();
        if (const auto * as_nullable = checkAndGetColumn<ColumnNullable>(held.get()))
            held = as_nullable->getNestedColumnPtr();

        if (const auto * as_dynamic = checkAndGetColumn<ColumnDynamic>(held.get()))
        {
            /// The pointee lives inside the dynamic column, so that is what has to stay alive.
            holder = held;
            return &as_dynamic->getVariantColumn();
        }

        const auto * as_variant = checkAndGetColumn<ColumnVariant>(held.get());
        if (as_variant)
            holder = held;

        return as_variant;
    }

    /// The path of Tuple positions leading to the first erased type inside `type`, or nothing when
    /// `type` erases nothing. Mirrors hasTypeErasingElement's traversal, including its Array/Map stop.
    static std::optional<VectorWithMemoryTracking<size_t>> findErasedPath(const IDataType & type)
    {
        checkStackSize();

        if (isDynamic(type) || isVariant(type))
            return VectorWithMemoryTracking<size_t>{};

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(removeNullable(type.getPtr()).get());
        if (!tuple_type)
            return {};

        const auto & elements = tuple_type->getElements();
        for (size_t position = 0; position < elements.size(); ++position)
        {
            if (auto nested = findErasedPath(*elements[position]))
            {
                VectorWithMemoryTracking<size_t> path{position};
                path.insert(path.end(), nested->begin(), nested->end());
                return path;
            }
        }

        return {};
    }

    /// A row's alternatives on both sides of the comparison, packed so rows agreeing on both share a
    /// group. Both halves are ColumnVariant::Discriminator, so the pair fits a UInt16; the wider type
    /// leaves a sentinel for a row that belongs to no group.
    using GroupKey = UInt32;
    static constexpr GroupKey no_group = std::numeric_limits<GroupKey>::max();

    static GroupKey makeGroupKey(ColumnVariant::Discriminator elements, ColumnVariant::Discriminator needle)
    {
        return static_cast<GroupKey>(elements) << 8 | needle;
    }

    /// The one global discriminator every element of `row` carries, NULL_DISCRIMINATOR when it holds
    /// only NULLs, or nothing when its elements disagree. NULLs are skipped: the peel answers those
    /// from the null maps.
    static std::optional<ColumnVariant::Discriminator> rowAlternative(
        const ColumnVariant & variant_column, size_t begin, size_t end)
    {
        const auto & local_discriminators = variant_column.getLocalDiscriminators();
        std::optional<ColumnVariant::Discriminator> alternative;

        for (size_t position = begin; position < end; ++position)
        {
            auto local = local_discriminators[position];
            if (local == ColumnVariant::NULL_DISCRIMINATOR)
                continue;

            auto global = variant_column.globalDiscriminatorByLocal(local);
            if (!alternative)
                alternative = global;
            else if (*alternative != global)
                return {};
        }

        return alternative.value_or(ColumnVariant::NULL_DISCRIMINATOR);
    }

    /// One group label per row, keyed by the alternatives it carries on both sides: a group is only
    /// answerable as a whole if every member peels the same way. A row whose own elements disagree is
    /// labelled no_group, and nothing means no group formed at all. A side that erases nothing gives
    /// every row the same key half, so grouping keys off whichever side is erased.
    static std::optional<VectorWithMemoryTracking<GroupKey>> groupRowsByAlternative(
        const ColumnArray & array, const DataTypePtr & element_type,
        const IColumn & needle, const DataTypePtr & needle_type)
    {
        const auto & offsets = array.getOffsets();

        const ColumnVariant * variant_column = nullptr;
        ColumnPtr variant_holder;
        if (auto path = findErasedPath(*element_type))
        {
            variant_column = findVariantAtPath(array.getData(), *path, variant_holder);
            if (!variant_column || variant_column->size() != array.getData().size())
                return {};
        }

        const ColumnVariant * needle_variant = nullptr;
        ColumnPtr needle_holder;
        if (auto needle_path = findErasedPath(*needle_type))
        {
            needle_variant = findVariantAtPath(needle, *needle_path, needle_holder);
            if (!needle_variant || needle_variant->size() != offsets.size())
                return {};
        }

        VectorWithMemoryTracking<GroupKey> labels(offsets.size(), no_group);
        bool any_grouped = false;
        ColumnArray::Offset current_offset = 0;

        for (size_t row = 0; row < offsets.size(); ++row)
        {
            auto elements_alternative = variant_column
                ? rowAlternative(*variant_column, current_offset, offsets[row])
                : std::optional<ColumnVariant::Discriminator>{ColumnVariant::NULL_DISCRIMINATOR};
            current_offset = offsets[row];

            if (!elements_alternative)
                continue;

            auto needle_alternative = needle_variant
                ? rowAlternative(*needle_variant, row, row + 1)
                : std::optional<ColumnVariant::Discriminator>{ColumnVariant::NULL_DISCRIMINATOR};

            if (!needle_alternative)
                continue;

            labels[row] = makeGroupKey(*elements_alternative, *needle_alternative);
            any_grouped = true;
        }

        if (!any_grouped)
            return {};

        return labels;
    }

    /// True when `type` is an Array or Map, at the top level or inside any chain of Tuple wrappers.
    static bool hasContainer(const IDataType & type)
    {
        checkStackSize();

        if (isArray(type) || isMap(type))
            return true;

        if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(&type))
        {
            for (const auto & element : tuple_type->getElements())
                if (hasContainer(*element))
                    return true;
        }

        return false;
    }

    /// Split a Tuple operand into its element columns, or return nothing when it is not a plain
    /// Tuple. A Nullable wrapper is removed by unwrapNullable before this is reached.
    static std::optional<ColumnsWithTypeAndName> tryDecomposeTuple(const ColumnWithTypeAndName & operand)
    {
        auto full_column = operand.column->convertToFullColumnIfConst();
        const auto * tuple_column = checkAndGetColumn<ColumnTuple>(full_column.get());
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(operand.type.get());

        if (!tuple_column || !tuple_type || tuple_column->tupleSize() != tuple_type->getElements().size())
            return {};

        ColumnsWithTypeAndName elements;
        elements.reserve(tuple_column->tupleSize());
        for (size_t i = 0, size = tuple_column->tupleSize(); i < size; ++i)
            elements.emplace_back(tuple_column->getColumnPtr(i), tuple_type->getElement(i), operand.name);

        return elements;
    }

    /// Apply one wrapper level's nullness to an already-computed match bitmap: two NULLs match, a
    /// lone NULL does not. Each level folds its own map, so an outer NULL and a nested one stay
    /// distinguishable.
    static void foldNullMaps(
        PaddedPODArray<UInt8> & matches_data,
        const ColumnWithTypeAndName & elements,
        const ColumnPtr & elements_null_map_column,
        const ColumnWithTypeAndName & needles,
        const ColumnPtr & needles_null_map_column)
    {
        if (!elements_null_map_column && !needles_null_map_column)
            return;

        /// A column may report its own nullness too, so both sources are consulted: pairing a NULL a
        /// wrapper knows about with one only the column knows about must still count as two NULLs.
        for (size_t row = 0; row < matches_data.size(); ++row)
        {
            const bool element_is_null = isNullAtRow(elements.column, elements_null_map_column, row);
            const bool needle_is_null = isNullAtRow(needles.column, needles_null_map_column, row);

            if (element_is_null || needle_is_null)
                matches_data[row] = element_is_null && needle_is_null;
        }
    }

    /// Fold `equals` over `elements` against `needles` into a match bitmap. Nothing means the pair
    /// cannot be compared, so the caller must abandon the call rather than combine a partial result.
    /// Tuples are decomposed here because ColumnTuple has no isNullAt to report a NULL nested in one.
    std::optional<ColumnUInt8::MutablePtr> evaluateElementwiseEquality(
        const ColumnWithTypeAndName & elements, const ColumnWithTypeAndName & needles) const
    {
        checkStackSize();

        const size_t rows = elements.column->size();

        /// Hold the null map columns for as long as their data is read below.
        ColumnPtr elements_null_map_column;
        ColumnPtr needles_null_map_column;
        auto bare_elements = unwrapNullable(elements, elements_null_map_column);
        auto bare_needles = unwrapNullable(needles, needles_null_map_column);

        /// Peel before decomposing, so an erased operand holding a Tuple reaches the branch below.
        if (auto peeled_elements = tryPeelErased(bare_elements))
        {
            bare_elements = peeled_elements->operand;
            foldNullMapInto(elements_null_map_column, peeled_elements->null_map_column);
        }
        else if (isDynamic(bare_elements.type) || isVariant(bare_elements.type))
            return {};

        if (auto peeled_needles = tryPeelErased(bare_needles))
        {
            bare_needles = peeled_needles->operand;
            foldNullMapInto(needles_null_map_column, peeled_needles->null_map_column);
        }
        else if (isDynamic(bare_needles.type) || isVariant(bare_needles.type))
            return {};

        /// A Nothing-typed operand carries no value either, and comparing one is rejected outright
        /// unless the result is Nothing too, so mark it the same way a fully-NULL erased column is.
        markAsAllNull(bare_elements, elements_null_map_column, rows);
        markAsAllNull(bare_needles, needles_null_map_column, rows);

        /// One side holds no value in any row, so nullness alone decides every row. The surviving
        /// side may still report its own nullness positionally, as a LowCardinality dictionary does,
        /// so it is asked here the same way the leaf fold asks it.
        if (!bare_elements.column || !bare_needles.column)
        {
            auto matches = ColumnUInt8::create(rows);
            auto & matches_data = matches->getData();
            for (size_t row = 0; row < rows; ++row)
                matches_data[row] = isNullAtRow(bare_elements.column, elements_null_map_column, row)
                    && isNullAtRow(bare_needles.column, needles_null_map_column, row);
            return matches;
        }

        if (auto element_parts = tryDecomposeTuple(bare_elements))
        {
            if (auto needle_parts = tryDecomposeTuple(bare_needles); needle_parts && element_parts->size() == needle_parts->size()
                    && !element_parts->empty())
            {
                /// A tuple matches iff every position does.
                auto matches = evaluateElementwiseEquality((*element_parts)[0], (*needle_parts)[0]);
                if (!matches)
                    return {};

                auto & matches_data = (*matches)->getData();

                for (size_t i = 1, size = element_parts->size(); i < size; ++i)
                {
                    auto part_matches = evaluateElementwiseEquality((*element_parts)[i], (*needle_parts)[i]);
                    if (!part_matches)
                        return {};

                    const auto & part_matches_data = (*part_matches)->getData();
                    for (size_t row = 0; row < matches_data.size(); ++row)
                        matches_data[row] &= part_matches_data[row];
                }

                foldNullMaps(
                    matches_data, bare_elements, elements_null_map_column, bare_needles, needles_null_map_column);
                return matches;
            }
        }

        ColumnsWithTypeAndName equals_arguments{bare_elements, bare_needles};
        ColumnPtr equality_result;
        try
        {
            auto equals_function = equals_resolver->build(equals_arguments);
            equality_result
                = equals_function->execute(equals_arguments, equals_function->getResultType(), rows, /* dry_run = */ false);
        }
        catch (const Exception & e)
        {
            /// The peeled types are not comparable, which the comparison subsystem alone can decide:
            /// its String arms are admitted by type and then converted per value, and a supertype it
            /// does accept may still have no conversion. Decline, so the existing dispatch answers
            /// exactly as it does today.
            if (e.code() != ErrorCodes::NO_COMMON_TYPE && e.code() != ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
                && e.code() != ErrorCodes::NOT_IMPLEMENTED)
                throw;
            return {};
        }

        auto matches = foldEqualityResult(equality_result, *bare_elements.column, *bare_needles.column, rows);
        foldNullMaps(matches->getData(), bare_elements, elements_null_map_column, bare_needles, needles_null_map_column);
        return matches;
    }

    /// Whether one row of an operand holds no value: either a wrapper level said so, or the column
    /// itself reports it. A missing column means the operand holds no value in any row.
    static bool isNullAtRow(const ColumnPtr & column, const ColumnPtr & null_map_column, size_t row)
    {
        if (null_map_column && assert_cast<const ColumnUInt8 &>(*null_map_column).getData()[row])
            return true;

        return !column || column->isNullAt(row);
    }

    /// Drop a Nothing-typed operand's column and mark every row NULL instead, so the fold answers
    /// from the null maps rather than attempting a comparison there.
    static void markAsAllNull(ColumnWithTypeAndName & operand, ColumnPtr & null_map_column, size_t rows)
    {
        if (!operand.column || !isNothing(operand.type))
            return;

        operand.column = nullptr;
        foldNullMapInto(null_map_column, ColumnUInt8::create(rows, UInt8(1)));
    }

    /// Merge a newly-peeled level's null map into the one already collected for this operand.
    /// Both mark absent rows of the same positional space, so a row is NULL if either says so.
    static void foldNullMapInto(ColumnPtr & accumulated, const ColumnPtr & addition)
    {
        if (!addition)
            return;

        if (!accumulated)
        {
            accumulated = addition;
            return;
        }

        auto merged = ColumnUInt8::create(assert_cast<const ColumnUInt8 &>(*accumulated).getData().begin(),
                                         assert_cast<const ColumnUInt8 &>(*accumulated).getData().end());
        auto & merged_data = merged->getData();
        const auto & addition_data = assert_cast<const ColumnUInt8 &>(*addition).getData();
        for (size_t row = 0; row < merged_data.size(); ++row)
            merged_data[row] |= addition_data[row];

        accumulated = std::move(merged);
    }

    /// Reduce a per-element match bitmap to one result per array row.
    ResultColumnPtr foldMatchesPerRow(const PaddedPODArray<UInt8> & matches, const ColumnArray::Offsets & offsets) const
    {
        auto col_result = ResultColumnType::create(offsets.size());
        auto & result_data = col_result->getData();

        ColumnArray::Offset current_offset = 0;
        for (size_t row = 0; row < offsets.size(); ++row)
        {
            ResultType current = 0;
            for (size_t j = 0, array_size = offsets[row] - current_offset; j < array_size; ++j)
            {
                if (!matches[current_offset + j])
                    continue;

                ConcreteAction::apply(current, j);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }
            result_data[row] = current;
            current_offset = offsets[row];
        }

        return col_result;
    }

    /// Membership over a type-erasing common type, using the registered `equals` rather than
    /// compareAt, which for such a column orders by variant name before value.
    /// Returns nullptr for every other common type, leaving the existing dispatch untouched.
    ColumnPtr executeErasedEquality(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
    {
        if (!equals_resolver)
            return nullptr;

        auto common_type = tryGetDecayedCommonType(arguments);
        if (!common_type || !hasTypeErasingElement(*common_type))
            return nullptr;

        const auto & array_type = assert_cast<const DataTypeArray &>(*arguments[0].type);
        auto element_type = array_type.getNestedType();
        const auto & needle_type = arguments[1].type;

        /// LowCardinality elements keep their dictionary fast path, whose null-needle answer depends on
        /// a dictionary index this comparison has no equivalent for.
        if (element_type->lowCardinality())
            return nullptr;

        const auto * col_array_const = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());
        const auto * col_array = col_array_const ? col_array_const : checkAndGetColumn<ColumnArray>(arguments[0].column.get());
        if (!col_array)
            return nullptr;

        const auto * needle_const = checkAndGetColumn<ColumnConst>(arguments[1].column.get());

        /// Const array and const needle: one evaluation over the single logical array, so the cost
        /// stays proportional to the array length instead of to the row count.
        if (col_array_const && needle_const)
        {
            const auto & elements = col_array->getDataPtr();
            size_t elements_count = elements->size();

            auto matches = evaluateElementwiseEquality(
                {elements, element_type, "elements"},
                {needle_const->getDataColumnPtr()->cloneResized(1)->replicate({elements_count}), needle_type, "needle"});
            if (!matches)
                return nullptr;

            ResultType current = 0;
            for (size_t i = 0; i < elements_count; ++i)
            {
                if (!(*matches)->getData()[i])
                    continue;

                ConcreteAction::apply(current, i);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
            }

            return result_type->createColumnConst(arguments[0].column->size(), current);
        }

        if (col_array_const)
            if (auto res = executeErasedEqualityConstArray(arguments, result_type, *col_array, element_type, needle_type))
                return res;

        auto full_array = arguments[0].column->convertToFullColumnIfConst();
        const auto & array = assert_cast<const ColumnArray &>(*full_array);
        auto full_needle = arguments[1].column->convertToFullColumnIfConst();

        if (auto matches = evaluateElementwiseEquality(
                {array.getDataPtr(), element_type, "elements"},
                {full_needle->replicate(array.getOffsets()), needle_type, "needle"}))
            return foldMatchesPerRow((*matches)->getData(), array.getOffsets());

        return executeErasedEqualityPerRowGroup(arguments, result_type, array, full_needle, element_type, needle_type);
    }

    /// Pairs the needles against a constant array in batches, so the temporary stays proportional to a
    /// batch rather than to the row count. A batch whose needles cannot be compared as a whole is
    /// grouped on its own, keeping that bound for the declining case too.
    ColumnPtr executeErasedEqualityConstArray(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const ColumnArray & col_array,
        const DataTypePtr & element_type,
        const DataTypePtr & needle_type) const
    {
        const auto & elements = col_array.getDataPtr();
        const size_t elements_count = elements->size();
        const size_t rows = arguments[0].column->size();

        if (elements_count == 0 || rows == 0)
            return nullptr;

        const size_t rows_per_batch = std::max<size_t>(1, DEFAULT_BLOCK_SIZE / elements_count);
        if (rows_per_batch >= rows)
            return nullptr;

        auto full_needle = arguments[1].column->convertToFullColumnIfConst();

        auto col_result = ResultColumnType::create(rows);
        auto & result_data = col_result->getData();

        ColumnArray::Offsets batch_offsets;
        batch_offsets.reserve(rows_per_batch);

        for (size_t first_row = 0; first_row < rows; first_row += rows_per_batch)
        {
            const size_t batch_rows = std::min(rows_per_batch, rows - first_row);

            /// One array's worth of elements per row, so these offsets both replicate the needles and
            /// describe the row boundaries the matches are folded over.
            batch_offsets.resize(batch_rows);
            for (size_t row = 0; row < batch_rows; ++row)
                batch_offsets[row] = (row + 1) * elements_count;

            auto batch_elements_mutable = elements->cloneEmpty();
            batch_elements_mutable->reserve(elements_count * batch_rows);
            for (size_t row = 0; row < batch_rows; ++row)
                batch_elements_mutable->insertRangeFrom(*elements, 0, elements_count);
            ColumnPtr batch_elements = std::move(batch_elements_mutable);

            auto batch_needle = full_needle->cut(first_row, batch_rows);

            ColumnPtr batch_result;
            if (auto matches = evaluateElementwiseEquality(
                    {batch_elements, element_type, "elements"},
                    {batch_needle->replicate(batch_offsets), needle_type, "needle"}))
            {
                batch_result = foldMatchesPerRow((*matches)->getData(), batch_offsets);
            }
            else
            {
                auto offsets_column = ColumnArray::ColumnOffsets::create(batch_rows);
                offsets_column->getData().assign(batch_offsets);
                auto batch_array = ColumnArray::create(batch_elements, std::move(offsets_column));

                /// The grouping needs the array flattened and the arm below it needs it constant.
                ColumnsWithTypeAndName batch_arguments = arguments;
                batch_arguments[0].column = arguments[0].column->cloneResized(batch_rows);
                batch_arguments[1].column = batch_needle;

                batch_result = executeErasedEqualityPerRowGroup(
                    batch_arguments, result_type, *batch_array, batch_needle, element_type, needle_type);

                if (!batch_result)
                    batch_result = executeArrayAfterErasedEquality(batch_arguments, result_type);

                batch_result = batch_result->convertToFullColumnIfConst();
            }

            const auto & batch_data = assert_cast<const ResultColumnType &>(*batch_result).getData();
            for (size_t row = 0; row < batch_rows; ++row)
                result_data[first_row + row] = batch_data[row];
        }

        return col_result;
    }

    /// Second attempt after a whole-block evaluation declined. A block shares one flattened element
    /// column, so that verdict covers the union of every row's elements; grouping asks it per row
    /// instead. Rows left out keep the answer the existing dispatch gives them.
    ColumnPtr executeErasedEqualityPerRowGroup(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const ColumnArray & array,
        const ColumnPtr & full_needle,
        const DataTypePtr & element_type,
        const DataTypePtr & needle_type) const
    {
        auto labels = groupRowsByAlternative(array, element_type, *full_needle, needle_type);
        if (!labels)
            return nullptr;

        const size_t rows = array.size();
        auto col_result = ResultColumnType::create(rows, ResultType(0));
        auto & result_data = col_result->getData();
        IColumn::Filter undecided(rows, 1);
        bool any_group_decided = false;

        /// The distinct keys present, so only groups a block really holds are built. Only a block
        /// heterogeneous on one of the two sides reaches here: a homogeneous one was answered above.
        UnorderedSetWithMemoryTracking<GroupKey> keys((*labels).begin(), (*labels).end());
        keys.erase(no_group);

        for (auto group_key : keys)
        {
            IColumn::Filter filter(rows, 0);
            for (size_t row = 0; row < rows; ++row)
                filter[row] = (*labels)[row] == group_key;

            auto group_array = array.filter(filter, -1);
            const auto & group = assert_cast<const ColumnArray &>(*group_array);

            auto group_matches = evaluateElementwiseEquality(
                {group.getDataPtr(), element_type, "elements"},
                {full_needle->filter(filter, -1)->replicate(group.getOffsets()), needle_type, "needle"});
            if (!group_matches)
                continue;

            any_group_decided = true;
            auto group_result = foldMatchesPerRow((*group_matches)->getData(), group.getOffsets());

            size_t group_row = 0;
            for (size_t row = 0; row < rows; ++row)
            {
                if (!filter[row])
                    continue;

                result_data[row] = group_result->getData()[group_row++];
                undecided[row] = 0;
            }
        }

        /// Nothing was gained over the whole-block attempt, so leave the call as it was.
        if (!any_group_decided)
            return nullptr;

        /// Rows left over are the ones whose elements disagree, or whose group declined; they must get
        /// exactly the answer the existing dispatch gives them, so it is asked for just those rows.
        const size_t undecided_rows = countBytesInFilter(undecided);
        if (undecided_rows != 0)
        {
            /// Filtering keeps a constant argument constant, which these rows need: for an erased element,
            /// a constant array's elements compare as `Field`s and a materialized one's as columns, and differ.
            auto fallback_arguments = arguments;
            for (auto & argument : fallback_arguments)
                argument.column = argument.column->filter(undecided, -1);

            auto fallback = executeArrayAfterErasedEquality(fallback_arguments, result_type);
            auto fallback_values = fallback->convertToFullColumnIfConst();

            size_t fallback_row = 0;
            for (size_t row = 0; row < rows; ++row)
                if (undecided[row])
                    result_data[row] = assert_cast<const ResultColumnType &>(*fallback_values).getData()[fallback_row++];
        }

        return col_result;
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

        /// A float zero equals two byte-distinct dictionary entries, -0.0 and 0.0, and a single index
        /// cannot denote both, so leave a zero needle to the path that compares values. The needle
        /// type is narrowed only so that reading it as a float is total.
        const auto needle_type = removeNullable(recursiveRemoveLowCardinality(arguments[1].type));
        if (isFloat(removeNullable(target_type)) && (isNumber(needle_type) || isEnum(needle_type))
            && !right_const->isNullAt(0) && right_const->getDataColumnPtr()->getFloat64(0) == 0.0)
            return nullptr;

        UInt64 index = 0;
        UInt64 left_size = arguments[0].column->size();
        ResultColumnPtr col_result = ResultColumnType::create();

        if (!LowCardinalityExecutionHelpers::dictionaryIndexForConstant(
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

        /// A Map key can never be Nullable, so the dictionary of a LowCardinality key column has no
        /// null entry for executeArrayLowCardinality's index-0 NULL needle convention to name.
        /// Normalise the cardinality away before the dispatch reaches that arm.
        for (auto & argument : arguments_copy)
        {
            argument.column = recursiveRemoveLowCardinality(argument.column);
            argument.type = recursiveRemoveLowCardinality(argument.type);
        }

        return executeArray(arguments_copy, result_type);
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
    bool hasPathInObjectRow(
        const ColumnObject & object_column,
        size_t row,
        const String & path,
        const String & prefix,
        const ColumnString * shared_paths,
        const ColumnVector<UInt64>::Container & shared_offsets) const
    {
        /// First, check for the requested path in typed paths.
        /// Typed paths are always considered to be present in each row (even if null),
        /// unless skip_null_typed_paths is enabled.
        const auto & typed_paths = object_column.getTypedPaths();
        if (auto it = typed_paths.find(path); it != typed_paths.end() && (!skip_null_typed_paths || !it->second->isNullAt(row)))
            return true;

        for (const auto & [key, col] : typed_paths)
        {
            if (key.starts_with(prefix) && (!skip_null_typed_paths || !col->isNullAt(row)))
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
     * 1. Typed paths - explicitly declared paths that are always present (even if null,
     *    unless skip_null_typed_paths is enabled)
     * 2. Dynamic paths - paths inferred at runtime, null means absence
     * 3. Shared data - overflow storage for rare paths, uses binary search
     *
     * Optimizations:
     * - For constant path: if found in typed paths, returns 1 for all rows immediately
     * - For constant path: pre-collects relevant columns to avoid repeated lookups
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

            /// Collect columns that match exact path or prefix.
            /// These columns need to be checked for non-null values per row.
            VectorWithMemoryTracking<const IColumn *> relevant_columns;

            const auto & typed_paths = object_column.getTypedPaths();
            bool found_in_typed = false;

            if (auto it = typed_paths.find(path); it != typed_paths.end())
            {
                found_in_typed = true;
                relevant_columns.push_back(it->second.get());
            }

            for (const auto & [key, col] : typed_paths)
            {
                if (key.starts_with(prefix))
                {
                    found_in_typed = true;
                    relevant_columns.push_back(col.get());
                }
            }

            /// Optimization: typed paths are always present, so a match means 1 for all rows.
            /// With skip_null_typed_paths the typed columns are checked per row instead.
            if (found_in_typed && !skip_null_typed_paths)
            {
                std::fill(res_data.begin(), res_data.end(), 1);
                return res_col;
            }

            const auto & dynamic_paths = object_column.getDynamicPathsPtrs();

            if (auto it = dynamic_paths.find(path); it != dynamic_paths.end())
                relevant_columns.push_back(it->second);

            for (const auto & [key, col] : dynamic_paths)
            {
                if (key.starts_with(prefix))
                    relevant_columns.push_back(col);
            }

            for (size_t i = 0; i < input_rows_count; ++i)
            {
                bool found = false;

                for (const auto * col : relevant_columns)
                {
                    if (!col->isNullAt(i))
                    {
                        found = true;
                        break;
                    }
                }

                /// Check shared data if not found in typed or dynamic paths
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
                    null_map_item);
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

    static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type)
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(arguments[0].column.get());

        if (!col_array)
            return nullptr;

        Array arr = col_array->getValue<Array>();
        const IColumn * item_arg = arguments[1].column.get();

        if (isColumnConst(*item_arg))
        {
            ResultType current = 0;
            const auto & value = (*item_arg)[0];
            if constexpr (std::is_same_v<ConcreteAction, IndexOfAssumeSorted>)
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
                    if (!accurateEquals(arr[i], value))
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

    /// Null only when default-constructed, i.e. no context was supplied at all; executeErasedEquality
    /// then declines.
    FunctionOverloadResolverPtr equals_resolver;

    bool skip_null_typed_paths = false;
};
}
