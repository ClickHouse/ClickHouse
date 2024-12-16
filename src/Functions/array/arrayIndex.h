#pragma once
#include <cstddef>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeArray.h>
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
#include "Common/Logger.h"
#include "Common/logger_useful.h"
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/memcmpSmall.h>
#include <Common/assert_cast.h>
#include <Columns/ColumnLowCardinality.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/castColumn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
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

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wsign-compare"

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
    static bool compare(const IColumn & left, const Result & right, size_t i, size_t)
    {
        return left.getUInt(i) == right;
    }

    /// Generic
    static bool compare(const IColumn & left, const IColumn & right, size_t i, size_t j)
    {
        return 0 == left.compareAt(i, RightArgIsConstant ? 0 : j, right, 1);
    }

#pragma clang diagnostic pop

    static constexpr bool hasNull(const NullMap * const null_map, size_t i) noexcept { return (*null_map)[i]; }

    template <size_t Case, typename Data, typename Target>
    static void process(
        const Data & data, const ArrOffsets & offsets, const Target & target, ResultArr & result,
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
class FunctionArrayIndex : public IFunction
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

        /// If map is first argument only has(map_column, key) function is supported
        if constexpr (std::is_same_v<ConcreteAction, HasAction>)
        {
            if (!array_type && !map_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument for function {} must be an array or map. Actual {}",
                    getName(),
                    first_argument_type->getName());

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
    ColumnPtr executeArrayImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
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
            nullable = checkAndGetColumn<ColumnNullable>(&col_array->getData());

        const auto & arg_column = arguments[1].column;
        const ColumnNullable * arg_nullable = checkAndGetColumn<ColumnNullable>(&*arg_column);

        if (!nullable && !arg_nullable)
        {
            return executeOnNonNullable(arguments, result_type);
        }

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
        auto right = recursiveRemoveLowCardinality(right_const->getDataColumnPtr());

        UInt64 index = 0;
        UInt64 left_size = arguments[0].column->size();
        ResultColumnPtr col_result = ResultColumnType::create();

        if (!right->isNullAt(0))
        {
            auto right_type = recursiveRemoveLowCardinality(arguments[1].type);
            right = castColumn({right, right_type, ""}, target_type);

            if (right->isNullable())
                right = checkAndGetColumn<ColumnNullable>(*right).getNestedColumnPtr();

            StringRef elem = right->getDataAt(0);
            const auto & left_dict = left_lc->getDictionary();

            if (std::optional<UInt64> maybe_index = left_dict.getOrFindValueIndex(elem); maybe_index)
            {
                index = *maybe_index;
            }
            else
            {
                col_result->getData().resize_fill(col_array->size());

                if (col_array_const)
                    return ColumnConst::create(std::move(col_result), left_size);

                return col_result;
            }
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

        return executeArrayImpl(arguments_copy, result_type);
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
                    item_const_string->getDataAt(0).size,
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

            for (size_t i = 0, size = arr.size(); i < size; ++i)
            {
                if (!applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                    continue;

                ConcreteAction::apply(current, i);

                if constexpr (!ConcreteAction::resume_execution)
                    break;
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
                    if (!applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
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
