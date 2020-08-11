#include <optional>
#include <type_traits>
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
    static constexpr bool apply(size_t, ResultType & current) noexcept { current = 1; return false; }
};

struct IndexOfAction
{
    using ResultType = UInt64;
    /// The index is returned starting from 1.
    static constexpr bool apply(size_t j, ResultType & current) noexcept { current = j + 1; return false; }
};

struct CountEqualAction
{
    using ResultType = UInt64;
    static constexpr bool apply(size_t, ResultType & current) noexcept { ++current; return true; }
};

/// Impls -- how to perform the search depending on the arguments data types.

template <
    class ConcreteAction,
    bool RightArgIsConstant = false,
    class IntegralInitial = UInt64,
    class IntegralResult = UInt64>
struct ArrayIndexMainImpl
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

    static bool compare(const Initial & left, const PaddedPODArray<Result> & right, size_t, size_t i)
    {
        return left == right[i];
    }

    static bool compare(const PaddedPODArray<Initial> & left, const Result & right, size_t i, size_t)
    {
        return left[i] == right;
    }

    static bool compare(const PaddedPODArray<Initial> & left, const PaddedPODArray<Result> & right, size_t i, size_t j)
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

    static inline bool hasNull(const NullMap * const null_map, size_t i) { return (*null_map)[i]; }

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

                if (!ConcreteAction::apply(j, current))
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
struct ArrayIndexNullImpl
{
    using ResultType = typename ConcreteAction::ResultType;

    static void process(
        const ColumnArray::Offsets & offsets,
        PaddedPODArray<ResultType> & result,
        const NullMap * null_map_data)
    {
        const size_t size = offsets.size();

        result.resize(size);

        ColumnArray::Offset current_offset = 0;

        for (size_t i = 0; i < size; ++i)
        {
            const size_t array_size = offsets[i] - current_offset;
            ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
                if (null_map_data && (*null_map_data)[current_offset + j])
                    if (!ConcreteAction::apply(j, current))
                        break;

            result[i] = current;
            current_offset = offsets[i];
        }
    }
};

template <class ConcreteAction>
struct ArrayIndexStringImpl
{
    static void vector_const(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & value,
        ColumnString::Offset value_size,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const NullMap * null_map_data)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset string_pos = current_offset == 0 && j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + j] - string_pos - 1;

                if (null_map_data && (*null_map_data)[current_offset + j])
                {
                }
                else if (memequalSmallAllowOverflow15(value.data(), value_size, &data[string_pos], string_size))
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }

    static void vectorVector(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnString::Chars & item_values,
        const ColumnString::Offsets & item_offsets,
        PaddedPODArray<typename ConcreteAction::ResultType> & result,
        const NullMap * null_map_data,
        const NullMap * null_map_item)
    {
        const auto size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            const auto array_size = offsets[i] - current_offset;
            typename ConcreteAction::ResultType current = 0;
            const auto value_pos = 0 == i ? 0 : item_offsets[i - 1];
            const auto value_size = item_offsets[i] - value_pos;

            for (size_t j = 0; j < array_size; ++j)
            {
                ColumnArray::Offset string_pos = current_offset == 0 && j == 0
                    ? 0
                    : string_offsets[current_offset + j - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + j] - string_pos;

                bool hit = false;

                if (null_map_data && (*null_map_data)[current_offset + j])
                {
                    if (null_map_item && (*null_map_item)[i])
                        hit = true;
                }
                else if (memequalSmallAllowOverflow15(&item_values[value_pos], value_size, &data[string_pos], string_size))
                    hit = true;

                if (hit)
                {
                    if (!ConcreteAction::apply(j, current))
                        break;
                }
            }

            result[i] = current;
            current_offset = offsets[i];
        }
    }
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

        return allowNested(
                array_nullable_nested,
                arg_or_arg_nullable_nested);
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

    return false;
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
      * we create a new block that contains non-nullable arguments:
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
    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/) const override
    {
        ColumnPtr& ptr = block.getByPosition(arguments[0]).column;

        /**
         * The columns here have two general cases, either being Array(T) or Const(Array(T)).
         * The last type will return nullptr after casting to ColumnArray, so we leave the casting
         * to execute* functions.
         */
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(ptr.get());
        const ColumnNullable * nullable = nullptr;

        if (col_array)
            nullable = checkAndGetColumn<ColumnNullable>(col_array->getData());

        auto & arg_column = block.getByPosition(arguments[1]).column;
        const ColumnNullable * arg_nullable = checkAndGetColumn<ColumnNullable>(*arg_column);

        if (!nullable && !arg_nullable)
            executeOnNonNullable(block, arguments, result);
        else
        {
            /**
             * To correctly process the Nullable values (either #col_array, #arg_column or both) we create a new block
             * and operate on it. The block structure follows:
             * {0, 1, 2, 3, 4}
             * {data (array) argument, "value" argument, data null map, "value" null map, function result}.
             */
            Block source_block = { {}, {}, {}, {}, {nullptr, block.getByPosition(result).type, ""} };

            if (nullable)
            {
                const auto & nested_col = nullable->getNestedColumnPtr();

                auto & data = source_block.getByPosition(0);

                data.column = ColumnArray::create(nested_col, col_array->getOffsetsPtr());
                data.type = std::make_shared<DataTypeArray>(
                    static_cast<const DataTypeNullable &>(
                        *static_cast<const DataTypeArray &>(
                            *block.getByPosition(arguments[0]).type
                        ).getNestedType()
                    ).getNestedType());

                auto & null_map = source_block.getByPosition(2);

                null_map.column = nullable->getNullMapColumnPtr();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & data = source_block.getByPosition(0);
                data = block.getByPosition(arguments[0]);
            }

            if (arg_nullable)
            {
                auto & arg = source_block.getByPosition(1);
                arg.column = arg_nullable->getNestedColumnPtr();
                arg.type =
                    static_cast<const DataTypeNullable &>(
                        *block.getByPosition(arguments[1]).type
                    ).getNestedType();

                auto & null_map = source_block.getByPosition(3);
                null_map.column = arg_nullable->getNullMapColumnPtr();
                null_map.type = std::make_shared<DataTypeUInt8>();
            }
            else
            {
                auto & arg = source_block.getByPosition(1);
                arg = block.getByPosition(arguments[1]);
            }

            /// Now perform the function.
            executeOnNonNullable(source_block, {0, 1, 2, 3}, 4);

            /// Move the result to its final position.
            const ColumnWithTypeAndName & source_col = source_block.getByPosition(4);
            ColumnWithTypeAndName & dest_col = block.getByPosition(result);
            dest_col.column = std::move(source_col.column);
        }
    }

private:
    using ResultType = typename ConcreteAction::ResultType;
    using ResultColumnType = ColumnVector<ResultType>;

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
    std::pair<const NullMap *, const NullMap *>
    getNullMaps(const Block & block, const ColumnNumbers & arguments) const noexcept
    {
        if (arguments.size() < 3)
            return {nullptr, nullptr};

        const NullMap * null_map_data = nullptr;
        const NullMap * null_map_item = nullptr;

        if (const auto & data_map = block.getByPosition(arguments[2]).column; data_map)
            null_map_data = &assert_cast<const ColumnUInt8 &>(*data_map).getData();

        if (const auto & item_map = block.getByPosition(arguments[3]).column; item_map)
            null_map_item = &assert_cast<const ColumnUInt8 &>(*item_map).getData();

        return {null_map_data, null_map_item};
    }

    /**
     * Given a variadic pack #Integral, apply executeIntegralExpanded with such parameters:
     * Integral s = {s1, s2, ...}
     * (s1, s1, s2, ...), (s2, s1, s2, ...), (s3, s1, s2, ...)
     */
    template <class ...Integral>
    bool executeIntegral(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        return (executeIntegralExpanded<Integral, Integral...>(block, arguments, result) || ...);
    }

    /// Invoke executeIntegralImpl with such parameters: (A, other1), (A, other2), ...
    template <class A, class ...Other>
    bool executeIntegralExpanded(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        return (executeIntegralImpl<A, Other>(block, arguments, result) || ...);
    }

    /**
     * The internal data type of the first argument (target array), if it's integral, like UInt8, may differ from the
     * second argument, namely, the @e value, so it's possible to invoke the <tt>has(Array(Int8), UInt64)</tt> e.g.
     * so we have to check all possible variants for #Initial and #Resulting types.
     */
    template <class Initial, class Resulting>
    bool executeIntegralImpl(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnVector<Initial> * col_nested = checkAndGetColumn<ColumnVector<Initial>>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        const auto [null_map_data, null_map_item] = getNullMaps(block, arguments);
        const IColumn* item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
            ArrayIndexNullImpl<ConcreteAction>::process(
                col_array->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (const auto item_arg_const = checkAndGetColumnConst<ColumnVector<Resulting>>(item_arg))
            ArrayIndexMainImpl<ConcreteAction, true, Initial, Resulting>::vector(
                col_nested->getData(),
                col_array->getOffsets(),
                item_arg_const->template getValue<Resulting>(),
                col_res->getData(),
                null_map_data,
                nullptr);
        else if (const auto item_arg_vector = checkAndGetColumn<ColumnVector<Resulting>>(item_arg))
            ArrayIndexMainImpl<ConcreteAction, false, Initial, Resulting>::vector(
                col_nested->getData(),
                col_array->getOffsets(),
                item_arg_vector->getData(),
                col_res->getData(),
                null_map_data,
                null_map_item);
        else
            return false;

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    /**
     * Catches arguments of type LC(T) (left) and ColumnConst (right).
     *
     * The perftests
     * https://clickhouse-test-reports.s3.yandex.net/12550/2d27fa0fa8c198a82bf1fe3625050ccf56695976/integration_tests_(release).html
     * showed that the amount of action needed to convert the non-constant right argument to the index column
     * (similar to the left one's) is significantly higher than converting the array itself to an ordinary column.
     *
     * So, in terms of performance it's more optimal to fall back to default implementation and catch only constant
     * right arguments here.
     *
     * Tips and tricks tried can be found at https://github.com/ClickHouse/ClickHouse/pull/12550 .
     */
    bool executeLowCardinalityAndConst(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * const col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnLowCardinality * const col_lc = checkAndGetColumn<ColumnLowCardinality>(&col_array->getData());

        if (!col_lc)
            return false;

        auto col_result = ResultColumnType::create();

        const ColumnConst * const col_arg = checkAndGetColumn<ColumnConst>(
                block.getByPosition(arguments[1]).column.get());

        if (!col_arg)
            return false;

        const IColumnUnique& col_lc_dict = col_lc->getDictionary();

        const bool different_inner_types = col_lc_dict.isNullable()
            ? !col_arg->structureEquals(*col_lc_dict.getNestedColumn().get())
            : true; // Can't compare so ignore this check

        const bool use_cloned_arg =
            col_arg->isNumeric()
            // outer types do not match
            && !col_arg->structureEquals(col_lc_dict)
            // inner types do not match (like A and Nullable(B) or A and Const(B));
            && different_inner_types;

        const DataTypeArray * const array_type = checkAndGetDataType<DataTypeArray>(
                block.getByPosition(arguments[0]).type.get());
        const DataTypePtr target_type_ptr = recursiveRemoveLowCardinality(array_type->getNestedType());

        const ColumnPtr col_arg_cloned = use_cloned_arg
            ? castColumn(block.getByPosition(arguments[1]), target_type_ptr)
            : col_arg->getPtr();

        const auto [null_map_data, null_map_item] = getNullMaps(block, arguments);

        const StringRef elem = col_arg_cloned->getDataAt(0);

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

                block.getByPosition(result).column = std::move(col_result);
                return true;
            }
        }

        ArrayIndexMainImpl<ConcreteAction, true>::vector(
            col_lc->getIndexes(),
            col_array->getOffsets(),
            index,
            col_result->getData(),
            null_map_data,
            null_map_item);

        block.getByPosition(result).column = std::move(col_result);
        return true;
    }

    bool executeString(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

        if (!col_nested)
            return false;

        auto col_res = ResultColumnType::create();

        const auto [null_map_data, null_map_item] = getNullMaps(block, arguments);
        const IColumn * item_arg = block.getByPosition(arguments[1]).column.get();

        if (item_arg->onlyNull())
            ArrayIndexNullImpl<ConcreteAction>::process(
                col_array->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (const auto *const item_arg_const = checkAndGetColumnConstStringOrFixedString(item_arg))
        {
            const ColumnString * item_const_string =
                checkAndGetColumn<ColumnString>(&item_arg_const->getDataColumn());

            const ColumnFixedString * item_const_fixedstring =
                checkAndGetColumn<ColumnFixedString>(&item_arg_const->getDataColumn());

            if (item_const_string)
                ArrayIndexStringImpl<ConcreteAction>::vector_const(
                    col_nested->getChars(),
                    col_array->getOffsets(),
                    col_nested->getOffsets(),
                    item_const_string->getChars(),
                    item_const_string->getDataAt(0).size,
                    col_res->getData(),
                    null_map_data);
            else if (item_const_fixedstring)
                ArrayIndexStringImpl<ConcreteAction>::vector_const(
                    col_nested->getChars(),
                    col_array->getOffsets(),
                    col_nested->getOffsets(),
                    item_const_fixedstring->getChars(),
                    item_const_fixedstring->getN(),
                    col_res->getData(),
                    null_map_data);
            else
                throw Exception(
                    "Logical error: ColumnConst contains not String nor FixedString column",
                        ErrorCodes::ILLEGAL_COLUMN);
        }
        else if (const auto *const item_arg_vector = checkAndGetColumn<ColumnString>(item_arg))
        {
            ArrayIndexStringImpl<ConcreteAction>::vectorVector(
                col_nested->getChars(),
                col_array->getOffsets(),
                col_nested->getOffsets(),
                item_arg_vector->getChars(),
                item_arg_vector->getOffsets(),
                col_res->getData(),
                null_map_data,
                null_map_item);
        }
        else
            return false;

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    bool executeConst(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnConst * col_array = checkAndGetColumnConst<ColumnArray>(
                block.getByPosition(arguments[0]).column.get());

        if (!col_array)
            return false;

        Array arr = col_array->getValue<Array>();

        const IColumn * item_arg = block.getByPosition(arguments[1]).column.get();

        if (isColumnConst(*item_arg))
        {
            ResultType current = 0;
            const auto & value = (*item_arg)[0];

            for (size_t i = 0, size = arr.size(); i < size; ++i)
            {
                if (applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                {
                    if (!ConcreteAction::apply(i, current))
                        break;
                }
            }

            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(
                item_arg->size(), static_cast<ResultType>(current));
        }
        else
        {
            /// Null map of the 2nd function argument, if it applies.
            const NullMap * null_map = nullptr;

            if (arguments.size() > 2)
            {
                const auto & col = block.getByPosition(arguments[3]).column;
                if (col)
                    null_map = &assert_cast<const ColumnUInt8 &>(*col).getData();
            }

            const auto size = item_arg->size();
            auto col_res = ResultColumnType::create(size);

            auto & data = col_res->getData();

            for (size_t row = 0; row < size; ++row)
            {
                const auto & value = (*item_arg)[row];

                data[row] = 0;
                for (size_t i = 0, arr_size = arr.size(); i < arr_size; ++i)
                {
                    bool hit = false;

                    if (arr[i].isNull())
                    {
                        if (null_map && (*null_map)[row])
                            hit = true;
                    }
                    else if (applyVisitor(FieldVisitorAccurateEquals(), arr[i], value))
                        hit = true;

                    if (hit)
                    {
                        if (!ConcreteAction::apply(i, data[row]))
                            break;
                    }
                }
            }

            block.getByPosition(result).column = std::move(col_res);
        }

        return true;
    }

    bool executeGeneric(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        const ColumnArray * col = checkAndGetColumn<ColumnArray>(block.getByPosition(arguments[0]).column.get());

        if (!col)
            return false;

        const IColumn & col_nested = col->getData();
        const IColumn & item_arg = *block.getByPosition(arguments[1]).column;

        auto col_res = ResultColumnType::create();

        auto [null_map_data, null_map_item] = getNullMaps(block, arguments);

        if (item_arg.onlyNull())
            ArrayIndexNullImpl<ConcreteAction>::process(
                col->getOffsets(),
                col_res->getData(),
                null_map_data);
        else if (isColumnConst(item_arg))
            ArrayIndexMainImpl<ConcreteAction, true>::vector(
                col_nested,
                col->getOffsets(),
                assert_cast<const ColumnConst &>(item_arg).getDataColumn(),
                col_res->getData(), /// TODO This is wrong.
                null_map_data,
                nullptr);
        else
        {
            const auto casted = col_nested.convertToFullColumnIfLowCardinality();
            ArrayIndexMainImpl<ConcreteAction>::vector(
                *casted.get(),
                col->getOffsets(),
                *item_arg.convertToFullColumnIfConst(),
                col_res->getData(),
                null_map_data,
                null_map_item);
        }

        block.getByPosition(result).column = std::move(col_res);
        return true;
    }

    void executeOnNonNullable(Block & block, const ColumnNumbers & arguments, size_t result) const
    {
        if (!(executeIntegral<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64>(block, arguments, result)
              || executeConst(block, arguments, result)
              || executeString(block, arguments, result)
              || executeLowCardinalityAndConst(block, arguments, result)
              || executeGeneric(block, arguments, result)))
            throw Exception(
                "Illegal internal type of first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};
}

