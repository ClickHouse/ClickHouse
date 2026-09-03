#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeQBit.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionLowCardinalityFastPath.h>
#include <Functions/IFunction.h>
#include <Functions/LowCardinalityExecutionHelpers.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context_fwd.h>
#include <base/TypeList.h>
#include <Interpreters/castColumn.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include <Common/VectorWithMemoryTracking.h>

#include <bit>
#include <cstring>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ZERO_ARRAY_OR_TUPLE_INDEX;
}

namespace
{

enum class ArrayElementExceptionMode : uint8_t
{
    Zero, /// Fill with zero or default if array index out of range or map key not found.
    Null /// Return ColumnNullable with NULLs if array index out of range or map key not found.
};

namespace ArrayImpl
{
template <ArrayElementExceptionMode mode>
class NullMapBuilder;
}

/// Array element types with a dedicated fast path. The dispatch ladders below try them in this
/// order, so the most common types come first; the generic fallback handles everything else.
using ElementTypesWithFastPath = TypeList<
    UInt8, UInt16, UInt32, UInt64,
    Int8, Int16, Int32, Int64,
    Float32, Float64,
    Decimal32, Decimal64, Decimal128, Decimal256,
    DateTime64,
    Int128, UInt128, Int256, UInt256>;

/// Types the index (second) argument may have, in dispatch order.
using IndexTypes = TypeList<UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64>;

/// Calls try_one<T> for each T in the list until one call succeeds. The fold expands to the same
/// short-circuit `||` chain as spelling out one call per type.
template <typename... Ts, typename F>
bool tryEachType(TypeList<Ts...>, F && try_one)
{
    return (try_one.template operator()<Ts>() || ...);
}


/** arrayElement(arr, i) - get the array element by index. If index is not constant and out of range - return default value of data type.
  * The index begins with 1. Also, the index can be negative - then it is counted from the end of the array.
  */
template <ArrayElementExceptionMode mode = ArrayElementExceptionMode::Zero>
class FunctionArrayElement : public IFunction
{
public:
    static constexpr bool is_null_mode = (mode == ArrayElementExceptionMode::Null);
    static constexpr auto name = (mode == ArrayElementExceptionMode::Zero) ? "arrayElement" : "arrayElementOrNull";

    String getName() const override;

    bool useDefaultImplementationForConstants() const override { return true; }
    /// A lazily replicated array argument is consumed by gathering from the compact nested column.
    /// When true it materializes the compacted representation to a full column.
    bool useDefaultImplementationForReplicatedColumns() const override { return false; }
    /// `Nullable(QBit)` with an array of indices must produce `Array(Nullable(T))`,
    /// which cannot be represented by the default nullable wrapper around the result.
    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForDynamic() const override { return true; }
    bool useDefaultImplementationForVariant() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    /// Keep the inherited getReturnTypeImpl(ColumnsWithTypeAndName) visible alongside the
    /// overload declared below; FunctionWithLowCardinalityFastPath calls it by qualified name.
    using IFunction::getReturnTypeImpl;
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

    /// Fast path hook for FunctionWithLowCardinalityFastPath (see FunctionLowCardinalityFastPath.h):
    /// element access over Array(LowCardinality(String)) and Map with LowCardinality string keys
    /// without materializing the dictionary into full columns. Returns nullptr to decline.
    ColumnPtr tryExecuteLowCardinality(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const;

private:
    ColumnPtr perform(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        ArrayImpl::NullMapBuilder<mode> & builder,
        size_t input_rows_count) const;

    /// Element access over a lazily replicated array (Replicated(Array)) without materializing it.
    ColumnPtr executeReplicated(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const;

    template <typename IndexType>
    static bool gatherReplicated(
        const IColumn & index_column,
        const ColumnIndex & replication_indexes,
        const ColumnArray::Offsets & offsets,
        const IColumn & data,
        IColumn & result,
        ArrayImpl::NullMapBuilder<mode> & builder);

    template <typename DataType>
    static ColumnPtr executeNumberConst(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const Field & index,
        ArrayImpl::NullMapBuilder<mode> & builder);

    template <typename IndexType, typename DataType>
    static ColumnPtr executeNumber(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder<mode> & builder);

    /// Optimize arrayElement when first argument has type Array(T) or Array(Nullable(T)) in which T is number type
    template <typename DataType>
    static ColumnPtr executeArrayNumberConst(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const Field & index,
        ArrayImpl::NullMapBuilder<mode> & builder);

    /// Optimize arrayElement when first argument has type Array(T) or Array(Nullable(T)) in which T is number type
    template <typename IndexType, typename DataType>
    static ColumnPtr executeArrayNumber(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder<mode> & builder);

    static ColumnPtr
    executeStringConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder);

    template <typename IndexType>
    static ColumnPtr executeString(
        const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder);

    /// Optimize arrayElement when first argument has type Array(String) or Array(Nullable(String))
    static ColumnPtr
    executeArrayStringConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder);

    /// Optimize arrayElement when first argument has type Array(String) or Array(Nullable(String))
    template <typename IndexType>
    static ColumnPtr executeArrayString(
        const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder);

    static ColumnPtr
    executeGenericConst(const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder);

    static ColumnPtr removeNullableIfNeeded(const ColumnPtr & column, const DataTypePtr & expect_type);

    template <typename IndexType>
    static ColumnPtr executeGeneric(
        const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder);

    template <typename IndexType>
    static ColumnPtr executeConst(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        const PaddedPODArray<IndexType> & indices,
        ArrayImpl::NullMapBuilder<mode> & builder,
        size_t input_rows_count);

    template <typename IndexType>
    ColumnPtr executeArgument(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        ArrayImpl::NullMapBuilder<mode> & builder,
        size_t input_rows_count) const;


    /** For a tuple array, the function is evaluated component-wise for each element of the tuple.
      */
    ColumnPtr executeTuple(const ColumnsWithTypeAndName & arguments, ArrayImpl::NullMapBuilder<mode> &, size_t input_rows_count) const;

    /** For a map array, the function is evaluated component-wise for its keys and values
      */
    ColumnPtr executeMap2(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    /** For a map the function finds the matched value for a key.
     *  Currently implemented just as linear search in array.
     *  However, optimizations are possible.
     */
    ColumnPtr executeMap(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const;

    ColumnPtr executeWithArrayIndex(
        const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const;

    /** For a QBit, reconstructs the n-th vector element from its bit planes. Only the planes of the single
      * stride group that contains the element are read.
      */
    ColumnPtr executeQBit(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    template <typename T>
    ColumnPtr executeQBitImpl(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    template <typename T>
    ColumnPtr executeQBitWithArrayIndex(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    using Offsets = ColumnArray::Offsets;

    static bool matchKeyToIndexNumber(
        const IColumn & data, const Offsets & offsets, bool is_key_const, const IColumn & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool
    matchKeyToIndexNumberConst(const IColumn & data, const Offsets & offsets, const Field & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool matchKeyToIndexString(
        const IColumn & data, const Offsets & offsets, bool is_key_const, const IColumn & index, PaddedPODArray<UInt64> & matched_idxs);

    static bool
    matchKeyToIndexStringConst(const IColumn & data, const Offsets & offsets, const Field & index, PaddedPODArray<UInt64> & matched_idxs);

    template <typename Matcher>
    static void executeMatchKeyToIndex(const Offsets & offsets, PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher);

    template <typename Matcher>
    static void
    executeMatchConstKeyToIndex(size_t num_rows, size_t num_values, PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher);
};


namespace ArrayImpl
{

template <ArrayElementExceptionMode mode>
class NullMapBuilder
{
public:
    static constexpr bool is_null_mode = (mode == ArrayElementExceptionMode::Null);
    explicit operator bool() const { return src_null_map || is_null_mode; }

    void initSource(const UInt8 * src_null_map_) { src_null_map = src_null_map_; }

    void initSink(size_t size)
    {
        auto sink = ColumnUInt8::create(size);
        sink_null_map = sink->getData().data();
        sink_null_map_holder = std::move(sink);
    }

    void update(size_t from)
    {
        if (sink_null_map)
        {
            sink_null_map[index] = src_null_map && src_null_map[from];
            ++index;
        }
    }

    void update()
    {
        if (sink_null_map)
        {
            sink_null_map[index] = 1;
            ++index;
        }
    }

    ColumnPtr getNullMapColumnPtr() && { return std::move(sink_null_map_holder); }

private:
    const UInt8 * src_null_map = nullptr;
    UInt8 * sink_null_map = nullptr;
    MutableColumnPtr sink_null_map_holder;
    size_t index = 0;
};

}

namespace
{

template <typename T, ArrayElementExceptionMode mode>
struct ArrayElementNumImpl
{
    /** Implementation for constant index.
      * If negative = false - index is from beginning of array, started from 0.
      * If negative = true - index is from end of array, started from 0.
      */
    template <bool negative>
    static void vectorConst(
        const PaddedPODArray<T> & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index,
        PaddedPODArray<T> & result,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = 0;
                if constexpr (negative)
                    j = offsets[i] - index - 1;
                else
                    j = current_offset + index;
                result[i] = data[j];
                builder.update(j);
            }
            else
            {
                result[i] = T();
                builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const PaddedPODArray<T> & data,
        const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices,
        PaddedPODArray<T> & result,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result.resize(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result[i] = data[j];
                builder.update(j);
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result[i] = data[j];
                builder.update(j);
            }
            else
            {
                result[i] = T();
                builder.update();
            }

            current_offset = offsets[i];
        }
    }
};


template <typename T, ArrayElementExceptionMode mode>
struct ArrayElementArrayNumImpl
{
    /** Implementation for constant index.
      * If negative = false - index is from beginning of array, started from 0.
      * If negative = true - index is from end of array, started from 0.
      * nullable_number is true when process input with type Array(Array(Nullable(T)))
      */
    template <bool negative, bool nullable_number>
    static void vectorConst(
        const PaddedPODArray<T> & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offsets & nested_offsets,
        const NullMap * number_null_map,
        const ColumnArray::Offset index,
        PaddedPODArray<T> & result_data,
        ColumnArray::Offsets & result_offsets,
        NullMap * result_number_null_map,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();

        /// First calculate the size of result_data or result_number_null_map
        size_t result_data_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            if (index < array_size)
            {
                size_t j = !negative ? (offsets[i - 1] + index) : (offsets[i] - index - 1);
                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                result_data_size += nested_array_size;
            }
        }

        /// Allocate enough memory in advance
        result_data.resize(result_data_size);
        result_offsets.resize(size);
        if constexpr (nullable_number)
            result_number_null_map->resize(result_data_size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            if (index < array_size)
            {
                size_t j = !negative ? (offsets[i - 1] + index) : (offsets[i] - index - 1);
                builder.update(j);

                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                ColumnArray::Offset nested_array_pos = nested_offsets[j - 1];
                memcpy(&result_data[current_offset], &data[nested_array_pos], nested_array_size * sizeof(T));
                if constexpr (nullable_number)
                    memcpy(&(*result_number_null_map)[current_offset], &(*number_null_map)[nested_array_pos], nested_array_size);

                current_offset += nested_array_size;
            }
            else
            {
                /// Empty Array(T), no need to copy anything
                builder.update();
            }

            result_offsets[i] = current_offset;
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex, bool nullable_number>
    static void vector(
        const PaddedPODArray<T> & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offsets & nested_offsets,
        const NullMap * number_null_map,
        const PaddedPODArray<TIndex> & indices,
        PaddedPODArray<T> & result_data,
        ColumnArray::Offsets & result_offsets,
        NullMap * result_number_null_map,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();

        /// First calculate the size of result_data or result_number_null_map
        size_t result_data_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i - 1] + index - 1;
                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                result_data_size += nested_array_size;
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                result_data_size += nested_array_size;
            }
        }

        /// Allocate enough memory in advance
        result_data.resize(result_data_size);
        result_offsets.resize(size);
        if constexpr (nullable_number)
            result_number_null_map->resize(result_data_size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i - 1] + index - 1;
                builder.update(j);

                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                ColumnArray::Offset nested_array_pos = nested_offsets[j - 1];
                memcpy(&result_data[current_offset], &data[nested_array_pos], nested_array_size * sizeof(T));
                if constexpr (nullable_number)
                    memcpy(&(*result_number_null_map)[current_offset], &(*number_null_map)[nested_array_pos], nested_array_size);

                current_offset += nested_array_size;
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                builder.update(j);

                ColumnArray::Offset nested_array_size = nested_offsets[j] - nested_offsets[j - 1];
                ColumnArray::Offset nested_array_pos = nested_offsets[j - 1];
                memcpy(&result_data[current_offset], &data[nested_array_pos], nested_array_size * sizeof(T));
                if constexpr (nullable_number)
                    memcpy(&(*result_number_null_map)[current_offset], &(*number_null_map)[nested_array_pos], nested_array_size);

                current_offset += nested_array_size;
            }
            else
            {
                /// Empty Array(T), no need to copy anything
                builder.update();
            }

            result_offsets[i] = current_offset;
        }
    }
};

template <ArrayElementExceptionMode mode>
struct ArrayElementArrayStringImpl
{
    /// nullable_string is true when process input with type Array(Array(Nullable(String)))
    template <bool negative, bool nullable_string>
    static void vectorConst(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offsets & nested_offsets,
        const ColumnString::Offsets & string_offsets,
        const NullMap * string_null_map,
        const ColumnArray::Offset index,
        ColumnString::Chars & result_data,
        ColumnArray::Offsets & result_offsets,
        ColumnArray::Offsets & result_string_offsets,
        NullMap * result_string_null_map,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();

        /// First calculate size of result_data(total count of strings) and result_string_offsets(total size of strings)
        size_t result_data_size = 0;
        size_t result_strings_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            if (index < array_size)
            {
                size_t adjusted_index = !negative ? index : (array_size - index - 1);
                size_t j = offsets[i - 1] + adjusted_index;

                auto nested_array_start = nested_offsets[j - 1];
                auto nested_array_size = nested_offsets[j] - nested_array_start;

                result_data_size += string_offsets[nested_array_start + nested_array_size - 1] - string_offsets[nested_array_start - 1];
                result_strings_size += nested_array_size;
            }
        }

        /// Allocate enough memory in advance
        result_data.resize(result_data_size);
        result_offsets.resize(size);
        result_string_offsets.reserve(result_strings_size);
        if constexpr (nullable_string)
            result_string_null_map->reserve(result_strings_size);

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_string_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];

            if (index < array_size)
            {
                size_t adjusted_index = !negative ? index : (array_size - index - 1);

                size_t j = offsets[i - 1] + adjusted_index;
                builder.update(j);

                auto nested_array_start = nested_offsets[j - 1];
                auto nested_array_size = nested_offsets[j] - nested_array_start;

                /// For each String in Array(String), append it to result_data and update result_offsets and result_string_offsets
                for (size_t k = 0; k < nested_array_size; ++k)
                {
                    auto string_start = string_offsets[nested_array_start + k - 1];
                    auto string_size = string_offsets[nested_array_start + k] - string_start;
                    memcpySmallAllowReadWriteOverflow15(&result_data[current_string_offset], &data[string_start], string_size);
                    current_string_offset += string_size;
                    result_string_offsets.push_back(current_string_offset);

                    if constexpr (nullable_string)
                        result_string_null_map->push_back((*string_null_map)[nested_array_start + k]);
                }
                current_offset += nested_array_size;
            }
            else
            {
                /// Insert empty Array(String) or Array(Nullable(String)), no need to copy anything
                builder.update();
            }
            result_offsets[i] = current_offset;
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex, bool nullable_string>
    static void vector(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offsets & nested_offsets,
        const ColumnString::Offsets & string_offsets,
        const NullMap * string_null_map,
        const PaddedPODArray<TIndex> & indices,
        ColumnString::Chars & result_data,
        ColumnArray::Offsets & result_offsets,
        ColumnArray::Offsets & result_string_offsets,
        NullMap * result_string_null_map,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();

        /// First calculate size of result_data(total count of strings) and result_string_offsets(total size of strings)
        size_t result_data_size = 0;
        size_t result_strings_size = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            size_t adjusted_index = 0; /// index in array from zero
            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
                adjusted_index = index - 1;
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
                adjusted_index = array_size + index;
            else
                adjusted_index = array_size; /// means no element should be taken

            if (adjusted_index < array_size)
            {
                size_t j = offsets[i - 1] + adjusted_index;

                auto nested_array_start = nested_offsets[j - 1];
                auto nested_array_size = nested_offsets[j] - nested_array_start;

                result_data_size += string_offsets[nested_array_start + nested_array_size - 1] - string_offsets[nested_array_start - 1];
                result_strings_size += nested_array_size;
            }
        }

        /// Allocate enough memory in advance
        result_data.resize(result_data_size);
        result_offsets.resize(size);
        result_string_offsets.reserve(result_strings_size);
        if constexpr (nullable_string)
            result_string_null_map->reserve(result_strings_size);

        ColumnArray::Offset current_offset = 0;
        ColumnArray::Offset current_string_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - offsets[i - 1];
            size_t adjusted_index = 0; /// index in array from zero

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
                adjusted_index = index - 1;
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
                adjusted_index = array_size + index;
            else
                adjusted_index = array_size; /// means no element should be taken


            if (adjusted_index < array_size)
            {
                size_t j = offsets[i - 1] + adjusted_index;
                builder.update(j);

                auto nested_array_start = nested_offsets[j - 1];
                auto nested_array_size = nested_offsets[j] - nested_array_start;

                /// For each String in Array(String), append it to result_data and update result_offsets and result_string_offsets
                for (size_t k = 0; k < nested_array_size; ++k)
                {
                    auto string_start = string_offsets[nested_array_start + k - 1];
                    auto string_size = string_offsets[nested_array_start + k] - string_start;
                    memcpySmallAllowReadWriteOverflow15(&result_data[current_string_offset], &data[string_start], string_size);
                    current_string_offset += string_size;
                    result_string_offsets.push_back(current_string_offset);

                    if constexpr (nullable_string)
                        result_string_null_map->push_back((*string_null_map)[nested_array_start + k]);
                }
                current_offset += nested_array_size;
            }
            else
            {
                /// Insert empty Array(String), no need to copy anything
                builder.update();
            }
            result_offsets[i] = current_offset;
        }
    }
};


template <ArrayElementExceptionMode mode>
struct ArrayElementStringImpl
{
    template <bool negative>
    static void vectorConst(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const ColumnArray::Offset index,
        ColumnString::Chars & result_data,
        ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);

        ColumnArray::Offset current_offset = 0;
        /// get the total result bytes at first, and reduce the cost of result_data.resize.
        size_t total_result_bytes = 0;
        VectorWithMemoryTracking<std::pair<const ColumnString::Char *, UInt64>> selected_bufs;
        selected_bufs.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t adjusted_index = 0;
                if constexpr (negative)
                    adjusted_index = array_size - index - 1;
                else
                    adjusted_index = index;

                size_t j = current_offset + adjusted_index;
                builder.update(j);

                ColumnArray::Offset string_pos
                    = current_offset == 0 && adjusted_index == 0 ? 0 : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;

                total_result_bytes += string_size;
                selected_bufs.emplace_back(&data[string_pos], string_size);
                result_offsets[i] = total_result_bytes;
            }
            else
            {
                result_offsets[i] = total_result_bytes;
                builder.update();
            }

            current_offset = offsets[i];
        }

        ColumnArray::Offset current_result_offset = 0;
        result_data.resize(total_result_bytes);
        for (const auto & buf : selected_bufs)
        {
            memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], buf.first, buf.second);
            current_result_offset += buf.second;
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const ColumnString::Chars & data,
        const ColumnArray::Offsets & offsets,
        const ColumnString::Offsets & string_offsets,
        const PaddedPODArray<TIndex> & indices,
        ColumnString::Chars & result_data,
        ColumnArray::Offsets & result_offsets,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result_offsets.resize(size);

        ColumnArray::Offset current_offset = 0;
        /// get the total result bytes at first, and reduce the cost of result_data.resize.
        size_t total_result_bytes = 0;
        VectorWithMemoryTracking<std::pair<const ColumnString::Char *, UInt64>> selected_bufs;
        selected_bufs.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            size_t adjusted_index = 0; /// index in array from zero

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
                adjusted_index = index - 1;
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
                adjusted_index = array_size + index;
            else
                adjusted_index = array_size; /// means no element should be taken

            if (adjusted_index < array_size)
            {
                size_t j = current_offset + adjusted_index;
                builder.update(j);

                ColumnArray::Offset string_pos
                    = current_offset == 0 && adjusted_index == 0 ? 0 : string_offsets[current_offset + adjusted_index - 1];

                ColumnArray::Offset string_size = string_offsets[current_offset + adjusted_index] - string_pos;
                total_result_bytes += string_size;
                selected_bufs.emplace_back(&data[string_pos], string_size);

                result_offsets[i] = total_result_bytes;
            }
            else
            {
                result_offsets[i] = total_result_bytes;
                builder.update();
            }

            current_offset = offsets[i];
        }

        ColumnArray::Offset current_result_offset = 0;
        result_data.resize(total_result_bytes);
        for (const auto & buf : selected_bufs)
        {
            memcpySmallAllowReadWriteOverflow15(&result_data[current_result_offset], buf.first, buf.second);
            current_result_offset += buf.second;
        }
    }
};

/// Generic implementation for other nested types.
template <ArrayElementExceptionMode mode>
struct ArrayElementGenericImpl
{
    template <bool negative>
    static void vectorConst(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const ColumnArray::Offset index,
        IColumn & result,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t j = !negative ? current_offset + index : offsets[i] - index - 1;
                result.insertFrom(data, j);
                builder.update(j);
            }
            else
            {
                result.insertDefault();
                builder.update();
            }

            current_offset = offsets[i];
        }
    }

    /** Implementation for non-constant index.
      */
    template <typename TIndex>
    static void vector(
        const IColumn & data,
        const ColumnArray::Offsets & offsets,
        const PaddedPODArray<TIndex> & indices,
        IColumn & result,
        ArrayImpl::NullMapBuilder<mode> & builder)
    {
        size_t size = offsets.size();
        result.reserve(size);

        ColumnArray::Offset current_offset = 0;
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            TIndex index = indices[i];
            if (index > 0 && static_cast<size_t>(index) <= array_size)
            {
                size_t j = current_offset + index - 1;
                result.insertFrom(data, j);
                builder.update(j);
            }
            else if (index < 0 && -static_cast<size_t>(index) <= array_size)
            {
                size_t j = offsets[i] + index;
                result.insertFrom(data, j);
                builder.update(j);
            }
            else
            {
                result.insertDefault();
                builder.update();
            }

            current_offset = offsets[i];
        }
    }
};

}

template <ArrayElementExceptionMode mode>
template <typename DataType>
ColumnPtr FunctionArrayElement<mode>::executeNumberConst(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    const Field & index,
    ArrayImpl::NullMapBuilder<mode> & builder)
{
    using ColVecType = ColumnVectorOrDecimal<DataType>;

    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColVecType * col_nested = checkAndGetColumn<ColVecType>(&col_array->getData());
    if (!col_nested)
        return nullptr;

    auto col_res = result_type->createColumn();
    ColVecType * col_res_vec = typeid_cast<ColVecType *>(col_res.get());
    if (!col_res_vec)
        return nullptr;

    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
    {
        ArrayElementNumImpl<DataType, mode>::template vectorConst<false>(
            col_nested->getData(), col_array->getOffsets(), index.safeGet<UInt64>() - 1, col_res_vec->getData(), builder);
    }
    else if (index.getType() == Field::Types::Int64)
    {
        /// Cast to UInt64 before negation allows to avoid undefined behaviour for negation of the most negative number.
        /// NOTE: this would be undefined behaviour in C++ sense, but nevertheless, compiler cannot see it on user provided data,
        /// and generates the code that we want on supported CPU architectures (overflow in sense of two's complement arithmetic).
        /// This is only needed to avoid UBSan report.

        /// Negative array indices work this way:
        /// arr[-1] is the element at offset 0 from the last
        /// arr[-2] is the element at offset 1 from the last and so on.
        ArrayElementNumImpl<DataType, mode>::template vectorConst<true>(
            col_nested->getData(),
            col_array->getOffsets(),
            -(static_cast<UInt64>(index.safeGet<Int64>()) + 1),
            col_res_vec->getData(),
            builder);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal type of array index");

    return col_res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType, typename DataType>
ColumnPtr FunctionArrayElement<mode>::executeNumber(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder<mode> & builder)
{
    using ColVecType = ColumnVectorOrDecimal<DataType>;

    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColVecType * col_nested = checkAndGetColumn<ColVecType>(&col_array->getData());
    if (!col_nested)
        return nullptr;

    auto col_res = result_type->createColumn();
    ColVecType * col_res_vec = typeid_cast<ColVecType *>(col_res.get());
    if (!col_res_vec)
        return nullptr;

    ArrayElementNumImpl<DataType, mode>::template vector<IndexType>(
        col_nested->getData(), col_array->getOffsets(), indices, col_res_vec->getData(), builder);

    return col_res;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeStringConst(
    const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    auto col_res = ColumnString::create();

    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
    {
        ArrayElementStringImpl<mode>::template vectorConst<false>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            index.safeGet<UInt64>() - 1,
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    }
    else if (index.getType() == Field::Types::Int64)
    {
        ArrayElementStringImpl<mode>::template vectorConst<true>(
            col_nested->getChars(),
            col_array->getOffsets(),
            col_nested->getOffsets(),
            -(UInt64(index.safeGet<Int64>()) + 1),
            col_res->getChars(),
            col_res->getOffsets(),
            builder);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal type of array index");

    return col_res;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeArrayStringConst(
    const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnArray * col_nested_array = checkAndGetColumn<ColumnArray>(&col_array->getData());
    if (!col_nested_array)
        return nullptr;

    const ColumnString * col_nested_elem = nullptr;
    const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col_nested_array->getDataPtr().get());
    if (!col_nullable)
        col_nested_elem = checkAndGetColumn<ColumnString>(col_nested_array->getDataPtr().get());
    else
        col_nested_elem = checkAndGetColumn<ColumnString>(col_nullable->getNestedColumnPtr().get());

    if (!col_nested_elem)
        return nullptr;

    const auto * string_null_map = col_nullable ? &col_nullable->getNullMapColumn() : nullptr;
    auto res_string = ColumnString::create();
    auto res_offsets = ColumnArray::ColumnOffsets::create();
    auto res_string_null_map = col_nullable ? ColumnUInt8::create() : nullptr;
    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
    {
        if (col_nullable)
            ArrayElementArrayStringImpl<mode>::template vectorConst<false, true>(
                col_nested_elem->getChars(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                col_nested_elem->getOffsets(),
                &string_null_map->getData(),
                index.safeGet<UInt64>() - 1,
                res_string->getChars(),
                res_offsets->getData(),
                res_string->getOffsets(),
                &res_string_null_map->getData(),
                builder);
        else
            ArrayElementArrayStringImpl<mode>::template vectorConst<false, false>(
                col_nested_elem->getChars(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                col_nested_elem->getOffsets(),
                nullptr,
                index.safeGet<UInt64>() - 1,
                res_string->getChars(),
                res_offsets->getData(),
                res_string->getOffsets(),
                nullptr,
                builder);
    }
    else if (index.getType() == Field::Types::Int64)
    {
        if (col_nullable)
            ArrayElementArrayStringImpl<mode>::template vectorConst<true, true>(
                col_nested_elem->getChars(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                col_nested_elem->getOffsets(),
                &string_null_map->getData(),
                -(UInt64(index.safeGet<Int64>()) + 1),
                res_string->getChars(),
                res_offsets->getData(),
                res_string->getOffsets(),
                &res_string_null_map->getData(),
                builder);
        else
            ArrayElementArrayStringImpl<mode>::template vectorConst<true, false>(
                col_nested_elem->getChars(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                col_nested_elem->getOffsets(),
                nullptr,
                -(UInt64(index.safeGet<Int64>()) + 1),
                res_string->getChars(),
                res_offsets->getData(),
                res_string->getOffsets(),
                nullptr,
                builder);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal type of array index");

    if (col_nullable)
        return ColumnArray::create(ColumnNullable::create(std::move(res_string), std::move(res_string_null_map)), std::move(res_offsets));

    return ColumnArray::create(std::move(res_string), std::move(res_offsets));
}

template <ArrayElementExceptionMode mode>
template <typename DataType>
ColumnPtr FunctionArrayElement<mode>::executeArrayNumberConst(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    const Field & index,
    ArrayImpl::NullMapBuilder<mode> & builder)
{
    using ColVecType = ColumnVectorOrDecimal<DataType>;

    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnArray * col_nested_array = checkAndGetColumn<ColumnArray>(col_array->getDataPtr().get());
    if (!col_nested_array)
        return nullptr;

    const ColVecType * col_nested_elem = nullptr;
    const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col_nested_array->getDataPtr().get());
    if (!col_nullable)
        col_nested_elem = checkAndGetColumn<ColVecType>(&col_nested_array->getData());
    else
        col_nested_elem = checkAndGetColumn<ColVecType>(col_nullable->getNestedColumnPtr().get());

    if (!col_nested_elem)
        return nullptr;

    auto res = result_type->createColumn();
    ColumnArray * res_array = typeid_cast<ColumnArray *>(res.get());
    if (!res_array)
        return nullptr;

    ColVecType * res_data = nullptr;
    ColumnNullable * res_nullable = typeid_cast<ColumnNullable *>(&res_array->getData());
    if (!res_nullable)
        res_data = typeid_cast<ColVecType *>(&res_array->getData());
    else
        res_data = typeid_cast<ColVecType *>(&res_nullable->getNestedColumn());

    const NullMap * null_map = col_nullable ? &col_nullable->getNullMapData() : nullptr;
    auto & res_offsets = res_array->getOffsets();
    NullMap * res_null_map = res_nullable ? &res_nullable->getNullMapData() : nullptr;

    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
    {
        if (col_nullable)
            ArrayElementArrayNumImpl<DataType, mode>::template vectorConst<false, true>(
                col_nested_elem->getData(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                null_map,
                index.safeGet<UInt64>() - 1,
                res_data->getData(),
                res_offsets,
                res_null_map,
                builder);
        else
            ArrayElementArrayNumImpl<DataType, mode>::template vectorConst<false, false>(
                col_nested_elem->getData(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                null_map,
                index.safeGet<UInt64>() - 1,
                res_data->getData(),
                res_offsets,
                res_null_map,
                builder);
    }
    else if (index.getType() == Field::Types::Int64)
    {
        /// Cast to UInt64 before negation allows to avoid undefined behaviour for negation of the most negative number.
        /// NOTE: this would be undefined behaviour in C++ sense, but nevertheless, compiler cannot see it on user provided data,
        /// and generates the code that we want on supported CPU architectures (overflow in sense of two's complement arithmetic).
        /// This is only needed to avoid UBSan report.

        /// Negative array indices work this way:
        /// arr[-1] is the element at offset 0 from the last
        /// arr[-2] is the element at offset 1 from the last and so on.
        if (col_nullable)
            ArrayElementArrayNumImpl<DataType, mode>::template vectorConst<true, true>(
                col_nested_elem->getData(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                null_map,
                -(static_cast<UInt64>(index.safeGet<Int64>()) + 1),
                res_data->getData(),
                res_offsets,
                res_null_map,
                builder);
        else
            ArrayElementArrayNumImpl<DataType, mode>::template vectorConst<true, false>(
                col_nested_elem->getData(),
                col_array->getOffsets(),
                col_nested_array->getOffsets(),
                null_map,
                -(static_cast<UInt64>(index.safeGet<Int64>()) + 1),
                res_data->getData(),
                res_offsets,
                res_null_map,
                builder);
    }
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal type of array index");

    return res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType, typename DataType>
ColumnPtr FunctionArrayElement<mode>::executeArrayNumber(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder<mode> & builder)
{
    using ColVecType = ColumnVectorOrDecimal<DataType>;

    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnArray * col_nested_array = checkAndGetColumn<ColumnArray>(col_array->getDataPtr().get());
    if (!col_nested_array)
        return nullptr;

    const ColVecType * col_nested_elem = nullptr;
    const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col_nested_array->getDataPtr().get());
    if (!col_nullable)
        col_nested_elem = checkAndGetColumn<ColVecType>(&col_nested_array->getData());
    else
        col_nested_elem = checkAndGetColumn<ColVecType>(col_nullable->getNestedColumnPtr().get());

    if (!col_nested_elem)
        return nullptr;

    MutableColumnPtr res = result_type->createColumn();
    ColumnArray * res_array = typeid_cast<ColumnArray *>(res.get());
    if (!res_array)
        return nullptr;

    ColVecType * res_data = nullptr;
    ColumnNullable * res_nullable = typeid_cast<ColumnNullable *>(&res_array->getData());
    if (!res_nullable)
        res_data = typeid_cast<ColVecType *>(&res_array->getData());
    else
        res_data = typeid_cast<ColVecType *>(&res_nullable->getNestedColumn());

    const NullMap * null_map = col_nullable ? &col_nullable->getNullMapData() : nullptr;
    auto & res_offsets = res_array->getOffsets();
    NullMap * res_null_map = res_nullable ? &res_nullable->getNullMapData() : nullptr;

    if (col_nullable)
        ArrayElementArrayNumImpl<DataType, mode>::template vector<IndexType, true>(
            col_nested_elem->getData(),
            col_array->getOffsets(),
            col_nested_array->getOffsets(),
            null_map,
            indices,
            res_data->getData(),
            res_offsets,
            res_null_map,
            builder);
    else
        ArrayElementArrayNumImpl<DataType, mode>::template vector<IndexType, false>(
            col_nested_elem->getData(),
            col_array->getOffsets(),
            col_nested_array->getOffsets(),
            null_map,
            indices,
            res_data->getData(),
            res_offsets,
            res_null_map,
            builder);
    return res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
ColumnPtr FunctionArrayElement<mode>::executeString(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const ColumnString * col_nested = checkAndGetColumn<ColumnString>(&col_array->getData());

    if (!col_nested)
        return nullptr;

    auto col_res = ColumnString::create();

    ArrayElementStringImpl<mode>::template vector<IndexType>(
        col_nested->getChars(),
        col_array->getOffsets(),
        col_nested->getOffsets(),
        indices,
        col_res->getChars(),
        col_res->getOffsets(),
        builder);

    return col_res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
ColumnPtr FunctionArrayElement<mode>::executeArrayString(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnArray * col_nested_array = checkAndGetColumn<ColumnArray>(&col_array->getData());
    if (!col_nested_array)
        return nullptr;

    const ColumnString * col_nested_elem = nullptr;
    const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col_nested_array->getDataPtr().get());
    if (!col_nullable)
        col_nested_elem = checkAndGetColumn<ColumnString>(&col_nested_array->getData());
    else
        col_nested_elem = checkAndGetColumn<ColumnString>(col_nullable->getNestedColumnPtr().get());

    if (!col_nested_elem)
        return nullptr;

    const auto * string_null_map = col_nullable ? &col_nullable->getNullMapColumn() : nullptr;
    auto res_string = ColumnString::create();
    auto res_offsets = ColumnArray::ColumnOffsets::create();
    auto res_string_null_map = col_nullable ? ColumnUInt8::create() : nullptr;

    if (col_nullable)
        ArrayElementArrayStringImpl<mode>::template vector<IndexType, true>(
            col_nested_elem->getChars(),
            col_array->getOffsets(),
            col_nested_array->getOffsets(),
            col_nested_elem->getOffsets(),
            &string_null_map->getData(),
            indices,
            res_string->getChars(),
            res_offsets->getData(),
            res_string->getOffsets(),
            &res_string_null_map->getData(),
            builder);
    else
        ArrayElementArrayStringImpl<mode>::template vector<IndexType, false>(
            col_nested_elem->getChars(),
            col_array->getOffsets(),
            col_nested_array->getOffsets(),
            col_nested_elem->getOffsets(),
            nullptr,
            indices,
            res_string->getChars(),
            res_offsets->getData(),
            res_string->getOffsets(),
            nullptr,
            builder);

    if (col_nullable)
        return ColumnArray::create(ColumnNullable::create(std::move(res_string), std::move(res_string_null_map)), std::move(res_offsets));
    else
        return ColumnArray::create(std::move(res_string), std::move(res_offsets));
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeGenericConst(
    const ColumnsWithTypeAndName & arguments, const Field & index, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    if (index.getType() == Field::Types::UInt64 || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() >= 0))
        ArrayElementGenericImpl<mode>::template vectorConst<false>(
            col_nested, col_array->getOffsets(), index.safeGet<UInt64>() - 1, *col_res, builder);
    else if (index.getType() == Field::Types::Int64)
        ArrayElementGenericImpl<mode>::template vectorConst<true>(
            col_nested, col_array->getOffsets(), -(static_cast<UInt64>(index.safeGet<Int64>() + 1)), *col_res, builder);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal type of array index");

    return col_res;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::removeNullableIfNeeded(const ColumnPtr & column, const DataTypePtr & expect_type)
{
    const auto * nullable_column = checkAndGetColumn<ColumnNullable>(column.get());
    if (nullable_column && !expect_type->isNullable())
    {
        /// It happens when input argument is Array(Tuple(T1, T2, ...)) or Array(Map(K, V)) in function ArrayElementOrNull.
        /// e.g. arrayElementOrNull(Array(Tuple(T1, T2))) would be transformed into Tuple(ArrayElementOrNull(T1), ArrayElementOrNull(T2))
        /// The former return type Tuple(T1, T2) because Tuple can't be wrapped into Nullable. But the latter return type Tuple(Nullable(T1), Nullable(T2)), which is different.
        /// To keep return type consistency, we need to unwrap the Nullable column returned by ArrayElementOrNull(T1) and ArrayElementOrNull(T2)
        return nullable_column->getNestedColumnPtr();
    }

    return column;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
ColumnPtr FunctionArrayElement<mode>::executeGeneric(
    const ColumnsWithTypeAndName & arguments, const PaddedPODArray<IndexType> & indices, ArrayImpl::NullMapBuilder<mode> & builder)
{
    const ColumnArray * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    const auto & col_nested = col_array->getData();
    auto col_res = col_nested.cloneEmpty();

    ArrayElementGenericImpl<mode>::template vector<IndexType>(col_nested, col_array->getOffsets(), indices, *col_res, builder);

    return col_res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
ColumnPtr FunctionArrayElement<mode>::executeConst(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    const PaddedPODArray<IndexType> & indices,
    ArrayImpl::NullMapBuilder<mode> & builder,
    size_t input_rows_count)
{
    const ColumnArray * col_array = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());

    if (!col_array)
        return nullptr;

    auto res = result_type->createColumn();

    size_t rows = input_rows_count;
    const IColumn & array_elements = col_array->getData();
    size_t array_size = array_elements.size();

    for (size_t i = 0; i < rows; ++i)
    {
        IndexType index = indices[i];
        if (index > 0 && static_cast<size_t>(index) <= array_size)
        {
            size_t j = index - 1;
            res->insertFrom(array_elements, j);
            builder.update(j);
        }
        else if (index < 0 && -static_cast<size_t>(index) <= array_size)
        {
            size_t j = array_size + index;
            res->insertFrom(array_elements, j);
            builder.update(j);
        }
        else
        {
            res->insertDefault();
            builder.update();
        }
    }

    return res;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
ColumnPtr FunctionArrayElement<mode>::executeArgument(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    ArrayImpl::NullMapBuilder<mode> & builder,
    size_t input_rows_count) const
{
    auto index = checkAndGetColumn<ColumnVector<IndexType>>(arguments[1].column.get());
    if (!index)
        return nullptr;
    const auto & index_data = index->getData();

    if (builder)
        builder.initSink(index_data.size());

    ColumnPtr res;
    bool matched = tryEachType(
                       ElementTypesWithFastPath{},
                       [&]<typename T>() { return (res = executeNumber<IndexType, T>(arguments, result_type, index_data, builder)) != nullptr; })
        || (res = executeConst<IndexType>(arguments, result_type, index_data, builder, input_rows_count))
        || (res = executeString<IndexType>(arguments, index_data, builder))
        || tryEachType(
               ElementTypesWithFastPath{},
               [&]<typename T>() { return (res = executeArrayNumber<IndexType, T>(arguments, result_type, index_data, builder)) != nullptr; })
        || (res = executeArrayString<IndexType>(arguments, index_data, builder))
        || (res = executeGeneric<IndexType>(arguments, index_data, builder));

    if (!matched)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", arguments[0].column->getName(), getName());

    return res;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeMap2(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnMap * col_map = typeid_cast<const ColumnMap *>(&col_array->getData());
    if (!col_map)
        return nullptr;

    const ColumnArray * col_map_nested = &col_map->getNestedColumn();
    const ColumnTuple & col_map_kv = checkAndGetColumn<ColumnTuple>(*col_map_nested->getDataPtr());
    ColumnPtr col_map_keys = col_map_kv.getColumnPtr(0);
    ColumnPtr col_map_values = col_map_kv.getColumnPtr(1);

    const DataTypeMap & map_type
        = typeid_cast<const DataTypeMap &>(*typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType());
    const auto & key_type = map_type.getKeyType();
    const auto & value_type = map_type.getValueType();

    ColumnsWithTypeAndName temporary_results(2);
    temporary_results[1] = arguments[1];

    ColumnPtr result_key_column;
    /// Calculate the function for the keys of the map.
    {
        ColumnWithTypeAndName array_of_keys;
        array_of_keys.column
            = ColumnArray::create(ColumnArray::create(col_map_keys, col_map_nested->getOffsetsPtr()), col_array->getOffsetsPtr());
        array_of_keys.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(key_type));
        temporary_results[0] = std::move(array_of_keys);

        auto type = getReturnTypeImpl({temporary_results[0].type, temporary_results[1].type});
        auto col = executeImpl(temporary_results, type, input_rows_count);
        result_key_column = removeNullableIfNeeded(col, key_type);
    }

    /// Calculate the function for the values of the map
    ColumnPtr result_value_column;
    {
        ColumnWithTypeAndName array_of_values;
        array_of_values.column
            = ColumnArray::create(ColumnArray::create(col_map_values, col_map_nested->getOffsetsPtr()), col_array->getOffsetsPtr());
        array_of_values.type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeArray>(value_type));
        temporary_results[0] = std::move(array_of_values);

        auto type = getReturnTypeImpl({temporary_results[0].type, temporary_results[1].type});
        auto col = executeImpl(temporary_results, type, input_rows_count);
        result_value_column = removeNullableIfNeeded(col, value_type);
    }

    const auto & data_keys = typeid_cast<const ColumnArray &>(*result_key_column).getDataPtr();
    const auto & data_values = typeid_cast<const ColumnArray &>(*result_value_column).getDataPtr();
    const auto & offsets = typeid_cast<const ColumnArray &>(*result_key_column).getOffsetsPtr();
    auto result_nested_column = ColumnArray::create(ColumnTuple::create(Columns{data_keys, data_values}), offsets);
    return ColumnMap::create(std::move(result_nested_column));
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeTuple(const ColumnsWithTypeAndName & arguments, ArrayImpl::NullMapBuilder<mode> & builder, size_t input_rows_count) const
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnTuple * col_nested = typeid_cast<const ColumnTuple *>(&col_array->getData());
    if (!col_nested)
        return nullptr;

    if (builder)
    {
        const auto & offsets = col_array->getOffsets();
        chassert(offsets.size() == input_rows_count);
        if (input_rows_count != 0)
            chassert(col_nested->size() == offsets.back());

        builder.initSink(input_rows_count);

        const IColumn * index_col = arguments[1].column.get();

        /// Non-const numeric index vector (Int*/UInt*) path
        auto fill_null_map_for_vector = [&](const auto * index_column) -> bool
        {
            if (!index_column)
                return false;

            using IndexColumn = std::decay_t<decltype(*index_column)>;
            using IndexType = typename IndexColumn::ValueType;

            const auto & indices = index_column->getData();
            chassert(indices.size() == input_rows_count);

            ColumnArray::Offset current_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const size_t array_size = offsets[row] - current_offset;
                const IndexType idx = indices[row];

                bool in_range = false;
                UInt64 zero_based = 0;

                if (idx > 0 && static_cast<UInt64>(idx) <= array_size)
                {
                    zero_based = static_cast<UInt64>(idx) - 1; /// 1-based to 0-based
                    in_range = true;
                }
                else if constexpr (std::is_signed_v<IndexType>)
                {
                    if (idx < 0 && -static_cast<UInt64>(idx) <= array_size)
                    {
                        /// Negative index from the end: arr[-1] is last element
                        zero_based = static_cast<UInt64>(array_size + idx);
                        in_range = true;
                    }
                }

                if (in_range)
                {
                    const size_t j = current_offset + zero_based; /// nested offset
                    chassert(j < col_nested->size());
                    builder.update(j);
                }
                else
                {
                    builder.update();
                }

                current_offset = offsets[row];
            }

            return true;
        };

        if (!isColumnConst(*index_col))
        {
            if (!(fill_null_map_for_vector(checkAndGetColumn<ColumnVector<Int8>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<Int16>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<Int32>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<Int64>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<UInt8>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<UInt16>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<UInt32>>(index_col))
                  || fill_null_map_for_vector(checkAndGetColumn<ColumnVector<UInt64>>(index_col))))
            {
                /// Index is not a plain integer vector; generic path will throw handle it
                return nullptr;
            }
        }
        else
        {
            /// Constant numeric index
            Field index_field = (*index_col)[0];

            /// All sizes of shorter widths are also covered because `Field` promotes them to Int64/UInt64
            if (index_field.getType() != Field::Types::UInt64 && index_field.getType() != Field::Types::Int64)
                return nullptr;

            Int64 idx = 0;
            if (index_field.getType() == Field::Types::UInt64)
                idx = static_cast<Int64>(index_field.safeGet<UInt64>());
            else
                idx = index_field.safeGet<Int64>();

            if constexpr (!is_null_mode)
            {
                /// Preserve historical behaviour
                if (idx == 0)
                    throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");
            }

            ColumnArray::Offset current_offset = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                const size_t array_size = offsets[row] - current_offset;

                bool in_range = false;
                UInt64 zero_based = 0;

                if (idx > 0 && static_cast<UInt64>(idx) <= array_size)
                {
                    zero_based = static_cast<UInt64>(idx) - 1;
                    in_range = true;
                }
                else if (idx < 0 && -static_cast<UInt64>(idx) <= array_size)
                {
                    zero_based = static_cast<UInt64>(array_size + idx); /// idx < 0
                    in_range = true;
                }

                if (in_range)
                {
                    const size_t j = current_offset + zero_based;
                    chassert(j < col_nested->size());
                    builder.update(j);
                }
                else
                {
                    builder.update();
                }

                current_offset = offsets[row];
            }
        }
    }

    const auto & tuple_columns = col_nested->getColumns();
    size_t tuple_size = tuple_columns.size();

    if (tuple_size == 0)
        return ColumnTuple::create(input_rows_count);

    const DataTypes & tuple_types
        = typeid_cast<const DataTypeTuple &>(*typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType()).getElements();

    /** We will calculate the function for the tuple of the internals of the array.
      * To do this, create a temporary columns.
      * It will consist of the following columns
      * - the index of the array to be taken;
      * - an array of the first elements of the tuples;
      * - the result of taking the elements by the index for an array of the first elements of the tuples;
      * - array of the second elements of the tuples;
      * - result of taking elements by index for an array of second elements of tuples;
      * ...
      */
    ColumnsWithTypeAndName temporary_results(2);
    temporary_results[1] = arguments[1];

    /// results of taking elements by index for arrays from each element of the tuples;
    Columns result_tuple_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i)
    {
        ColumnWithTypeAndName array_of_tuple_section;
        array_of_tuple_section.column = ColumnArray::create(tuple_columns[i], col_array->getOffsetsPtr());
        array_of_tuple_section.type = std::make_shared<DataTypeArray>(tuple_types[i]);
        temporary_results[0] = array_of_tuple_section;

        auto type = getReturnTypeImpl({temporary_results[0].type, temporary_results[1].type});
        auto col = executeImpl(temporary_results, type, input_rows_count);
        result_tuple_columns[i] = removeNullableIfNeeded(col, tuple_types[i]);
    }

    return ColumnTuple::create(result_tuple_columns);
}

namespace
{

template <typename DataColumn, typename IndexColumn>
struct MatcherString
{
    const DataColumn & data;
    const IndexColumn & index;

    bool match(size_t row_data, size_t row_index) const
    {
        auto data_ref = data.getDataAt(row_data);
        auto index_ref = index.getDataAt(row_index);
        return memequalSmallAllowOverflow15(index_ref.data(), index_ref.size(), data_ref.data(), data_ref.size());
    }
};

template <typename DataColumn>
struct MatcherStringConst
{
    const DataColumn & data;
    const String & index;

    bool match(size_t row_data, size_t /* row_index */) const
    {
        auto data_ref = data.getDataAt(row_data);
        return index.size() == data_ref.size() && memcmp(index.data(), data_ref.data(), data_ref.size()) == 0;
    }
};

template <typename DataType, typename IndexType>
struct MatcherNumber
{
    const PaddedPODArray<DataType> & data;
    const PaddedPODArray<IndexType> & index;

    bool match(size_t row_data, size_t row_index) const { return data[row_data] == static_cast<DataType>(index[row_index]); }
};

template <typename DataType>
struct MatcherNumberConst
{
    const PaddedPODArray<DataType> & data;
    DataType index;

    bool match(size_t row_data, size_t /* row_index */) const { return data[row_data] == index; }
};

}

template <ArrayElementExceptionMode mode>
template <typename Matcher>
void FunctionArrayElement<mode>::executeMatchKeyToIndex(
    const Offsets & offsets, PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher)
{
    size_t rows = offsets.size();

    /// `m[key]` returns the value of the FIRST occurrence of the key in the row, so each
    /// row is scanned left to right and the first match is taken (index encoded as
    /// position + 1, with 0 meaning "not found"). Duplicate keys in a Map are a legal
    /// (if degenerate) state, so a cross-row position-prediction shortcut is not used: it
    /// could accept a later duplicate at the predicted offset while an earlier occurrence
    /// exists, yielding a value that depends on the preceding rows in the block (see issue
    /// #111203). Ruling out an earlier duplicate still requires scanning from the start,
    /// so there is no correct constant-time shortcut to prefer over the scan.
    for (size_t i = 0; i < rows; ++i)
    {
        const auto & begin = offsets[ssize_t(i) - 1];
        const auto & end = offsets[i];
        bool matched = false;
        for (size_t j = begin; j < end; ++j)
        {
            if (matcher.match(j, i))
            {
                matched_idxs.push_back(j - begin + 1);
                matched = true;
                break;
            }
        }

        if (!matched)
            matched_idxs.push_back(0);
    }
}

template <ArrayElementExceptionMode mode>
template <typename Matcher>
void FunctionArrayElement<mode>::executeMatchConstKeyToIndex(
    size_t num_rows, size_t num_values, PaddedPODArray<UInt64> & matched_idxs, const Matcher & matcher)
{
    for (size_t i = 0; i < num_rows; ++i)
    {
        bool matched = false;
        for (size_t j = 0; j < num_values; ++j)
        {
            if (matcher.match(j, i))
            {
                matched_idxs.push_back(j + 1);
                matched = true;
                break;
            }
        }

        if (!matched)
            matched_idxs.push_back(0);
    }
}

template <typename F>
bool castColumnString(const IColumn * column, F && f)
{
    return castTypeToEither<ColumnString, ColumnFixedString>(column, std::forward<F>(f));
}

bool isStringOrFixedStringColumn(const IColumn & column)
{
    return typeid_cast<const ColumnString *>(&column) || typeid_cast<const ColumnFixedString *>(&column);
}

template <ArrayElementExceptionMode mode>
bool FunctionArrayElement<mode>::matchKeyToIndexStringConst(
    const IColumn & data, const Offsets & offsets, const Field & index, PaddedPODArray<UInt64> & matched_idxs)
{
    if (index.getType() != Field::Types::String)
        return false;

    /// The dictionary lookup below is defined only for String and FixedString keys. For other
    /// LowCardinality key types, fall through so that the regular dispatch reports the type error
    /// instead of silently finding no match.
    const auto * low_cardinality_data = typeid_cast<const ColumnLowCardinality *>(&data);
    if (low_cardinality_data
        && isStringOrFixedStringColumn(*low_cardinality_data->getDictionary().getNestedNotNullableColumn()))
    {
        const auto & requested_key = index.safeGet<String>();
        auto dictionary_index = low_cardinality_data->getDictionary().getOrFindValueIndex(requested_key);
        matched_idxs.reserve(offsets.size());

        if (!dictionary_index)
        {
            matched_idxs.resize_fill(offsets.size());
            return true;
        }

        struct MatcherLowCardinalityStringConst
        {
            const ColumnLowCardinality & data;
            UInt64 dictionary_index;

            bool match(size_t row_data, size_t /* row_index */) const
            {
                return data.getIndexAt(row_data) == dictionary_index;
            }
        };

        MatcherLowCardinalityStringConst matcher{*low_cardinality_data, *dictionary_index};
        executeMatchKeyToIndex(offsets, matched_idxs, matcher);
        return true;
    }

    return castColumnString(
        &data,
        [&](const auto & data_column)
        {
            using DataColumn = std::decay_t<decltype(data_column)>;
            MatcherStringConst<DataColumn> matcher{data_column, index.safeGet<String>()};
            executeMatchKeyToIndex(offsets, matched_idxs, matcher);
            return true;
        });
}

template <ArrayElementExceptionMode mode>
bool FunctionArrayElement<mode>::matchKeyToIndexString(
    const IColumn & data, const Offsets & offsets, bool is_key_const, const IColumn & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnString(
        &data,
        [&](const auto & data_column)
        {
            return castColumnString(
                &index,
                [&](const auto & index_column)
                {
                    using DataColumn = std::decay_t<decltype(data_column)>;
                    using IndexColumn = std::decay_t<decltype(index_column)>;

                    MatcherString<DataColumn, IndexColumn> matcher{data_column, index_column};
                    if (is_key_const)
                        executeMatchConstKeyToIndex(index.size(), data.size(), matched_idxs, matcher);
                    else
                        executeMatchKeyToIndex(offsets, matched_idxs, matcher);

                    return true;
                });
        });
}

template <typename FromType, typename ToType>
constexpr bool areConvertibleTypes
    = std::is_same_v<FromType, ToType> || (is_integer<FromType> && is_integer<ToType> && std::is_convertible_v<FromType, ToType>);

template <typename F>
bool castColumnNumeric(const IColumn * column, F && f)
{
    return castTypeToEither<
        ColumnVector<UInt8>,
        ColumnVector<UInt16>,
        ColumnVector<UInt32>,
        ColumnVector<UInt64>,
        ColumnVector<UInt128>,
        ColumnVector<UInt256>,
        ColumnVector<Int8>,
        ColumnVector<Int16>,
        ColumnVector<Int32>,
        ColumnVector<Int64>,
        ColumnVector<Int128>,
        ColumnVector<Int256>,
        ColumnVector<UUID>,
        ColumnVector<IPv4>,
        ColumnVector<IPv6>>(column, std::forward<F>(f));
}

template <ArrayElementExceptionMode mode>
bool FunctionArrayElement<mode>::matchKeyToIndexNumberConst(
    const IColumn & data, const Offsets & offsets, const Field & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnNumeric(
        &data,
        [&](const auto & data_column)
        {
            using DataType = typename std::decay_t<decltype(data_column)>::ValueType;
            std::optional<DataType> index_as_integer;

            Field::dispatch(
                [&](const auto & value)
                {
                    using FieldType = std::decay_t<decltype(value)>;
                    if constexpr (areConvertibleTypes<FieldType, DataType>)
                        index_as_integer = static_cast<DataType>(value);
                },
                index);

            if (!index_as_integer)
                return false;

            MatcherNumberConst<DataType> matcher{data_column.getData(), *index_as_integer};
            executeMatchKeyToIndex(offsets, matched_idxs, matcher);
            return true;
        });
}

template <ArrayElementExceptionMode mode>
bool FunctionArrayElement<mode>::matchKeyToIndexNumber(
    const IColumn & data, const Offsets & offsets, bool is_key_const, const IColumn & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnNumeric(
        &data,
        [&](const auto & data_column)
        {
            return castColumnNumeric(
                &index,
                [&](const auto & index_column)
                {
                    using DataType = typename std::decay_t<decltype(data_column)>::ValueType;
                    using IndexType = typename std::decay_t<decltype(index_column)>::ValueType;

                    if constexpr (areConvertibleTypes<IndexType, DataType>)
                    {
                        MatcherNumber<DataType, IndexType> matcher{data_column.getData(), index_column.getData()};
                        if (is_key_const)
                            executeMatchConstKeyToIndex(index_column.size(), data_column.size(), matched_idxs, matcher);
                        else
                            executeMatchKeyToIndex(offsets, matched_idxs, matcher);

                        return true;
                    }

                    return false;
                });
        });
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeMap(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
    const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());
    chassert(col_map || col_const_map);

    if (col_const_map)
        col_map = typeid_cast<const ColumnMap *>(&col_const_map->getDataColumn());

    const auto & nested_column = col_map->getNestedColumn();
    const auto & keys_data = col_map->getNestedData().getColumn(0);
    const auto & values_data = col_map->getNestedData().getColumn(1);
    const auto & offsets = nested_column.getOffsets();

    const auto & type_map = assert_cast<const DataTypeMap &>(*arguments[0].type);

    /// A map with Enum keys can be indexed by the name of an enum value, e.g. `m['name']`.
    /// Cast the index to the key type, so it is matched by the numeric value of the enum.
    ColumnPtr index_column = arguments[1].column;
    if (isEnum(type_map.getKeyType()) && isStringOrFixedString(removeLowCardinality(arguments[1].type)))
        index_column = castColumn(arguments[1], type_map.getKeyType());

    /// At first step calculate indices in array of values for requested keys.
    auto indices_column = DataTypeNumber<UInt64>().createColumn();
    indices_column->reserve(input_rows_count);
    auto & indices_data = assert_cast<ColumnVector<UInt64> &>(*indices_column).getData();

    bool executed = false;
    if (!isColumnConst(*index_column))
    {
        executed = matchKeyToIndexNumber(keys_data, offsets, !!col_const_map, *index_column, indices_data)
            || matchKeyToIndexString(keys_data, offsets, !!col_const_map, *index_column, indices_data);
    }
    else
    {
        Field index = (*index_column)[0];
        executed = matchKeyToIndexNumberConst(keys_data, offsets, index, indices_data)
            || matchKeyToIndexStringConst(keys_data, offsets, index, indices_data);
    }

    if (!executed)
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal types of arguments: {}, {} for function {}",
            arguments[0].type->getName(),
            arguments[1].type->getName(),
            getName());

    ColumnPtr values_array = ColumnArray::create(values_data.getPtr(), nested_column.getOffsetsPtr());
    if (col_const_map)
        values_array = ColumnConst::create(values_array, input_rows_count);

    /// Prepare arguments to call arrayElement for array with values and calculated indices at previous step.
    ColumnsWithTypeAndName new_arguments
        = {{values_array, std::make_shared<DataTypeArray>(type_map.getValueType()), ""},
           {std::move(indices_column), std::make_shared<DataTypeNumber<UInt64>>(), ""}};

    return executeImpl(new_arguments, result_type, input_rows_count);
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeWithArrayIndex(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const auto * result_array_type = checkAndGetDataType<DataTypeArray>(result_type.get());
    chassert(result_array_type);
    const auto & result_element_type = result_array_type->getNestedType();

    const ColumnArray * col_data_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    const ColumnArray * col_data_array_const = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());
    if (!col_data_array && !col_data_array_const)
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of first argument of function {}",
            arguments[0].column->getName(),
            getName());

    const ColumnArray & data_array = col_data_array ? *col_data_array : *col_data_array_const;
    bool is_data_const = (col_data_array_const != nullptr);

    const ColumnArray * col_index_array = checkAndGetColumn<ColumnArray>(arguments[1].column.get());
    ColumnPtr materialized_index;
    if (!col_index_array)
    {
        materialized_index = arguments[1].column->convertToFullColumnIfConst();
        col_index_array = checkAndGetColumn<ColumnArray>(materialized_index.get());
        if (!col_index_array)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of second argument of function {}",
                arguments[1].column->getName(),
                getName());
    }

    /// The result element type has `LowCardinality` removed, just like for the scalar index,
    /// so materialize the dictionary before copying elements out of it.
    ColumnPtr data_column_holder = recursiveRemoveLowCardinality(data_array.getDataPtr());
    const IColumn & data_col = *data_column_holder;
    const auto & data_offsets = data_array.getOffsets();
    const auto & index_offsets = col_index_array->getOffsets();

    /// The index elements may be `LowCardinality` and/or `Nullable`, just like a scalar index.
    /// A `NULL` index behaves exactly like index `0`: the scalar form returns `NULL` when the
    /// result can be nullable, and the default value otherwise.
    ColumnPtr index_column_holder = recursiveRemoveLowCardinality(col_index_array->getDataPtr());
    const ColumnNullable * nullable_index = checkAndGetColumn<ColumnNullable>(index_column_holder.get());
    const IColumn & index_data_col = nullable_index ? nullable_index->getNestedColumn() : *index_column_holder;
    const NullMap * index_null_map = nullable_index ? &nullable_index->getNullMapData() : nullptr;

    const ColumnNullable * nullable_data = checkAndGetColumn<ColumnNullable>(&data_col);
    const IColumn & inner_data = nullable_data ? nullable_data->getNestedColumn() : data_col;
    const NullMap * source_null_map = nullable_data ? &nullable_data->getNullMapData() : nullptr;

    /// For const source, every row uses the same single array (offset 0..data_offsets[0])
    const size_t const_array_size = is_data_const ? data_offsets[0] : 0;

    bool result_is_nullable = result_element_type->isNullable();

    /// An out-of-range index yields the default value of the element type, which is `NULL` only when
    /// the element type is nullable on its own -- in `arrayElementOrNull` mode or for a nullable
    /// source element type. A nullable *index* element type must not turn an out-of-range index into
    /// `NULL`, exactly like the scalar form: `[10, 20, 30][toNullable(5)]` is `0`, not `NULL`.
    bool out_of_bounds_is_null = result_is_nullable && (is_null_mode || nullable_data != nullptr);

    size_t total_indices = input_rows_count ? index_offsets[input_rows_count - 1] : 0;

    /// Result offsets are identical to index offsets
    auto result_offsets_col = ColumnArray::ColumnOffsets::create();
    auto & result_offsets = result_offsets_col->getData();
    result_offsets.assign(index_offsets.begin(), index_offsets.begin() + input_rows_count);

    /// Index resolution: converts 1-based/negative index to 0-based offset within the row's array slice.
    /// Returns array_size (sentinel) for out-of-bounds.
    auto resolve_index = []<typename IndexType>(IndexType idx, size_t array_size) -> size_t
    {
        if constexpr (std::is_signed_v<IndexType>)
        {
            if (idx > 0 && static_cast<size_t>(idx) <= array_size)
                return static_cast<size_t>(idx) - 1;
            if (idx < 0 && -static_cast<size_t>(idx) <= array_size)
                return array_size - (-static_cast<size_t>(idx));
        }
        else
        {
            if (idx > 0 && static_cast<size_t>(idx) <= array_size)
                return static_cast<size_t>(idx) - 1;
        }
        return array_size;
    };

    /// Try numeric fast paths: direct PODArray access, no virtual calls
    ColumnPtr fast_result_data;
    auto try_numeric = [&](const auto * col_numeric) -> bool
    {
        if (!col_numeric)
            return false;

        using ColVecType = std::decay_t<decltype(*col_numeric)>;
        using DataType = typename ColVecType::ValueType;

        const auto & src_data = col_numeric->getData();
        typename ColVecType::MutablePtr result_col;
        if constexpr (is_decimal<DataType>)
            result_col = ColVecType::create(0, col_numeric->getScale());
        else
            result_col = ColVecType::create();
        auto & result_vec = result_col->getData();
        result_vec.resize(total_indices);

        NullMap * result_null_map = nullptr;
        MutableColumnPtr null_map_holder;
        if (result_is_nullable)
        {
            null_map_holder = ColumnUInt8::create(total_indices, UInt8(0));
            result_null_map = &assert_cast<ColumnUInt8 &>(*null_map_holder).getData();
        }

        auto fill = [&]<typename IndexType>(const PaddedPODArray<IndexType> & indices)
        {
            size_t out = 0;
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                size_t data_start = is_data_const ? 0 : (row > 0 ? data_offsets[row - 1] : 0);
                size_t array_size = is_data_const ? const_array_size : (data_offsets[row] - data_start);
                size_t idx_start = row > 0 ? index_offsets[row - 1] : 0;
                size_t idx_end = index_offsets[row];

                for (size_t k = idx_start; k < idx_end; ++k, ++out)
                {
                    if (index_null_map && (*index_null_map)[k])
                    {
                        result_vec[out] = DataType();
                        if (result_null_map)
                            (*result_null_map)[out] = UInt8(1);
                        continue;
                    }

                    size_t resolved = resolve_index(indices[k], array_size);
                    if (resolved < array_size)
                    {
                        size_t source_pos = data_start + resolved;
                        if (source_null_map && (*source_null_map)[source_pos])
                        {
                            result_vec[out] = DataType();
                            if (result_null_map)
                                (*result_null_map)[out] = UInt8(1);
                        }
                        else
                        {
                            result_vec[out] = src_data[source_pos];
                        }
                    }
                    else
                    {
                        result_vec[out] = DataType();
                        if (result_null_map && out_of_bounds_is_null)
                            (*result_null_map)[out] = UInt8(1);
                    }
                }
            }
        };

        auto dispatch_fill = [&](const auto * idx_col) -> bool
        {
            if (!idx_col)
                return false;
            fill(idx_col->getData());
            return true;
        };

        if (!dispatch_fill(checkAndGetColumn<ColumnVector<UInt8>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<UInt16>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<UInt32>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<UInt64>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<Int8>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<Int16>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<Int32>>(&index_data_col))
            && !dispatch_fill(checkAndGetColumn<ColumnVector<Int64>>(&index_data_col)))
            return false;

        if (null_map_holder)
            fast_result_data = ColumnNullable::create(std::move(result_col), std::move(null_map_holder));
        else
            fast_result_data = std::move(result_col);
        return true;
    };

    if (try_numeric(checkAndGetColumn<ColumnVector<UInt8>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UInt16>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UInt32>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UInt64>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UInt128>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UInt256>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int8>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int16>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int32>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int64>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int128>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Int256>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Float32>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<Float64>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<UUID>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<IPv4>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnVector<IPv6>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnDecimal<Decimal32>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnDecimal<Decimal64>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnDecimal<Decimal128>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnDecimal<Decimal256>>(&inner_data))
        || try_numeric(checkAndGetColumn<ColumnDecimal<DateTime64>>(&inner_data)))
    {
        return ColumnArray::create(fast_result_data, std::move(result_offsets_col));
    }

    /// Generic fallback path using insertFrom (handles String, Array, Tuple, etc.)
    auto result_nested_col = removeNullable(result_element_type)->createColumn();
    result_nested_col->reserve(total_indices);

    NullMap * result_null_map = nullptr;
    MutableColumnPtr null_map_holder;
    if (result_is_nullable)
    {
        null_map_holder = ColumnUInt8::create(total_indices, UInt8(0));
        result_null_map = &assert_cast<ColumnUInt8 &>(*null_map_holder).getData();
    }

    auto generic_process = [&]<typename IndexType>(const PaddedPODArray<IndexType> & indices)
    {
        size_t out = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            size_t data_start = is_data_const ? 0 : (row > 0 ? data_offsets[row - 1] : 0);
            size_t array_size = is_data_const ? const_array_size : (data_offsets[row] - data_start);
            size_t idx_start = row > 0 ? index_offsets[row - 1] : 0;
            size_t idx_end = index_offsets[row];

            for (size_t k = idx_start; k < idx_end; ++k, ++out)
            {
                if (index_null_map && (*index_null_map)[k])
                {
                    result_nested_col->insertDefault();
                    if (result_null_map)
                        (*result_null_map)[out] = UInt8(1);
                    continue;
                }

                size_t resolved = resolve_index(indices[k], array_size);
                if (resolved < array_size)
                {
                    size_t source_pos = data_start + resolved;
                    if (source_null_map && (*source_null_map)[source_pos])
                    {
                        result_nested_col->insertDefault();
                        if (result_null_map)
                            (*result_null_map)[out] = UInt8(1);
                    }
                    else
                    {
                        result_nested_col->insertFrom(inner_data, source_pos);
                    }
                }
                else
                {
                    result_nested_col->insertDefault();
                    if (result_null_map && out_of_bounds_is_null)
                        (*result_null_map)[out] = UInt8(1);
                }
            }
        }
    };

    auto try_dispatch_generic = [&](const auto * col) -> bool
    {
        if (!col)
            return false;
        generic_process(col->getData());
        return true;
    };

    if (!try_dispatch_generic(checkAndGetColumn<ColumnVector<UInt8>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<UInt16>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<UInt32>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<UInt64>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<Int8>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<Int16>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<Int32>>(&index_data_col))
        && !try_dispatch_generic(checkAndGetColumn<ColumnVector<Int64>>(&index_data_col)))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of second argument of function {}",
            arguments[1].column->getName(),
            getName());
    }

    ColumnPtr result_data;
    if (null_map_holder)
        result_data = ColumnNullable::create(std::move(result_nested_col), std::move(null_map_holder));
    else
        result_data = std::move(result_nested_col);

    return ColumnArray::create(result_data, std::move(result_offsets_col));
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeQBit(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    const auto & qbit_type = assert_cast<const DataTypeQBit &>(*removeNullable(arguments[0].type));

    switch (qbit_type.getElementType()->getTypeId())
    {
        case TypeIndex::Int8:
            return executeQBitImpl<Int8>(arguments, input_rows_count);
        case TypeIndex::BFloat16:
            return executeQBitImpl<BFloat16>(arguments, input_rows_count);
        case TypeIndex::Float32:
            return executeQBitImpl<Float32>(arguments, input_rows_count);
        case TypeIndex::Float64:
            return executeQBitImpl<Float64>(arguments, input_rows_count);
        default:
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Unexpected QBit element type {} in function {}", qbit_type.getElementType()->getName(), getName());
    }
}

template <ArrayElementExceptionMode mode>
template <typename T>
ColumnPtr FunctionArrayElement<mode>::executeQBitWithArrayIndex(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    using Word = std::conditional_t<
        sizeof(T) == 1,
        uint8_t,
        std::conditional_t<sizeof(T) == 2, UInt16, std::conditional_t<sizeof(T) == 4, UInt32, UInt64>>>;

    const auto & qbit_type = assert_cast<const DataTypeQBit &>(*removeNullable(arguments[0].type));
    const size_t dimension = qbit_type.getDimension();
    const size_t stride = qbit_type.getStride();
    const size_t element_size = qbit_type.getElementSize();
    const size_t bytes_per_group = DataTypeQBit::bitsToBytes(stride);

    /// A constant nullable source arrives as `ColumnConst(ColumnNullable(ColumnQBit))`, so the constant
    /// has to be peeled before the null map is looked for.
    const bool qbit_is_const = isColumnConst(*arguments[0].column);
    const IColumn & unwrapped_qbit
        = qbit_is_const ? assert_cast<const ColumnConst &>(*arguments[0].column).getDataColumn() : *arguments[0].column;
    const auto * nullable_qbit = checkAndGetColumn<ColumnNullable>(&unwrapped_qbit);
    const auto * source_null_map = nullable_qbit ? &nullable_qbit->getNullMapData() : nullptr;
    const auto & qbit_col = assert_cast<const ColumnQBit &>(nullable_qbit ? nullable_qbit->getNestedColumn() : unwrapped_qbit);
    const auto & tuple = qbit_col.getNestedData();

    /// The null map of a constant source holds a single row.
    auto source_is_null = [&](size_t row) { return source_null_map && (*source_null_map)[qbit_is_const ? 0 : row]; };

    const ColumnArray * index_array = checkAndGetColumn<ColumnArray>(arguments[1].column.get());
    ColumnPtr materialized_index;
    if (!index_array)
    {
        materialized_index = arguments[1].column->convertToFullColumnIfConst();
        index_array = checkAndGetColumn<ColumnArray>(materialized_index.get());
    }
    if (!index_array)
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", arguments[1].column->getName(), getName());

    ColumnPtr index_holder = recursiveRemoveLowCardinality(index_array->getDataPtr());
    const auto * nullable_index = checkAndGetColumn<ColumnNullable>(index_holder.get());
    const IColumn & index_data = nullable_index ? nullable_index->getNestedColumn() : *index_holder;
    const NullMap * index_null_map = nullable_index ? &nullable_index->getNullMapData() : nullptr;
    const auto & offsets = index_array->getOffsets();
    const size_t total_indices = input_rows_count ? offsets[input_rows_count - 1] : 0;

    auto result = ColumnVector<T>::create(total_indices);
    auto & result_data = result->getData();
    memset(result_data.data(), 0, total_indices * sizeof(T));

    const bool result_is_nullable = is_null_mode || nullable_index || source_null_map;
    ColumnUInt8::MutablePtr null_map;
    if (result_is_nullable)
        null_map = ColumnUInt8::create(total_indices, UInt8(0));

    auto plane_chars = [&](size_t group, size_t bit) -> const UInt8 *
    {
        return reinterpret_cast<const UInt8 *>(
            assert_cast<const ColumnFixedString &>(tuple.getColumn(group * element_size + bit)).getChars().data());
    };

    auto extract_into = [&](size_t output_row, size_t qbit_row, size_t element)
    {
        const size_t group = element / stride;
        const size_t within_group = element % stride;
        const size_t byte_offset = bytes_per_group - 1 - within_group / 8;
        const size_t bit_in_byte = within_group % 8;

        Word word = 0;
        for (size_t bit = 0; bit < element_size; ++bit)
        {
            const UInt8 byte = plane_chars(group, bit)[qbit_row * bytes_per_group + byte_offset];
            word |= static_cast<Word>(static_cast<Word>((byte >> bit_in_byte) & 1) << (element_size - 1 - bit));
        }
        result_data[output_row] = std::bit_cast<T>(word);
    };

    auto process = [&]<typename IndexType>(const PaddedPODArray<IndexType> & indices)
    {
        size_t output_row = 0;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            const size_t begin = row ? offsets[row - 1] : 0;
            for (size_t pos = begin; pos < offsets[row]; ++pos, ++output_row)
            {
                if (source_is_null(row))
                {
                    null_map->getData()[output_row] = 1;
                    continue;
                }

                if (index_null_map && (*index_null_map)[pos])
                {
                    if (null_map)
                        null_map->getData()[output_row] = 1;
                    continue;
                }

                std::optional<size_t> element;
                if constexpr (std::is_signed_v<IndexType>)
                {
                    if (indices[pos] > 0 && static_cast<UInt64>(indices[pos]) <= dimension)
                        element = static_cast<size_t>(indices[pos] - 1);
                    else if (indices[pos] < 0)
                    {
                        const UInt64 abs_index = UInt64(0) - static_cast<UInt64>(indices[pos]);
                        if (abs_index <= dimension)
                            element = dimension - static_cast<size_t>(abs_index);
                    }
                }
                else if (indices[pos] >= 1 && indices[pos] <= dimension)
                    element = static_cast<size_t>(indices[pos] - 1);

                if (element)
                    extract_into(output_row, qbit_is_const ? 0 : row, *element);
                else if constexpr (is_null_mode)
                    null_map->getData()[output_row] = 1;
            }
        }
    };

    auto dispatch = [&](const auto * column) -> bool
    {
        if (!column)
            return false;
        process(column->getData());
        return true;
    };

    if (!dispatch(checkAndGetColumn<ColumnVector<UInt8>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<UInt16>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<UInt32>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<UInt64>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<Int8>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<Int16>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<Int32>>(&index_data))
        && !dispatch(checkAndGetColumn<ColumnVector<Int64>>(&index_data)))
        throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of second argument of function {}", arguments[1].column->getName(), getName());

    auto result_offsets = ColumnArray::ColumnOffsets::create();
    result_offsets->getData().assign(offsets.begin(), offsets.begin() + input_rows_count);
    ColumnPtr result_data_column;
    if (null_map)
        result_data_column = ColumnNullable::create(std::move(result), std::move(null_map));
    else
        result_data_column = std::move(result);
    return ColumnArray::create(result_data_column, std::move(result_offsets));
}

/** A QBit stores each vector bit-transposed: tuple column `group * element_size + bit` is a FixedString bit plane
  * holding bit `bit` (MSB first) of the `stride` dimensions of stride group `group`. Within a plane each byte holds
  * one octet of 8 dimensions, high octets at low byte offsets, LSB-first within a byte (see
  * SerializationQBit::transposeBits). Reconstructing element `e` therefore reads exactly one bit from each of the
  * `element_size` planes of the single stride group containing `e`; the planes of all other stride groups are not
  * touched.
  *
  * Out-of-range indices follow the Array semantics: the default value (zero) for arrayElement, NULL for
  * arrayElementOrNull, and negative indices count from the end of the vector.
  */
template <ArrayElementExceptionMode mode>
template <typename T>
ColumnPtr FunctionArrayElement<mode>::executeQBitImpl(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    /// Note: the 8-bit word is `uint8_t` (not ClickHouse's `UInt8`, which is `char8_t`).
    using Word = std::conditional_t<
        sizeof(T) == 1,
        uint8_t,
        std::conditional_t<sizeof(T) == 2, UInt16, std::conditional_t<sizeof(T) == 4, UInt32, UInt64>>>;

    if (checkAndGetDataType<DataTypeArray>(arguments[1].type.get()))
        return executeQBitWithArrayIndex<T>(arguments, input_rows_count);

    const auto & qbit_type = assert_cast<const DataTypeQBit &>(*removeNullable(arguments[0].type));
    const size_t dimension = qbit_type.getDimension();
    const size_t stride = qbit_type.getStride();
    const size_t element_size = qbit_type.getElementSize();
    const size_t bytes_per_group = DataTypeQBit::bitsToBytes(stride);

    /// The QBit column stays constant when only the index is a full column (useDefaultImplementationForConstants
    /// unwraps constants only when every argument is constant). A constant nullable source arrives as
    /// `ColumnConst(ColumnNullable(ColumnQBit))`, so the constant has to be peeled before the null map is looked for.
    const bool qbit_is_const = isColumnConst(*arguments[0].column);
    const IColumn & unwrapped_qbit
        = qbit_is_const ? assert_cast<const ColumnConst &>(*arguments[0].column).getDataColumn() : *arguments[0].column;
    const auto * nullable_qbit = checkAndGetColumn<ColumnNullable>(&unwrapped_qbit);
    const auto * source_null_map = nullable_qbit ? &nullable_qbit->getNullMapData() : nullptr;
    const auto & qbit_col = assert_cast<const ColumnQBit &>(nullable_qbit ? nullable_qbit->getNestedColumn() : unwrapped_qbit);
    const auto & tuple = qbit_col.getNestedData();

    /// The null map of a constant source holds a single row.
    auto source_is_null = [&](size_t row) { return source_null_map && (*source_null_map)[qbit_is_const ? 0 : row]; };

    auto res = ColumnVector<T>::create(input_rows_count);
    auto & res_data = res->getData();
    /// Out-of-range rows keep the default (zero) value, so start from an all-zero buffer that bits are ORed into.
    memset(res_data.data(), 0, input_rows_count * sizeof(T));

    const bool index_is_nullable = arguments[1].type->isNullable();
    ColumnUInt8::MutablePtr null_map;
    if constexpr (is_null_mode)
        null_map = ColumnUInt8::create(input_rows_count, UInt8(0));
    else if (source_null_map || index_is_nullable)
        null_map = ColumnUInt8::create(input_rows_count, UInt8(0));

    auto plane_chars = [&](size_t group, size_t bit) -> const UInt8 *
    {
        return reinterpret_cast<const UInt8 *>(
            assert_cast<const ColumnFixedString &>(tuple.getColumn(group * element_size + bit)).getChars().data());
    };

    /// Resolve a 1-based index into a 0-based element position, or nullopt when out of range.
    auto resolve_signed = [dimension](Int64 index) -> std::optional<size_t>
    {
        if (index > 0 && static_cast<UInt64>(index) <= dimension)
            return static_cast<size_t>(index - 1);
        if (index < 0)
        {
            /// Compute |index| in the unsigned domain: -INT64_MIN does not fit in Int64.
            const UInt64 abs_index = UInt64(0) - static_cast<UInt64>(index);
            if (abs_index <= dimension)
                return dimension - static_cast<size_t>(abs_index);
        }
        return std::nullopt;
    };
    auto resolve_unsigned = [dimension](UInt64 index) -> std::optional<size_t>
    {
        if (index >= 1 && index <= dimension)
            return static_cast<size_t>(index - 1);
        return std::nullopt;
    };

    auto set_out_of_range = [&](size_t row)
    {
        if constexpr (is_null_mode)
            null_map->getData()[row] = 1;
        /// res_data[row] stays the default (zero).
    };

    auto extract_into = [&](size_t row, size_t element)
    {
        const size_t group = element / stride;
        const size_t within_group = element % stride;
        const size_t byte_offset = bytes_per_group - 1 - within_group / 8;
        const size_t bit_in_byte = within_group % 8;
        const size_t qbit_row = qbit_is_const ? 0 : row;

        Word word = 0;
        for (size_t bit = 0; bit < element_size; ++bit)
        {
            const UInt8 byte = plane_chars(group, bit)[qbit_row * bytes_per_group + byte_offset];
            word |= static_cast<Word>(static_cast<Word>((byte >> bit_in_byte) & 1) << (element_size - 1 - bit));
        }
        res_data[row] = std::bit_cast<T>(word);
    };

    const IColumn & index_column = *arguments[1].column;

    if (isColumnConst(index_column))
    {
        const Field index = index_column[0];

        if constexpr (!is_null_mode)
        {
            /// Mirror the Array behaviour: a constant index 0 is an error while a non-constant index 0 returns the default value.
            if (index == 0u)
                throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");
        }

        std::optional<size_t> element;
        if (index.getType() == Field::Types::UInt64)
            element = resolve_unsigned(index.safeGet<UInt64>());
        else if (index.getType() == Field::Types::Int64)
            element = resolve_signed(index.safeGet<Int64>());
        else if (index.isNull() && index_is_nullable)
        {
            null_map->getData().assign(input_rows_count, UInt8(1));
            return ColumnNullable::create(std::move(res), std::move(null_map));
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must have UInt or Int type", getName());

        if (!element)
        {
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                if (source_is_null(row))
                    null_map->getData()[row] = 1;
                else
                    set_out_of_range(row);
            }
        }
        else
        {
            /// The bit position is the same for every row, so read the planes one by one for cache friendliness.
            const size_t group = *element / stride;
            const size_t within_group = *element % stride;
            const size_t byte_offset = bytes_per_group - 1 - within_group / 8;
            const size_t bit_in_byte = within_group % 8;

            Word * words = reinterpret_cast<Word *>(res_data.data());
            for (size_t bit = 0; bit < element_size; ++bit)
            {
                const UInt8 * src = plane_chars(group, bit) + byte_offset;
                const size_t shift = element_size - 1 - bit;
                for (size_t row = 0; row < input_rows_count; ++row)
                {
                    if (source_is_null(row))
                    {
                        null_map->getData()[row] = 1;
                        continue;
                    }
                    const size_t qbit_row = qbit_is_const ? 0 : row;
                    words[row] |= static_cast<Word>(static_cast<Word>((src[qbit_row * bytes_per_group] >> bit_in_byte) & 1) << shift);
                }
            }
        }
    }
    else
    {
        const auto * nullable_index = checkAndGetColumn<ColumnNullable>(&index_column);
        const IColumn & nested_index_column = nullable_index ? nullable_index->getNestedColumn() : index_column;
        const auto * index_null_map = nullable_index ? &nullable_index->getNullMapData() : nullptr;

        auto execute_index_type = [&]<typename IndexType>() -> bool
        {
            const auto * col_index = checkAndGetColumn<ColumnVector<IndexType>>(&nested_index_column);
            if (!col_index)
                return false;

            const auto & indices = col_index->getData();
            for (size_t row = 0; row < input_rows_count; ++row)
            {
                if (source_is_null(row))
                {
                    null_map->getData()[row] = 1;
                    continue;
                }
                if (index_null_map && (*index_null_map)[row])
                {
                    null_map->getData()[row] = 1;
                    continue;
                }
                std::optional<size_t> element;
                if constexpr (std::is_signed_v<IndexType>)
                    element = resolve_signed(indices[row]);
                else
                    element = resolve_unsigned(indices[row]);

                if (element)
                    extract_into(row, *element);
                else
                    set_out_of_range(row);
            }
            return true;
        };

        if (!(execute_index_type.template operator()<UInt8>() || execute_index_type.template operator()<UInt16>()
              || execute_index_type.template operator()<UInt32>() || execute_index_type.template operator()<UInt64>()
              || execute_index_type.template operator()<Int8>() || execute_index_type.template operator()<Int16>()
              || execute_index_type.template operator()<Int32>() || execute_index_type.template operator()<Int64>()))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must have UInt or Int type", getName());
    }

    if (null_map)
        return ColumnNullable::create(std::move(res), std::move(null_map));

    return res;
}

template <ArrayElementExceptionMode mode>
String FunctionArrayElement<mode>::getName() const
{
    return name;
}

template <ArrayElementExceptionMode mode>
DataTypePtr FunctionArrayElement<mode>::getReturnTypeImpl(const DataTypes & arguments) const
{
    /// `useDefaultImplementationForNulls` is disabled for the whole overload set because of QBit
    /// (see executeImpl). Reproduce the type of the default adapter for a NULL literal argument.
    /// This convention holds for every source type, QBit included.
    if (arguments[0]->onlyNull() || arguments[1]->onlyNull())
        return makeNullable(std::make_shared<DataTypeNothing>());

    if (const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get()))
    {
        auto value_type = recursiveRemoveLowCardinality(map_type->getValueType());
        return (is_null_mode || arguments[1]->isNullable()) && value_type->canBeInsideNullable() ? makeNullable(value_type) : value_type;
    }

    const bool qbit_is_nullable = arguments[0]->isNullable();
    if (const auto * qbit_type = checkAndGetDataType<DataTypeQBit>(removeNullable(arguments[0]).get()))
    {
        if (const auto * index_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get()))
        {
            auto index_element_type = recursiveRemoveLowCardinality(index_array_type->getNestedType());
            const bool index_element_is_nullable = index_element_type->isNullable();
            if (!isNativeInteger(removeNullable(index_element_type)))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument for function '{}' must be integer or array of integers, got '{}' instead",
                    getName(),
                    arguments[1]->getName());
            }

            auto element_type = qbit_type->getElementType();
            if ((is_null_mode || index_element_is_nullable || qbit_is_nullable) && element_type->canBeInsideNullable())
                element_type = makeNullable(element_type);
            return std::make_shared<DataTypeArray>(element_type);
        }

        if (!isNativeInteger(removeNullable(arguments[1])))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function '{}' must be integer, got '{}' instead",
                getName(),
                arguments[1]->getName());
        }

        /// The n-th element of a QBit vector is reconstructed at the full precision of the element type.
        const auto & element_type = qbit_type->getElementType();
        return (is_null_mode || qbit_is_nullable || arguments[1]->isNullable()) ? makeNullable(element_type) : element_type;
    }

    const auto * array_type = checkAndGetDataType<DataTypeArray>(arguments[0].get());
    if (!array_type)
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "First argument for function '{}' must be array, got '{}' instead",
            getName(),
            arguments[0]->getName());
    }

    if (const auto * index_array_type = checkAndGetDataType<DataTypeArray>(arguments[1].get()))
    {
        auto index_element_type = recursiveRemoveLowCardinality(index_array_type->getNestedType());
        bool index_element_is_nullable = index_element_type->isNullable();
        if (!isNativeInteger(removeNullable(index_element_type)))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function '{}' must be integer or array of integers, got '{}' instead",
                getName(),
                arguments[1]->getName());
        }

        /// `arr[indexes]` is equivalent to `arrayMap(i -> arr[i], indexes)`, so an element of the
        /// result has exactly the type the scalar form returns for the same array. In particular,
        /// a `NULL` index makes the scalar form return `NULL`, so a nullable index element type
        /// makes the result element type nullable as well.
        auto nested_type = recursiveRemoveLowCardinality(array_type->getNestedType());
        if ((is_null_mode || index_element_is_nullable) && nested_type->canBeInsideNullable())
            nested_type = makeNullable(nested_type);
        return std::make_shared<DataTypeArray>(nested_type);
    }

    auto nested_type = recursiveRemoveLowCardinality(array_type->getNestedType());
    auto index_type = removeNullable(removeLowCardinality(arguments[1]));
    if (!isNativeInteger(index_type))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function '{}' must be integer or array of integers, got '{}' instead",
            getName(),
            arguments[1]->getName());
    }

    return (is_null_mode || arguments[1]->isNullable()) && nested_type->canBeInsideNullable() ? makeNullable(nested_type) : nested_type;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::tryExecuteLowCardinality(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    if (arguments.size() != 2 || !isColumnConst(*arguments[1].column))
        return nullptr;

    /// An array of indexes is handled by `executeWithArrayIndex`, which produces an array result
    /// this path does not know how to build.
    if (checkAndGetDataType<DataTypeArray>(arguments[1].type.get()))
        return nullptr;

    /// Nullable and LowCardinality(Nullable) arguments make the result type Nullable,
    /// which this path does not produce. Leave them to the default implementations.
    for (const auto & argument : arguments)
        if (isNullableOrLowCardinalityNullable(argument.type))
            return nullptr;

    Field index = (*arguments[1].column)[0];
    if ((index.getType() == Field::Types::UInt64 && index.safeGet<UInt64>() == 0)
        || (index.getType() == Field::Types::Int64 && index.safeGet<Int64>() == 0))
        return nullptr;

    /// Only optimize arrayElement here. arrayElementOrNull would need to build the null map
    /// for out-of-bounds rows, which is a separate path from the measured materialization hot spot.
    if constexpr (!is_null_mode)
    {
        if (const auto * col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get()))
        {
            const auto * low_cardinality_data = typeid_cast<const ColumnLowCardinality *>(&col_array->getData());
            const auto & array_type = assert_cast<const DataTypeArray &>(*arguments[0].type);
            if (low_cardinality_data
                && isStringOrFixedString(removeLowCardinality(array_type.getNestedType()))
                && (index.getType() == Field::Types::UInt64 || index.getType() == Field::Types::Int64))
                return LowCardinalityExecutionHelpers::LowCardinalityArrayView{
                    .elements = *low_cardinality_data,
                    .offsets = col_array->getOffsets(),
                    .rows = input_rows_count,
                }.arrayElementConst(index, *result_type);
        }
    }

    const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
    if (!col_map)
        return nullptr;

    const auto & map_column = *col_map;
    if (!typeid_cast<const ColumnLowCardinality *>(&map_column.getNestedData().getColumn(0)))
        return nullptr;

    /// The string key lookup below is defined only for String and FixedString keys. For other
    /// LowCardinality key types, leave the arguments to the default implementations so that the
    /// regular dispatch reports the type error, exactly as without the specialized path.
    const auto & map_type = assert_cast<const DataTypeMap &>(*arguments[0].type);
    if (!isStringOrFixedString(removeLowCardinality(map_type.getKeyType())))
        return nullptr;

    if (index.getType() != Field::Types::String)
        return nullptr;

    return recursiveRemoveLowCardinality(executeMap(arguments, result_type, input_rows_count));
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    /// A replicated column can arrive in either position.
    if (typeid_cast<const ColumnReplicated *>(arguments[0].column.get())
        || typeid_cast<const ColumnReplicated *>(arguments[1].column.get()))
        return executeReplicated(arguments, result_type, input_rows_count);

    const bool is_qbit = checkAndGetDataType<DataTypeQBit>(removeNullable(arguments[0].type).get());

    /// The default nullable implementation cannot preserve a NULL `QBit` source for an
    /// array-of-indices result: the result itself is an Array and cannot be wrapped in
    /// Nullable. It is therefore handled by executeQBit below, and `useDefaultImplementationForNulls`
    /// is disabled for the whole overload set. The established Array and Map paths keep the
    /// conventions of the default adapter, which are reproduced here.
    /// A NULL literal argument makes the result a NULL constant, without evaluating the function.
    /// This convention holds for every source type, QBit included.
    if (arguments[0].type->onlyNull() || arguments[1].type->onlyNull())
        return result_type->createColumnConstWithDefaultValue(input_rows_count);

    if (!is_qbit)
    {
        if (arguments[1].type->isNullable())
        {
            /// The same for a constant NULL of a nullable type, as long as the result can hold a NULL.
            if (result_type->isNullable() && isColumnConst(*arguments[1].column) && arguments[1].column->onlyNull())
                return result_type->createColumnConstWithDefaultValue(input_rows_count);

            /// For a non-nullable result the convention is to evaluate the function on the nested index
            /// without turning the result into Nullable.
            auto nested_arguments = arguments;
            nested_arguments[1] = columnGetNested(arguments[1]);
            auto result = executeImpl(nested_arguments, removeNullable(result_type), input_rows_count);
            return result_type->isNullable()
                ? wrapInNullable(result, arguments, result_type, input_rows_count)
                : result;
        }
    }

    if (is_qbit)
        return executeQBit(arguments, input_rows_count);

    const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
    const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());

    if (col_map || col_const_map)
        return executeMap(arguments, result_type, input_rows_count);

    /// Array-of-indices mode: arr1[arr2] where arr2 is Array(Int*)
    if (checkAndGetDataType<DataTypeArray>(arguments[1].type.get()))
        return executeWithArrayIndex(arguments, result_type, input_rows_count);

    /// Check nullability.
    bool is_array_of_nullable = false;
    const ColumnArray * col_array = nullptr;
    const ColumnArray * col_const_array = nullptr;

    col_array = checkAndGetColumn<ColumnArray>(arguments[0].column.get());
    if (col_array)
    {
        is_array_of_nullable = isColumnNullable(col_array->getData());
    }
    else
    {
        col_const_array = checkAndGetColumnConstData<ColumnArray>(arguments[0].column.get());
        if (col_const_array)
            is_array_of_nullable = isColumnNullable(col_const_array->getData());
        else
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
    }

    if (!is_array_of_nullable)
    {
        ArrayImpl::NullMapBuilder<mode> builder;
        auto res = perform(arguments, removeNullable(result_type), builder, input_rows_count);

        if (builder && res->canBeInsideNullable())
            return ColumnNullable::create(res, std::move(builder).getNullMapColumnPtr());

        return res;
    }

    /// Perform initializations.
    ArrayImpl::NullMapBuilder<mode> builder;
    ColumnsWithTypeAndName source_columns;

    const DataTypePtr & input_type
        = typeid_cast<const DataTypeNullable &>(*typeid_cast<const DataTypeArray &>(*arguments[0].type).getNestedType())
              .getNestedType();

    DataTypePtr tmp_ret_type = removeNullable(result_type);

    if (col_array)
    {
        const auto & nullable_col = typeid_cast<const ColumnNullable &>(col_array->getData());
        const auto & nested_col = nullable_col.getNestedColumnPtr();

        /// Put nested_col inside a ColumnArray.
        source_columns = {
            {ColumnArray::create(nested_col, col_array->getOffsetsPtr()), std::make_shared<DataTypeArray>(input_type), ""},
            arguments[1],
        };

        builder.initSource(nullable_col.getNullMapData().data());
    }
    else
    {
        /// ColumnConst(ColumnArray(ColumnNullable(...)))
        const auto & nullable_col = assert_cast<const ColumnNullable &>(col_const_array->getData());
        const auto & nested_col = nullable_col.getNestedColumnPtr();

        source_columns = {
            {ColumnConst::create(ColumnArray::create(nested_col, col_const_array->getOffsetsPtr()), input_rows_count),
             std::make_shared<DataTypeArray>(input_type),
             ""},
            arguments[1],
        };

        builder.initSource(nullable_col.getNullMapData().data());
    }

    auto res = perform(source_columns, tmp_ret_type, builder, input_rows_count);

    /// Store the result.
    return ColumnNullable::create(res, builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeReplicated(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    ColumnsWithTypeAndName args = arguments;

    const auto * replicated_array = typeid_cast<const ColumnReplicated *>(args[0].column.get());
    const auto * replicated_index = typeid_cast<const ColumnReplicated *>(args[1].column.get());

    /// When the array and the index are replicated by the same indexes column (they came from the same expansion,
    /// e.g. one ARRAY JOIN), row i reads nested_array[idx[i]][nested_index[idx[i]]], so the result is the nested
    /// computation replicated by the same indexes.
    if (replicated_array && replicated_index
        && replicated_array->getIndexesColumn().get() == replicated_index->getIndexesColumn().get()
        && replicated_array->getNestedColumn()->size() == replicated_index->getNestedColumn()->size())
    {
        /// Compute on the compact nested columns and stay lazy.(Remove unused indexes)
        auto compact = replicated_array->getIndexes().buildCompactIndexedColumns(
            {replicated_array->getNestedColumn(), replicated_index->getNestedColumn()});

        /// Now recurse on the internal rows
        size_t nested_rows_count = compact.compact_indexed_columns[0]->size();
        ColumnsWithTypeAndName nested_args
            = {{compact.compact_indexed_columns[0], args[0].type, args[0].name},
               {compact.compact_indexed_columns[1], args[1].type, args[1].name}};
        auto nested_res = executeImpl(nested_args, result_type, nested_rows_count);

        /// Wrap the result in a new ColumnReplicated with the compacted indexes column
        return convertToFullColumnIfReplicationNotUseful(
            ColumnReplicated::create(std::move(nested_res), compact.compact_indexes));
    }

    /// The index argument is a per-row number and when it is replicated independently of the array,
    /// its repetition has no structure, so materialize it.
    args[1].column = args[1].column->convertToFullColumnIfReplicated();

    if (!replicated_array)
        return executeImpl(args, result_type, input_rows_count);

    const auto * col_array = typeid_cast<const ColumnArray *>(replicated_array->getNestedColumn().get());

    /// Fall back to materialization for the shapes the fast path does not cover:
    /// Replicated over Map, and LowCardinality elements (their handling is layered above this function
    bool fast_path_supported = col_array
        && !typeid_cast<const ColumnLowCardinality *>(&col_array->getData())
        && !typeid_cast<const ColumnMap *>(&col_array->getData());
    if (!fast_path_supported)
    {
        args[0].column = args[0].column->convertToFullColumnIfReplicated();
        return executeImpl(args, result_type, input_rows_count);
    }

    if (isColumnConst(*args[1].column))
    {
        /// A constant index gives one value per nested row: execute on the compact nested array and replicate the result lazily.
        /// Compact away nested rows the indexes never reference, so the nested work is proportional to the used rows.
        auto compact = replicated_array->getIndexes().buildCompactIndexedColumns({replicated_array->getNestedColumn()});
        size_t nested_rows_count = compact.compact_indexed_columns[0]->size();
        ColumnsWithTypeAndName nested_args
            = {{compact.compact_indexed_columns[0], args[0].type, args[0].name},
               {args[1].column->cloneResized(nested_rows_count), args[1].type, args[1].name}};

        /// Recurse on the internal rows
        auto nested_res = executeImpl(nested_args, result_type, nested_rows_count);
        /// Wrap the result in a new ColumnReplicated with the compacted indexes column
        return convertToFullColumnIfReplicationNotUseful(
            ColumnReplicated::create(std::move(nested_res), compact.compact_indexes));
    }

    const auto & offsets = col_array->getOffsets();
    const IColumn * data = &col_array->getData();

    ArrayImpl::NullMapBuilder<mode> builder;
    bool is_array_of_nullable = isColumnNullable(*data);
    if (is_array_of_nullable)
    {
        const auto & nullable_data = assert_cast<const ColumnNullable &>(*data);
        builder.initSource(nullable_data.getNullMapData().data());
        data = &nullable_data.getNestedColumn();
    }

    if (builder)
        builder.initSink(input_rows_count);

    auto result = data->cloneEmpty();
    result->reserve(input_rows_count);

    const auto & replication_indexes = replicated_array->getIndexes();
    const auto & index_column = *args[1].column;
    /// Core loop to build result by dispatching based on the index type
    if (!tryEachType(
            IndexTypes{},
            [&]<typename T>() { return gatherReplicated<T>(index_column, replication_indexes, offsets, *data, *result, builder); }))
    {
        /// The index is not a plain numeric column (e.g. Nullable, or an array of indexes):
        /// materialize the array and let the generic path handle it.
        args[0].column = args[0].column->convertToFullColumnIfReplicated();
        return executeImpl(args, result_type, input_rows_count);
    }

    if (is_array_of_nullable)
        return ColumnNullable::create(
            std::move(result), builder ? std::move(builder).getNullMapColumnPtr() : ColumnUInt8::create());

    ColumnPtr immutable_result = std::move(result);
    if (builder && immutable_result->canBeInsideNullable())
        return ColumnNullable::create(immutable_result, std::move(builder).getNullMapColumnPtr());

    return immutable_result;
}

template <ArrayElementExceptionMode mode>
template <typename IndexType>
bool FunctionArrayElement<mode>::gatherReplicated(
    const IColumn & index_column,
    const ColumnIndex & replication_indexes,
    const ColumnArray::Offsets & offsets,
    const IColumn & data,
    IColumn & result,
    ArrayImpl::NullMapBuilder<mode> & builder)
{
    const auto * index_vec = checkAndGetColumn<ColumnVector<IndexType>>(&index_column);
    if (!index_vec)
        return false;

    const auto & indices = index_vec->getData();
    size_t rows = indices.size();
    /// Each output element is one insertFrom reading directly from the shared nested data
    for (size_t i = 0; i < rows; ++i)
    {
        ssize_t nested_row = replication_indexes.getIndexAt(i);
        /// `offsets[-1]` is a guaranteed zero (`PaddedPODArray` left padding), same as `ColumnArray::offsetAt`.
        ColumnArray::Offset begin = offsets[nested_row - 1];
        ColumnArray::Offset end = offsets[nested_row];

        IndexType index = indices[i];
        /// Positive index is 1-based from the beginning of the array, negative counts from the end.
        /// Any invalid index (zero, out of range) lands outside [begin, end) and produces a default value.
        size_t insert_position = index > 0 ? begin + index - 1 : end + index;
        if (begin <= insert_position && insert_position < end)
        {
            result.insertFrom(data, insert_position);
            builder.update(insert_position);
        }
        else
        {
            result.insertDefault();
            builder.update();
        }
    }

    return true;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::perform(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    ArrayImpl::NullMapBuilder<mode> & builder,
    size_t input_rows_count) const
{
    ColumnPtr res;
    if ((res = executeTuple(arguments, builder, input_rows_count)))
        return res;
    if ((res = executeMap2(arguments, input_rows_count)))
        return res;
    if (!isColumnConst(*arguments[1].column))
    {
        if (!tryEachType(
                IndexTypes{},
                [&]<typename T>() { return (res = executeArgument<T>(arguments, result_type, builder, input_rows_count)) != nullptr; }))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must have UInt or Int type", getName());
    }
    else
    {
        Field index = (*arguments[1].column)[0];

        if (index.getType() != Field::Types::UInt64 && index.getType() != Field::Types::Int64)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument for function {} must have UInt or Int type", getName());

        if (builder)
            builder.initSink(input_rows_count);

        if constexpr (!is_null_mode)
        {
            if (index == 0u)
            {
                /// It is very confusing that arrayElement throws an exception only for a constant index 0, but return default value for a non-constant index 0.
                /// e.g. `select [1,2,3][0]` throws exception, but `select [1,2,3][number] from numbers(1)` returns 0.
                /// TODO: do we need to fix it?
                throw Exception(ErrorCodes::ZERO_ARRAY_OR_TUPLE_INDEX, "Array indices are 1-based");
            }
        }

        bool matched = tryEachType(
                           ElementTypesWithFastPath{},
                           [&]<typename T>() { return (res = executeNumberConst<T>(arguments, result_type, index, builder)) != nullptr; })
            || (res = executeStringConst(arguments, index, builder))
            || tryEachType(
                   ElementTypesWithFastPath{},
                   [&]<typename T>() { return (res = executeArrayNumberConst<T>(arguments, result_type, index, builder)) != nullptr; })
            || (res = executeArrayStringConst(arguments, index, builder))
            || (res = executeGenericConst(arguments, index, builder));

        if (!matched)
            throw Exception(
                ErrorCodes::ILLEGAL_COLUMN,
                "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(),
                getName());
    }

    return res;
}

}

REGISTER_FUNCTION(ArrayElement)
{
    FunctionDocumentation::Description description = R"(
Gets the element of the provided array with index `n` where `n` can be any integer type.
If the index falls outside of the bounds of an array, it returns a default value (0 for numbers, an empty string for strings, etc.),
except for arguments of a non-constant array and a constant index 0. In this case there will be an error `Array indices are 1-based`.

When `n` is an array of integers, returns an array of the elements at the specified positions (a gather operation).
This is equivalent to `arrayMap(i -> arr[i], n)`, but has a separate, more efficient implementation.
Out-of-bounds positions produce the default value, the same as for a scalar index.
The index elements may be nullable. A `NULL` index produces `NULL` (and makes the result element type nullable) when the element type can be
wrapped in `Nullable`; for element types that cannot be inside `Nullable` (such as `Array`, `Map`), a `NULL` index produces the
default value instead. This is the same behavior as for a scalar `NULL` index.

:::note
Arrays in ClickHouse are one-indexed.
:::

Negative indexes are supported. In this case, the corresponding element is selected, numbered from the end. For example, `arr[-1]` is the last item in the array.

Operator `[n]` provides the same functionality.

The first argument may also be a [QBit](/sql-reference/data-types/qbit): the n-th vector element is reconstructed at the full precision of the QBit element type, reading only the bit planes of the stride group that contains it.
    )";
    FunctionDocumentation::Syntax syntax = "arrayElement(arr, n)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array to search. [`Array(T)`](/reference/data-types/array) or [`QBit`](/reference/data-types/qbit)."},
        {"n", "Position of the element to get, or an array of positions. The positions may be nullable. [`(U)Int*`](/reference/data-types/int-uint) or [`Array((U)Int*)`](/reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"When `n` is a scalar, returns the element of type `T`. When `n` is an array, returns `Array(Nullable(T))` if the index elements are nullable and `T` can be wrapped in `Nullable`, otherwise `Array(T)`.", {"Any", "Array(T)", "Array(Nullable(T))"}};
    FunctionDocumentation::Examples examples = {
        {"Usage example", "SELECT arrayElement(arr, 2) FROM (SELECT [1, 2, 3] AS arr)", "2"},
        {"Negative indexing", "SELECT arrayElement(arr, -1) FROM (SELECT [1, 2, 3] AS arr)", "3"},
        {"Using [n] notation", "SELECT arr[2] FROM (SELECT [1, 2, 3] AS arr)", "2"},
        {"Index out of array bounds", "SELECT arrayElement(arr, 4) FROM (SELECT [1, 2, 3] AS arr)", "0"},
        {"Array of indices", "SELECT [10, 20, 30, 40][[2, 4, 1]]", "[20,40,10]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionWithLowCardinalityFastPath<FunctionArrayElement<ArrayElementExceptionMode::Zero>>>(documentation);

    FunctionDocumentation::Description description_null = R"(
Gets the element of the provided array with index `n` where `n` can be any integer type.
If the index falls outside of the bounds of an array, `NULL` is returned instead of a default value,
as long as the result type can be nullable. For element types that are not already nullable and cannot be
put inside `Nullable` (such as `Array`, `Map`), the default value of the element type is returned instead.

When `n` is an array of integers, returns an array of the elements at the specified positions.
This is equivalent to `arrayMap(i -> arrayElementOrNull(arr, i), n)`, but has a separate, more efficient implementation.
Out-of-bounds positions and `NULL` indexes produce `NULL` values in the result array, following the same rule as for a scalar index.

:::note
Arrays in ClickHouse are one-indexed.
:::

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.
)";
    FunctionDocumentation::Syntax syntax_null = "arrayElementOrNull(arr, n)";
    FunctionDocumentation::Arguments arguments_null = {
        {"arr", "The array to search. [`Array(T)`](/reference/data-types/array)."},
        {"n", "Position of the element to get, or an array of positions. The positions may be nullable. [`(U)Int*`](/reference/data-types/int-uint) or [`Array((U)Int*)`](/reference/data-types/array)."}
    };
    FunctionDocumentation::ReturnedValue returned_value_null = {"When `n` is a scalar, returns `Nullable(T)` if `T` can be wrapped in `Nullable`, otherwise `T`. When `n` is an array, returns `Array(Nullable(T))` if `T` can be wrapped in `Nullable`, otherwise `Array(T)`.", {"Any", "Nullable(T)", "Array(T)", "Array(Nullable(T))"}};
    FunctionDocumentation::Examples examples_null = {
        {"Usage example", "SELECT arrayElementOrNull(arr, 2) FROM (SELECT [1, 2, 3] AS arr)", "2"},
        {"Negative indexing", "SELECT arrayElementOrNull(arr, -1) FROM (SELECT [1, 2, 3] AS arr)", "3"},
        {"Index out of array bounds", "SELECT arrayElementOrNull(arr, 4) FROM (SELECT [1, 2, 3] AS arr)", "\\N"},
        {"Array of indices", "SELECT arrayElementOrNull([10, 20, 30], [1, 5, 2])", "[10,NULL,20]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in_null = {1, 1};
    FunctionDocumentation::Category category_null = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation_null = {description_null, syntax_null, arguments_null, {}, returned_value_null, examples_null, introduced_in_null, category_null};

    factory.registerFunction<FunctionWithLowCardinalityFastPath<FunctionArrayElement<ArrayElementExceptionMode::Null>>>(documentation_null);
}
}
