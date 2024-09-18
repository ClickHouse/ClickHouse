#include <Columns/ColumnArray.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Core/ColumnNumbers.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Functions/castTypeToEither.h>
#include <Interpreters/Context.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>

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


/** arrayElement(arr, i) - get the array element by index. If index is not constant and out of range - return default value of data type.
  * The index begins with 1. Also, the index can be negative - then it is counted from the end of the array.
  */
template <ArrayElementExceptionMode mode = ArrayElementExceptionMode::Zero>
class FunctionArrayElement : public IFunction
{
public:
    static constexpr bool is_null_mode = (mode == ArrayElementExceptionMode::Null);
    static constexpr auto name = (mode == ArrayElementExceptionMode::Zero) ? "arrayElement" : "arrayElementOrNull";
    static FunctionPtr create(ContextPtr context_);

    String getName() const override;

    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override;

private:
    ColumnPtr perform(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        ArrayImpl::NullMapBuilder<mode> & builder,
        size_t input_rows_count) const;

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
    ColumnPtr executeTuple(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    /** For a map array, the function is evaluated component-wise for its keys and values
      */
    ColumnPtr executeMap2(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const;

    /** For a map the function finds the matched value for a key.
     *  Currently implemented just as linear search in array.
     *  However, optimizations are possible.
     */
    ColumnPtr executeMap(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const;

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

    ContextPtr context;
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
                size_t j;
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
            size_t adjusted_index; /// index in array from zero
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
            size_t adjusted_index; /// index in array from zero

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
        ColumnString::Chars zero_buf(16, '\0'); /// Needs 15 extra bytes for memcpySmallAllowReadWriteOverflow15
        std::vector<std::pair<const ColumnString::Char *, UInt64>> selected_bufs;
        selected_bufs.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;

            if (index < array_size)
            {
                size_t adjusted_index;
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
                /// Insert an empty row.
                total_result_bytes += 1;
                selected_bufs.emplace_back(zero_buf.data(), 1);
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

        ColumnString::Chars zero_buf(16, '\0'); /// Needs 15 extra bytes for memcpySmallAllowReadWriteOverflow15
        ColumnArray::Offset current_offset = 0;
        /// get the total result bytes at first, and reduce the cost of result_data.resize.
        size_t total_result_bytes = 0;
        std::vector<std::pair<const ColumnString::Char *, UInt64>> selected_bufs;
        selected_bufs.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            size_t array_size = offsets[i] - current_offset;
            size_t adjusted_index; /// index in array from zero

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
                /// Insert empty string
                total_result_bytes += 1;
                selected_bufs.emplace_back(zero_buf.data(), 1);
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
FunctionPtr FunctionArrayElement<mode>::create(ContextPtr)
{
    return std::make_shared<FunctionArrayElement>();
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
    else
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
    else
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
    if (!((res = executeNumber<IndexType, UInt8>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, UInt16>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, UInt32>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, UInt64>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int8>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int16>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int32>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int64>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Float32>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Float64>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Decimal32>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Decimal64>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Decimal128>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Decimal256>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, DateTime64>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int128>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, UInt128>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, Int256>(arguments, result_type, index_data, builder))
          || (res = executeNumber<IndexType, UInt256>(arguments, result_type, index_data, builder))
          || (res = executeConst<IndexType>(arguments, result_type, index_data, builder, input_rows_count))
          || (res = executeString<IndexType>(arguments, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt8>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt16>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt32>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt64>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int8>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int16>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int32>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int64>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Float32>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Float64>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Decimal32>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Decimal64>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Decimal128>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Decimal256>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, DateTime64>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int128>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt128>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, Int256>(arguments, result_type, index_data, builder))
          || (res = executeArrayNumber<IndexType, UInt256>(arguments, result_type, index_data, builder))
          || (res = executeArrayString<IndexType>(arguments, index_data, builder))
          || (res = executeGeneric<IndexType>(arguments, index_data, builder))))
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
ColumnPtr FunctionArrayElement<mode>::executeTuple(const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
{
    const ColumnArray * col_array = typeid_cast<const ColumnArray *>(arguments[0].column.get());
    if (!col_array)
        return nullptr;

    const ColumnTuple * col_nested = typeid_cast<const ColumnTuple *>(&col_array->getData());
    if (!col_nested)
        return nullptr;

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
        return memequalSmallAllowOverflow15(index_ref.data, index_ref.size, data_ref.data, data_ref.size);
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
        return index.size() == data_ref.size && memcmp(index.data(), data_ref.data, data_ref.size) == 0;
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
    size_t expected_match_pos = 0;
    bool matched = false;
    if (!rows)
        return;

    /// In practice, map keys are usually in the same order, it is worth a try to
    /// predict the next key position. So it can avoid a lot of unnecessary comparisons.
    for (size_t j = offsets[-1], end = offsets[0]; j < end; ++j)
    {
        if (matcher.match(j, 0))
        {
            matched_idxs.push_back(j - offsets[-1] + 1);
            matched = true;
            expected_match_pos = end + j - offsets[-1];
            break;
        }
    }
    if (!matched)
    {
        expected_match_pos = offsets[0];
        matched_idxs.push_back(0);
    }
    size_t i = 1;
    for (; i < rows; ++i)
    {
        const auto & begin = offsets[i - 1];
        const auto & end = offsets[i];
        if (expected_match_pos < end && matcher.match(expected_match_pos, i))
        {
            auto map_key_index = expected_match_pos - begin;
            matched_idxs.push_back(map_key_index + 1);
            expected_match_pos = end + map_key_index;
        }
        else
            break;
    }

    // fallback to linear search
    for (; i < rows; ++i)
    {
        matched = false;
        const auto & begin = offsets[i - 1];
        const auto & end = offsets[i];
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

template <ArrayElementExceptionMode mode>
bool FunctionArrayElement<mode>::matchKeyToIndexStringConst(
    const IColumn & data, const Offsets & offsets, const Field & index, PaddedPODArray<UInt64> & matched_idxs)
{
    return castColumnString(
        &data,
        [&](const auto & data_column)
        {
            using DataColumn = std::decay_t<decltype(data_column)>;
            if (index.getType() != Field::Types::String)
                return false;
            MatcherStringConst<DataColumn> matcher{data_column, index.safeGet<const String &>()};
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
    assert(col_map || col_const_map);

    if (col_const_map)
        col_map = typeid_cast<const ColumnMap *>(&col_const_map->getDataColumn());

    const auto & nested_column = col_map->getNestedColumn();
    const auto & keys_data = col_map->getNestedData().getColumn(0);
    const auto & values_data = col_map->getNestedData().getColumn(1);
    const auto & offsets = nested_column.getOffsets();

    /// At first step calculate indices in array of values for requested keys.
    auto indices_column = DataTypeNumber<UInt64>().createColumn();
    indices_column->reserve(input_rows_count);
    auto & indices_data = assert_cast<ColumnVector<UInt64> &>(*indices_column).getData();

    bool executed = false;
    if (!isColumnConst(*arguments[1].column))
    {
        executed = matchKeyToIndexNumber(keys_data, offsets, !!col_const_map, *arguments[1].column, indices_data)
            || matchKeyToIndexString(keys_data, offsets, !!col_const_map, *arguments[1].column, indices_data);
    }
    else
    {
        Field index = (*arguments[1].column)[0];
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

    const auto & type_map = assert_cast<const DataTypeMap &>(*arguments[0].type);

    /// Prepare arguments to call arrayElement for array with values and calculated indices at previous step.
    ColumnsWithTypeAndName new_arguments
        = {{values_array, std::make_shared<DataTypeArray>(type_map.getValueType()), ""},
           {std::move(indices_column), std::make_shared<DataTypeNumber<UInt64>>(), ""}};

    return executeImpl(new_arguments, result_type, input_rows_count);
}

template <ArrayElementExceptionMode mode>
String FunctionArrayElement<mode>::getName() const
{
    return name;
}

template <ArrayElementExceptionMode mode>
DataTypePtr FunctionArrayElement<mode>::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get()))
    {
        auto value_type = map_type->getValueType();
        return is_null_mode && value_type->canBeInsideNullable() ? makeNullable(value_type) : value_type;
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

    if (!isNativeInteger(arguments[1]))
    {
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Second argument for function '{}' must be integer, got '{}' instead",
            getName(),
            arguments[1]->getName());
    }

    auto nested_type = array_type->getNestedType();
    return is_null_mode && nested_type->canBeInsideNullable() ? makeNullable(nested_type) : nested_type;
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::executeImpl(
    const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[0].column.get());
    const auto * col_const_map = checkAndGetColumnConst<ColumnMap>(arguments[0].column.get());

    if (col_map || col_const_map)
        return executeMap(arguments, result_type, input_rows_count);

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
        else
            return res;
    }
    else
    {
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
}

template <ArrayElementExceptionMode mode>
ColumnPtr FunctionArrayElement<mode>::perform(
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & result_type,
    ArrayImpl::NullMapBuilder<mode> & builder,
    size_t input_rows_count) const
{
    ColumnPtr res;
    if ((res = executeTuple(arguments, input_rows_count)))
        return res;
    else if ((res = executeMap2(arguments, input_rows_count)))
        return res;
    else if (!isColumnConst(*arguments[1].column))
    {
        if (!((res = executeArgument<UInt8>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<UInt16>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<UInt32>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<UInt64>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<Int8>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<Int16>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<Int32>(arguments, result_type, builder, input_rows_count))
              || (res = executeArgument<Int64>(arguments, result_type, builder, input_rows_count))))
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

        if (!((res = executeNumberConst<UInt8>(arguments, result_type, index, builder))
              || (res = executeNumberConst<UInt16>(arguments, result_type, index, builder))
              || (res = executeNumberConst<UInt32>(arguments, result_type, index, builder))
              || (res = executeNumberConst<UInt64>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int8>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int16>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int32>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int64>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Float32>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Float64>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Decimal32>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Decimal64>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Decimal128>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Decimal256>(arguments, result_type, index, builder))
              || (res = executeNumberConst<DateTime64>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int128>(arguments, result_type, index, builder))
              || (res = executeNumberConst<UInt128>(arguments, result_type, index, builder))
              || (res = executeNumberConst<Int256>(arguments, result_type, index, builder))
              || (res = executeNumberConst<UInt256>(arguments, result_type, index, builder))
              || (res = executeStringConst(arguments, index, builder))
              || (res = executeArrayNumberConst<UInt8>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<UInt16>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<UInt32>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<UInt64>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int8>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int16>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int32>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int64>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Float32>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Float64>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Decimal32>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Decimal64>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Decimal128>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Decimal256>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<DateTime64>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int128>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<UInt128>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<Int256>(arguments, result_type, index, builder))
              || (res = executeArrayNumberConst<UInt256>(arguments, result_type, index, builder))
              || (res = executeArrayStringConst(arguments, index, builder)) || (res = executeGenericConst(arguments, index, builder))))
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
    factory.registerFunction<FunctionArrayElement<ArrayElementExceptionMode::Zero>>(FunctionDocumentation{
        .description = R"(
Get the element with the index `n` from the array `arr`. `n` must be any integer type. Indexes in an array begin from one.

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns some default value (0 for numbers, an empty string for strings, etc.), except for the case with a non-constant array and a constant index 0 (in this case there will be an error `Array indices are 1-based`).
        )",
        .categories{"Array"}});
    factory.registerFunction<FunctionArrayElement<ArrayElementExceptionMode::Null>>(FunctionDocumentation{
        .description = R"(
Get the element with the index `n`from the array `arr`. `n` must be any integer type. Indexes in an array begin from one.

Negative indexes are supported. In this case, it selects the corresponding element numbered from the end. For example, `arr[-1]` is the last item in the array.

If the index falls outside of the bounds of an array, it returns `NULL` instead of a default value.
        )",
        .categories{"Array"}});
}
}
