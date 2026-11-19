#include <DataTypes/DataTypeMapHelpers.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>
#include <base/memcmpSmall.h>


namespace DB
{

namespace
{

/// A sentinel value meaning "key not found in this row".
constexpr size_t KEY_NOT_FOUND = std::numeric_limits<size_t>::max();

/// ---------------------------------------------------------------------------
/// Phase 1: Find the position of the requested key in each row.
///
/// Builds `matched_positions[i]` = flat index into the keys/values column
/// where the key was found for row (start + i), or KEY_NOT_FOUND.
/// Uses position prediction: if the key was at relative offset K in the
/// previous row, we try offset K first in the current row before falling
/// back to a linear scan.
/// ---------------------------------------------------------------------------

/// Generic key matcher that uses virtual compareAt. Used as a fallback
/// when the key column type is not one of the specialized types.
struct KeyMatcherGeneric
{
    const IColumn & keys_column;
    const IColumn & key;

    bool match(size_t keys_row) const
    {
        return keys_column.compareAt(keys_row, 0, key, 0) == 0;
    }
};

/// Specialized key matcher for ColumnVector<T>. Compares values directly
/// without virtual dispatch.
template <typename T>
struct KeyMatcherVector
{
    const typename ColumnVector<T>::Container & data;
    T key_value;

    bool match(size_t keys_row) const
    {
        return data[keys_row] == key_value;
    }
};

/// Specialized key matcher for ColumnString. Compares string data directly
/// using size check + memcmp, avoiding virtual dispatch and assert_cast.
struct KeyMatcherString
{
    const ColumnString::Chars & chars;
    const ColumnString::Offsets & string_offsets;
    const char * key_data;
    size_t key_size;

    bool match(size_t keys_row) const
    {
        size_t offset = string_offsets[ssize_t(keys_row) - 1];
        size_t size = string_offsets[keys_row] - offset;
        if (size != key_size)
            return false;
        return memcmp(&chars[offset], key_data, key_size) == 0;
    }
};

/// Specialized key matcher for ColumnFixedString. Compares fixed-size data
/// directly using memcmpSmallAllowOverflow15.
struct KeyMatcherFixedString
{
    const ColumnFixedString::Chars & chars;
    size_t n;
    const UInt8 * key_data;

    bool match(size_t keys_row) const
    {
        return memcmpSmallAllowOverflow15(&chars[keys_row * n], key_data, n) == 0;
    }
};

/// The core position-finding loop, parametrized by Matcher type.
/// For each row in [start, end), finds the flat index of the matching key.
template <typename Matcher>
void findKeyPositions(
    const ColumnArray::Offsets & offsets,
    const Matcher & matcher,
    size_t start,
    size_t end,
    PaddedPODArray<size_t> & matched_positions)
{
    size_t num_rows = end - start;
    matched_positions.resize(num_rows);

    /// Relative offset of the key within the map from the previous row.
    /// Used for position prediction.
    size_t predicted_relative_pos = 0;
    bool have_prediction = false;

    for (size_t i = start; i < end; ++i)
    {
        size_t positions_row_idx = i - start;
        size_t offset_start = offsets[ssize_t(i) - 1];
        size_t offset_end = offsets[i];

        /// Try the predicted position first.
        if (have_prediction)
        {
            size_t predicted_pos = offset_start + predicted_relative_pos;
            if (predicted_pos < offset_end && matcher.match(predicted_pos))
            {
                matched_positions[positions_row_idx] = predicted_pos;
                continue;
            }
        }

        /// Prediction missed or not available. Fall back to linear scan.
        bool found = false;
        for (size_t j = offset_start; j < offset_end; ++j)
        {
            if (matcher.match(j))
            {
                matched_positions[positions_row_idx] = j;
                predicted_relative_pos = j - offset_start;
                have_prediction = true;
                found = true;
                break;
            }
        }

        if (!found)
        {
            /// Keep the prediction unchanged: if only this row is missing the key,
            /// subsequent rows likely have the same key order, so the prediction
            /// may still be valid. A wrong prediction costs only one extra match call.
            matched_positions[positions_row_idx] = KEY_NOT_FOUND;
        }
    }
}

/// Dispatches to the appropriate specialized matcher based on the key column type,
/// then calls findKeyPositions with that matcher.
void findKeyPositionsDispatch(
    const IColumn & keys_column,
    const ColumnArray::Offsets & offsets,
    const IColumn & key,
    size_t start,
    size_t end,
    PaddedPODArray<size_t> & matched_positions)
{
    TypeIndex type_id = keys_column.getDataType();

    /// Try ColumnVector<T> specializations.
    switch (type_id)
    {
#define DISPATCH_VECTOR(T) \
        case TypeIndex::T: \
        { \
            using ColType = ColumnVector<T>; \
            const auto & typed_col = assert_cast<const ColType &>(keys_column); \
            const auto & key_col = assert_cast<const ColType &>(key); \
            KeyMatcherVector<T> matcher{typed_col.getData(), key_col.getData()[0]}; \
            findKeyPositions(offsets, matcher, start, end, matched_positions); \
            return; \
        }

        DISPATCH_VECTOR(UInt8)
        DISPATCH_VECTOR(UInt16)
        DISPATCH_VECTOR(UInt32)
        DISPATCH_VECTOR(UInt64)
        DISPATCH_VECTOR(Int8)
        DISPATCH_VECTOR(Int16)
        DISPATCH_VECTOR(Int32)
        DISPATCH_VECTOR(Int64)
        DISPATCH_VECTOR(Float32)
        DISPATCH_VECTOR(Float64)
#undef DISPATCH_VECTOR

        case TypeIndex::String:
        {
            const auto & typed_col = assert_cast<const ColumnString &>(keys_column);
            const auto & key_col = assert_cast<const ColumnString &>(key);
            auto key_ref = key_col.getDataAt(0);
            KeyMatcherString matcher{typed_col.getChars(), typed_col.getOffsets(), key_ref.data(), key_ref.size()};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
            return;
        }
        case TypeIndex::FixedString:
        {
            const auto & typed_col = assert_cast<const ColumnFixedString &>(keys_column);
            const auto & key_col = assert_cast<const ColumnFixedString &>(key);
            KeyMatcherFixedString matcher{typed_col.getChars(), typed_col.getN(), key_col.getChars().data()};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
            return;
        }
        default:
        {
            /// Fallback: generic matcher using virtual compareAt.
            KeyMatcherGeneric matcher{keys_column, key};
            findKeyPositions(offsets, matcher, start, end, matched_positions);
        }
    }
}

/// ---------------------------------------------------------------------------
/// Phase 2: Extract values at the matched positions.
///
/// For each row, if matched_positions[i] != KEY_NOT_FOUND, copy the value from
/// values_column at that flat index into result. Otherwise, insert a default.
/// ---------------------------------------------------------------------------

/// Generic value extractor using virtual insertFrom / insertDefault.
void extractValuesGeneric(
    const IColumn & values_column,
    IColumn & result,
    const PaddedPODArray<size_t> & matched_positions)
{
    result.reserve(result.size() + matched_positions.size());
    for (size_t pos : matched_positions)
    {
        if (pos != KEY_NOT_FOUND)
            result.insertFrom(values_column, pos);
        else
            result.insertDefault();
    }
}

/// Specialized value extractor for ColumnVector<T>, with optional Nullable support.
/// If src_null_map / dst_null_map are non-null, propagates null flags.
/// For missing keys, inserts a default value and sets the null flag to 1.
template <typename T>
void extractValuesVector(
    const ColumnVector<T> & values_column,
    ColumnVector<T> & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    const auto & src_data = values_column.getData();
    auto & dst_data = result.getData();
    size_t old_size = dst_data.size();
    size_t num_rows = matched_positions.size();
    dst_data.resize(old_size + num_rows);
    if (dst_null_map)
        dst_null_map->resize(old_size + num_rows);

    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            dst_data[old_size + i] = src_data[pos];
            if (dst_null_map)
                (*dst_null_map)[old_size + i] = (*src_null_map)[pos];
        }
        else
        {
            dst_data[old_size + i] = T{};
            if (dst_null_map)
                (*dst_null_map)[old_size + i] = static_cast<UInt8>(1);
        }
    }
}

/// Specialized value extractor for ColumnString, with optional Nullable support.
/// Two-pass approach: first pass computes offsets, total chars size, and fills null map;
/// second pass re-iterates matched_positions to copy string data.
void extractValuesString(
    const ColumnString & values_column,
    ColumnString & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    const auto & src_chars = values_column.getChars();
    const auto & src_offsets = values_column.getOffsets();
    auto & dst_chars = result.getChars();
    auto & dst_offsets = result.getOffsets();

    size_t old_offsets_size = dst_offsets.size();
    size_t num_rows = matched_positions.size();

    dst_offsets.resize(old_offsets_size + num_rows);
    if (dst_null_map)
        dst_null_map->resize(dst_null_map->size() + num_rows);

    /// First pass: compute result offsets, total chars size, and fill null map.
    /// total_chars_size must start from the existing chars size because
    /// ColumnString offsets are absolute positions into the chars array.
    size_t old_chars_size = dst_chars.size();
    size_t total_chars_size = old_chars_size;
    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            total_chars_size += src_offsets[pos] - src_offsets[ssize_t(pos) - 1];
            if (dst_null_map)
                (*dst_null_map)[old_offsets_size + i] = (*src_null_map)[pos];
        }
        else
        {
            if (dst_null_map)
                (*dst_null_map)[old_offsets_size + i] = static_cast<UInt8>(1);
        }
        dst_offsets[old_offsets_size + i] = total_chars_size;
    }

    /// Second pass: resize chars once and copy string data.
    dst_chars.resize(total_chars_size);
    size_t current_offset = old_chars_size;
    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            size_t src_offset = src_offsets[ssize_t(pos) - 1];
            size_t src_size = src_offsets[pos] - src_offset;
            memcpy(&dst_chars[current_offset], &src_chars[src_offset], src_size);
            current_offset += src_size;
        }
    }
}

/// Specialized value extractor for ColumnFixedString, with optional Nullable support.
void extractValuesFixedString(
    const ColumnFixedString & values_column,
    ColumnFixedString & result,
    const PaddedPODArray<size_t> & matched_positions,
    const NullMap * src_null_map = nullptr,
    NullMap * dst_null_map = nullptr)
{
    size_t n = values_column.getN();
    const auto & src_chars = values_column.getChars();
    auto & dst_chars = result.getChars();
    size_t num_rows = matched_positions.size();

    size_t old_chars_size = dst_chars.size();
    size_t old_num_rows = old_chars_size / n;
    dst_chars.resize(old_chars_size + num_rows * n);
    if (dst_null_map)
        dst_null_map->resize(old_num_rows + num_rows);

    for (size_t i = 0; i < num_rows; ++i)
    {
        size_t pos = matched_positions[i];
        if (pos != KEY_NOT_FOUND)
        {
            memcpy(&dst_chars[old_chars_size + i * n], &src_chars[pos * n], n);
            if (dst_null_map)
                (*dst_null_map)[old_num_rows + i] = (*src_null_map)[pos];
        }
        else
        {
            memset(&dst_chars[old_chars_size + i * n], 0, n);
            if (dst_null_map)
                (*dst_null_map)[old_num_rows + i] = static_cast<UInt8>(1);
        }
    }
}

/// Dispatches to the appropriate specialized value extractor based on the value column type.
/// For Nullable columns, unwraps to the nested column and passes null map pointers
/// to the same extractors used for non-nullable columns.
void extractValuesDispatch(
    const IColumn & values_column,
    IColumn & result,
    const PaddedPODArray<size_t> & matched_positions)
{
    /// Unwrap Nullable if present to get the underlying data column and null maps.
    const IColumn * data_column = &values_column;
    IColumn * result_data_column = &result;
    const NullMap * src_null_map = nullptr;
    NullMap * dst_null_map = nullptr;

    if (const auto * nullable_values = typeid_cast<const ColumnNullable *>(&values_column))
    {
        auto & nullable_result = assert_cast<ColumnNullable &>(result);
        data_column = &nullable_values->getNestedColumn();
        result_data_column = &nullable_result.getNestedColumn();
        src_null_map = &nullable_values->getNullMapData();
        dst_null_map = &nullable_result.getNullMapData();
    }

    TypeIndex type_id = data_column->getDataType();

    switch (type_id)
    {
#define DISPATCH_VECTOR(T) \
        case TypeIndex::T: \
        { \
            using ColType = ColumnVector<T>; \
            extractValuesVector<T>( \
                assert_cast<const ColType &>(*data_column), \
                assert_cast<ColType &>(*result_data_column), \
                matched_positions, src_null_map, dst_null_map); \
            return; \
        }

        DISPATCH_VECTOR(UInt8)
        DISPATCH_VECTOR(UInt16)
        DISPATCH_VECTOR(UInt32)
        DISPATCH_VECTOR(UInt64)
        DISPATCH_VECTOR(Int8)
        DISPATCH_VECTOR(Int16)
        DISPATCH_VECTOR(Int32)
        DISPATCH_VECTOR(Int64)
        DISPATCH_VECTOR(Float32)
        DISPATCH_VECTOR(Float64)
#undef DISPATCH_VECTOR

        case TypeIndex::String:
        {
            extractValuesString(
                assert_cast<const ColumnString &>(*data_column),
                assert_cast<ColumnString &>(*result_data_column),
                matched_positions, src_null_map, dst_null_map);
            return;
        }
        case TypeIndex::FixedString:
        {
            extractValuesFixedString(
                assert_cast<const ColumnFixedString &>(*data_column),
                assert_cast<ColumnFixedString &>(*result_data_column),
                matched_positions, src_null_map, dst_null_map);
            return;
        }
        default:
        {
            /// Fallback for all other column types (handles both Nullable and non-Nullable).
            extractValuesGeneric(values_column, result, matched_positions);
        }
    }
}

}

void extractKeyValueFromMap(
    const IColumn & nested_column,
    const IColumn & key,
    IColumn & result,
    size_t start,
    size_t end)
{
    const auto & array_column = assert_cast<const ColumnArray &>(nested_column);
    const auto & tuple_column = assert_cast<const ColumnTuple &>(array_column.getData());
    const auto & offsets = array_column.getOffsets();
    const auto & keys_column = tuple_column.getColumn(0);
    const auto & values_column = tuple_column.getColumn(1);

    /// Phase 1: find the position of the requested key in each row.
    PaddedPODArray<size_t> matched_positions;
    findKeyPositionsDispatch(keys_column, offsets, key, start, end, matched_positions);

    /// Phase 2: extract values at the matched positions.
    extractValuesDispatch(values_column, result, matched_positions);
}

std::optional<std::pair<String, String>> tryParseMapSubcolumnName(const String & column_name)
{
    static constexpr std::string_view key_marker = ".key_";

    auto pos = column_name.find(key_marker);
    if (pos == String::npos)
        return std::nullopt;

    auto map_column_name = column_name.substr(0, pos);
    auto serialized_key = column_name.substr(pos + key_marker.size());
    return std::pair{std::move(map_column_name), std::move(serialized_key)};
}

}
