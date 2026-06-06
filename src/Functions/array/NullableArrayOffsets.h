#pragma once

#include <Columns/ColumnArray.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>

#include <algorithm>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace NullableArrayOffsets
{

inline bool rowNullMapHasAnyNull(const ColumnUInt8 & row_null_map)
{
    const auto & data = row_null_map.getData();
    return std::find(data.begin(), data.end(), UInt8(1)) != data.end();
}

inline ColumnPtr emptyNullRows(const ColumnArray & array, const ColumnUInt8 * row_null_map, size_t num_rows)
{
    if (!row_null_map)
        return nullptr;

    const auto & null_map_data = row_null_map->getData();
    if (null_map_data.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nullable array null map size {} does not match row count {}",
            null_map_data.size(),
            num_rows);

    if (!rowNullMapHasAnyNull(*row_null_map))
        return nullptr;

    const auto & source_offsets = array.getOffsets();
    if (source_offsets.size() != num_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Nullable array offsets size {} does not match row count {}",
            source_offsets.size(),
            num_rows);

    auto result_data = array.getData().cloneEmpty();
    auto result_offsets_column = ColumnArray::ColumnOffsets::create();
    auto & result_offsets = result_offsets_column->getData();
    result_offsets.reserve(num_rows);

    ColumnArray::Offset source_previous_offset = 0;
    ColumnArray::Offset result_current_offset = 0;
    for (size_t row = 0; row < num_rows; ++row)
    {
        const ColumnArray::Offset source_current_offset = source_offsets[row];
        const size_t source_size = source_current_offset - source_previous_offset;

        if (!null_map_data[row])
        {
            result_data->insertRangeFrom(array.getData(), source_previous_offset, source_size);
            result_current_offset += source_size;
        }

        result_offsets.push_back(result_current_offset);
        source_previous_offset = source_current_offset;
    }

    return ColumnArray::create(std::move(result_data), std::move(result_offsets_column));
}

}
}
