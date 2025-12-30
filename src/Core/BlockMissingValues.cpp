#include <Core/BlockMissingValues.h>

#include <Common/PODArray.h>

namespace DB
{

void BlockMissingValues::init(size_t num_columns)
{
    rows_mask_by_column_id.resize(num_columns);
}

void BlockMissingValues::setBit(size_t column_idx, size_t row_idx)
{
    RowsBitMask & mask = rows_mask_by_column_id[column_idx];
    mask.resize(row_idx + 1);
    mask.set(row_idx, true);
}

void BlockMissingValues::setBits(size_t column_idx, size_t rows)
{
    auto & mask = rows_mask_by_column_id[column_idx];
    mask.set(0, std::min(mask.size(), rows), true);
    mask.resize(rows, true);
}

void BlockMissingValues::setBitsFromNullMap(size_t column_idx, const PaddedPODArray<UInt8> & null_map)
{
    RowsBitMask & mask = rows_mask_by_column_id[column_idx];
    mask.resize(null_map.size());
    for (size_t i = 0; i < null_map.size(); ++i)
        mask.set(i, bool(null_map[i]));
}

const BlockMissingValues::RowsBitMask & BlockMissingValues::getDefaultsBitmask(size_t column_idx) const
{
    return rows_mask_by_column_id[column_idx];
}

bool BlockMissingValues::hasDefaultBits(size_t column_idx) const
{
    /// It is correct because we resize bitmask only when set a bit.
    return !rows_mask_by_column_id[column_idx].empty();
}

void BlockMissingValues::clear()
{
    for (auto & mask : rows_mask_by_column_id)
        mask.clear();
}

bool BlockMissingValues::empty() const
{
    return std::ranges::all_of(rows_mask_by_column_id, [&](const auto & mask)
    {
        return mask.empty();
    });
}

size_t BlockMissingValues::getNumColumns() const
{
    return rows_mask_by_column_id.size();
}

}
