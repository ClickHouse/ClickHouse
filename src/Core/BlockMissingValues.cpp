#include <Core/BlockMissingValues.h>

namespace DB
{

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

size_t BlockMissingValues::size() const
{
    size_t res = 0;
    for (const auto & mask : rows_mask_by_column_id)
        res += !mask.empty();
    return res;
}

}
