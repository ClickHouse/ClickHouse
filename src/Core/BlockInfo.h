#pragma once

#include <common/types.h>

#include <unordered_map>
#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/** More information about the block.
  */
struct BlockInfo
{
    /** is_overflows:
      * After running GROUP BY ... WITH TOTALS with the max_rows_to_group_by and group_by_overflow_mode = 'any' settings,
      *  a row is inserted in the separate block with aggregated values that have not passed max_rows_to_group_by.
      * If it is such a block, then is_overflows is set to true for it.
      */

    /** bucket_num:
      * When using the two-level aggregation method, data with different key groups are scattered across different buckets.
      * In this case, the bucket number is indicated here. It is used to optimize the merge for distributed aggregation.
      * Otherwise -1.
      */

#define APPLY_FOR_BLOCK_INFO_FIELDS(M) \
    M(bool,     is_overflows,     false,     1) \
    M(Int32,    bucket_num,     -1,     2)

#define DECLARE_FIELD(TYPE, NAME, DEFAULT, FIELD_NUM) \
    TYPE NAME = DEFAULT;

    APPLY_FOR_BLOCK_INFO_FIELDS(DECLARE_FIELD)

#undef DECLARE_FIELD

    /// Write the values in binary form. NOTE: You could use protobuf, but it would be overkill for this case.
    void write(WriteBuffer & out) const;

    /// Read the values in binary form.
    void read(ReadBuffer & in);
};

/// Block extension to support delayed defaults. AddingDefaultsBlockInputStream uses it to replace missing values with column defaults.
class BlockMissingValues
{
public:
    using RowsBitMask = std::vector<bool>; /// a bit per row for a column

    /// Get mask for column, column_idx is index inside corresponding block
    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    /// Check that we have to replace default value at least in one of columns
    bool hasDefaultBits(size_t column_idx) const;
    void setBit(size_t column_idx, size_t row_idx);
    bool empty() const { return rows_mask_by_column_id.empty(); }
    size_t size() const { return rows_mask_by_column_id.size(); }
    void clear() { rows_mask_by_column_id.clear(); }

private:
    using RowsMaskByColumnId = std::unordered_map<size_t, RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less columns and rows then related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

}
