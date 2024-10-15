#pragma once

#include <vector>
#include <boost/dynamic_bitset.hpp>
#include <Processors/Chunk.h>

namespace DB
{

/// Block extension to support delayed defaults.
/// AddingDefaultsTransform uses it to replace missing values with column defaults.
class BlockMissingValues
{
public:
    using RowsBitMask = boost::dynamic_bitset<>; /// a bit per row for a column
    explicit BlockMissingValues(size_t num_columns) : rows_mask_by_column_id(num_columns) {}

    /// Get mask for column, column_idx is index inside corresponding block
    const RowsBitMask & getDefaultsBitmask(size_t column_idx) const;
    /// Check that we have to replace default value at least in one of columns
    bool hasDefaultBits(size_t column_idx) const;
    /// Set bit for a specified row in a single column.
    void setBit(size_t column_idx, size_t row_idx);
    /// Set bits for all rows in a single column.
    void setBits(size_t column_idx, size_t rows);

    void clear();
    bool empty() const;
    size_t size() const;

private:
    using RowsMaskByColumnId = std::vector<RowsBitMask>;

    /// If rows_mask_by_column_id[column_id][row_id] is true related value in Block should be replaced with column default.
    /// It could contain less rows than related block.
    RowsMaskByColumnId rows_mask_by_column_id;
};

/// The same as above but can be used as a chunk info.
class ChunkMissingValues : public BlockMissingValues, public ChunkInfoCloneable<ChunkMissingValues>
{
};

}
