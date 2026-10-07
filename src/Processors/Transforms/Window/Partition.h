#pragma once

#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

/// Rows-range of the partition seen so far.
struct PartitionBounds
{
    RowNumber start;
    RowNumber end;
    bool fully_visible = false;
};

class Partition
{
public:
    Partition();

    void beginAt(const SlidingBlocks & blocks, RowNumber first_row);
    void advance(const SlidingBlocks & blocks);
    void finish(const SlidingBlocks & blocks, RowNumber data_end);

    const PartitionBounds & bounds() const;

    int64_t size() const;
    int64_t rowIndex(const SlidingBlocks & blocks, RowNumber row) const;
    int64_t peerGroupIndex(const SlidingBlocks & blocks, RowNumber row) const;
    RowPoint pointAt(const SlidingBlocks & blocks, RowNumber row) const;

private:
    PartitionBounds partition_bounds;
    int64_t rows_count = 0;
    int64_t first_row_index_in_input = 0;
    int64_t first_peer_group_index_in_input = 0;
};

}
