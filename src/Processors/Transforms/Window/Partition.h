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
    void finish();

    const PartitionBounds & bounds() const;
    const RowPoint & endPoint() const;

    int64_t size() const;
    int64_t peerGroupIndex(const SlidingBlocks & blocks, RowNumber row) const;

private:
    PartitionBounds partition_bounds;
    RowPoint end_point;
    int64_t rows_count = 0;
    int64_t last_peer_group_index = 0;
};

}
