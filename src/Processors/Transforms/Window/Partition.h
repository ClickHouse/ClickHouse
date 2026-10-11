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
    void finish(RowNumber data_end);

    const PartitionBounds & bounds() const;

private:
    PartitionBounds partition_bounds;
};

}
