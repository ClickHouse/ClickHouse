#pragma once

#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

/// Rows-range of the partition seen so far.
struct PartitionBounds
{
    RowNumber start;
    RowNumber end;
    int64_t rows_count = 0;
    bool start_visible = false;
    bool end_visible = false;
};

class Partition
{
public:
    Partition();

    void beginAt(const SlidingBlocks & blocks, RowNumber first_row);
    void advance(const SlidingBlocks & blocks);
    void finish();

    [[nodiscard]] const PartitionBounds & bounds() const;

private:
    PartitionBounds partition_bounds;
};

}
