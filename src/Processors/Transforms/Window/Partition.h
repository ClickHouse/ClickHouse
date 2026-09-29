#pragma once

#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

/// The rows of the partition seen so far.
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

    void beginAt(RowNumber first_row);
    void advance(const SlidingBlock & block);
    void finish();

    const PartitionBounds & bounds() const;

private:
    PartitionBounds found;
};

}
