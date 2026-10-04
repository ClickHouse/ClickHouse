#include <Processors/Transforms/Window/Partition.h>

namespace DB
{

namespace
{

bool isPartitionStart(const SlidingBlocks & blocks, RowNumber row)
{
    return blocks.blockAt(row.block).index.partition_starts[row.row];
}

}

Partition::Partition()
{
    finish(RowNumber{0, 0});
}

void Partition::beginAt(const SlidingBlocks & blocks, RowNumber first_row)
{
    partition_bounds = PartitionBounds{.start = first_row, .end = blocks.next(first_row), .fully_visible = false};
}

void Partition::advance(const SlidingBlocks & blocks)
{
    while (!partition_bounds.fully_visible && partition_bounds.end < blocks.end())
    {
        if (isPartitionStart(blocks, partition_bounds.end))
            partition_bounds.fully_visible = true;
        else
            partition_bounds.end = blocks.next(partition_bounds.end);
    }
}

void Partition::finish(RowNumber data_end)
{
    partition_bounds.end = data_end;
    partition_bounds.fully_visible = true;
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
}

}
