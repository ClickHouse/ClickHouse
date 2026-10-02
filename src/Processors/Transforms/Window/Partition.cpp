#include <Processors/Transforms/Window/Partition.h>

#include <algorithm>

namespace DB
{

namespace
{

RowNumber advancePartitionEnd(const SlidingBlock & block, RowNumber end)
{
    const auto & starts = block.index.partition_starts;
    const auto next_start = std::find(starts.begin() + end.row, starts.end(), true);
    if (next_start == starts.end())
        return RowNumber{block.block_number + 1, 0};

    return RowNumber{block.block_number, next_start - starts.begin()};
}

}

Partition::Partition()
{
    beginAt(RowNumber{0, 0});
}

void Partition::beginAt(RowNumber first_row)
{
    partition_bounds = PartitionBounds{.start = first_row, .end = RowNumber{first_row.block, first_row.row + 1}, .fully_visible = false};
}

void Partition::advance(const SlidingBlocks & blocks)
{
    while (!partition_bounds.fully_visible && partition_bounds.end < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(partition_bounds.end.block);
        partition_bounds.end = advancePartitionEnd(block, partition_bounds.end);
        partition_bounds.fully_visible = partition_bounds.end.block == block.block_number;
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
