#include <Processors/Transforms/Window/Partition.h>

namespace DB
{

Partition::Partition()
    : partition_bounds{.fully_visible = true}
{
}

void Partition::beginAt(const SlidingBlocks & blocks, RowNumber first_row)
{
    partition_bounds = PartitionBounds{.start = first_row, .end = blocks.next(first_row), .fully_visible = false};
    advance(blocks);
}

void Partition::advance(const SlidingBlocks & blocks)
{
    RowNumber & end = partition_bounds.end;
    while (!partition_bounds.fully_visible && end < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(end.block);

        int64_t next_start = end.row;
        while (next_start < block.rows_count && !block.index.partition_starts[next_start])
            ++next_start;

        end = blocks.move(end, next_start - end.row);
        if (next_start < block.rows_count)
            finish();
    }
}

void Partition::finish()
{
    partition_bounds.fully_visible = true;
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
}

}
