#include <Processors/Transforms/Window/Partition.h>

namespace DB
{

Partition::Partition()
    : partition_bounds{.start = RowNumber{0, 0}, .end = RowNumber{0, 0}, .rows_count = 0, .start_visible = true, .end_visible = true}
{
}

void Partition::beginAt(const SlidingBlocks & blocks, RowNumber first_row)
{
    partition_bounds.start = first_row;
    partition_bounds.end = blocks.next(first_row);
    partition_bounds.rows_count = 1;
    partition_bounds.start_visible = true;
    partition_bounds.end_visible = false;
    advance(blocks);
}

void Partition::advance(const SlidingBlocks & blocks)
{
    RowNumber & end = partition_bounds.end;
    while (!partition_bounds.end_visible && end < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(end.block);

        int64_t next_start = end.row;
        while (next_start < block.rows_count && !block.index.partition_starts[next_start])
            ++next_start;

        const int64_t rows_passed = next_start - end.row;
        partition_bounds.rows_count += rows_passed;
        end = blocks.move(end, rows_passed);
        if (next_start < block.rows_count)
            finish();
    }
}

void Partition::finish()
{
    partition_bounds.end_visible = true;
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
}

}
