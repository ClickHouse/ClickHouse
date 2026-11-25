#include <Processors/Transforms/Window/Partition.h>

#include <algorithm>

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

    /// Single-row partitions call this once per row. Their end is visible right away, so this check keeps `advance`, which
    /// calls `std::find` and therefore saves registers on every call, off that path.
    const RowNumber & end = partition_bounds.end;
    if (end < blocks.end() && blocks.blockAt(end.block).index.partition_starts[end.row])
    {
        finish();
        return;
    }

    advance(blocks);
}

void Partition::advance(const SlidingBlocks & blocks)
{
    RowNumber & end = partition_bounds.end;
    while (!partition_bounds.end_visible && end < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(end.block);

        /// `std::find` on a vector of bools scans whole words instead of checking every bit.
        const auto & partition_starts = block.index.partition_starts;
        const int64_t next_start = std::find(partition_starts.begin() + end.row, partition_starts.end(), true) - partition_starts.begin();

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
