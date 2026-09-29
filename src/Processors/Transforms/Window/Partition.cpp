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
    found = PartitionBounds{.start = first_row, .end = RowNumber{first_row.block, first_row.row + 1}, .fully_visible = false};
}

void Partition::advance(const SlidingBlock & block)
{
    if (found.fully_visible)
        return;

    found.end = advancePartitionEnd(block, found.end);
    found.fully_visible = found.end.block == block.block_number;
}

void Partition::finish(RowNumber data_end)
{
    found.end = data_end;
    found.fully_visible = true;
}

const PartitionBounds & Partition::bounds() const
{
    return found;
}

}
