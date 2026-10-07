#include <Processors/Transforms/Window/Partition.h>

#include <algorithm>

namespace DB
{

Partition::Partition()
    : partition_bounds{.start = RowNumber{0, 0}, .end = RowNumber{0, 0}, .fully_visible = true}
{
}

void Partition::beginAt(const SlidingBlocks & blocks, RowNumber first_row)
{
    partition_bounds = PartitionBounds{.start = first_row, .end = blocks.next(first_row), .fully_visible = false};
    const SlidingBlock & block = blocks.blockAt(first_row.block);
    first_row_index_in_input = block.first_row_index_in_input + first_row.row;
    first_peer_group_index_in_input = block.index.peer_group_indexes[first_row.row];
    rows_count = 1;
}

void Partition::advance(const SlidingBlocks & blocks)
{
    while (!partition_bounds.fully_visible && partition_bounds.end < blocks.end())
    {
        const std::vector<bool> & starts = blocks.blockAt(partition_bounds.end.block).index.partition_starts;
        const auto next_start = std::find(starts.begin() + partition_bounds.end.row, starts.end(), true);
        partition_bounds.end = blocks.move(partition_bounds.end, next_start - starts.begin() - partition_bounds.end.row);
        if (next_start != starts.end())
            finish(blocks, partition_bounds.end);
    }
    rows_count = rowIndex(blocks, partition_bounds.end);
}

void Partition::finish(const SlidingBlocks & blocks, RowNumber end)
{
    partition_bounds.end = end;
    partition_bounds.fully_visible = true;
    rows_count = rowIndex(blocks, end);
    end_point = RowPoint{.location = end, .row_index_in_partition = rows_count, .peer_group_index_in_partition = peerGroupIndex(blocks, blocks.prev(end)) + 1};
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
}

const RowPoint & Partition::endPoint() const
{
    return end_point;
}

int64_t Partition::size() const
{
    return rows_count;
}

int64_t Partition::rowIndex(const SlidingBlocks & blocks, RowNumber row) const
{
    return blocks.rowIndex(row) - first_row_index_in_input;
}

int64_t Partition::peerGroupIndex(const SlidingBlocks & blocks, RowNumber row) const
{
    return blocks.blockAt(row.block).index.peer_group_indexes[row.row] - first_peer_group_index_in_input;
}

}
