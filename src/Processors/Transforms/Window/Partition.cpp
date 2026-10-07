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
        partition_bounds.fully_visible = next_start != starts.end();
        partition_bounds.end = blocks.move(partition_bounds.end, next_start - starts.begin() - partition_bounds.end.row);
    }
    rows_count = rowIndex(blocks, partition_bounds.end);
}

void Partition::finish(const SlidingBlocks & blocks, RowNumber data_end)
{
    partition_bounds.end = data_end;
    partition_bounds.fully_visible = true;
    rows_count = rowIndex(blocks, data_end);
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
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

RowPoint Partition::pointAt(const SlidingBlocks & blocks, RowNumber row) const
{
    if (row == blocks.end())
    {
        chassert(partition_bounds.fully_visible && row == partition_bounds.end);
        return RowPoint{.location = row, .row_index_in_partition = rows_count, .peer_group_index_in_partition = peerGroupIndex(blocks, blocks.prev(row)) + 1};
    }

    const SlidingBlock & block = blocks.blockAt(row.block);
    return RowPoint{
        .location = row,
        .row_index_in_partition = block.first_row_index_in_input + row.row - first_row_index_in_input,
        .peer_group_index_in_partition = block.index.peer_group_indexes[row.row] - first_peer_group_index_in_input,
    };
}

}
