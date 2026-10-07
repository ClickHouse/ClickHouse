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
    first_row_index_in_input = blocks.rowIndex(first_row);
    first_peer_group_index_in_input = blocks.blockAt(first_row.block).index.peer_group_indexes[first_row.row];
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

int64_t Partition::size(const SlidingBlocks & blocks) const
{
    return rowIndex(blocks, partition_bounds.end);
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
        return RowPoint{.location = row, .row_index_in_partition = rowIndex(blocks, row), .peer_group_index_in_partition = peerGroupIndex(blocks, blocks.prev(row)) + 1};
    }

    return RowPoint{.location = row, .row_index_in_partition = rowIndex(blocks, row), .peer_group_index_in_partition = peerGroupIndex(blocks, row)};
}

}
