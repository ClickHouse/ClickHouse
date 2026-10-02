#include <Processors/Transforms/Window/Peers.h>

namespace DB
{

namespace
{

bool isPeerGroupStart(const SlidingBlocks & blocks, RowNumber row)
{
    return blocks.blockAt(row.block).index.peer_group_starts[row.row];
}

RowPoint advancePeerGroupEnd(const SlidingBlocks & blocks, const PartitionBounds & partition, RowPoint end)
{
    end.location = blocks.next(end.location);
    end.row_index_in_partition += 1;
    if (end.location < partition.end && isPeerGroupStart(blocks, end.location))
        end.peer_group_index_in_partition += 1;

    return end;
}

}

void Peers::enterPartition(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    current_row = RowPoint{.location = partition.start};
    peers_bounds = PeersBounds{.start = current_row, .end = advancePeerGroupEnd(blocks, partition, current_row), .fully_visible = false};
}

void Peers::advancePeerGroupBounds(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    /// Check if current row points to a next group.
    if (peers_bounds.start.location != current_row.location && isPeerGroupStart(blocks, current_row.location))
    {
        current_row.peer_group_index_in_partition += 1;
        peers_bounds = PeersBounds{.start = current_row, .end = advancePeerGroupEnd(blocks, partition, current_row), .fully_visible = false};
    }

    /// Advance peer group end as much as possible.
    while (peers_bounds.end.location < partition.end && !isPeerGroupStart(blocks, peers_bounds.end.location))
        peers_bounds.end = advancePeerGroupEnd(blocks, partition, peers_bounds.end);

    peers_bounds.fully_visible = peers_bounds.end.location < partition.end || partition.fully_visible;
}

void Peers::advanceCurrentRow(const SlidingBlocks & blocks)
{
    current_row.location = blocks.next(current_row.location);
    current_row.row_index_in_partition += 1;
}

const RowPoint & Peers::current() const
{
    return current_row;
}

const PeersBounds & Peers::bounds() const
{
    return peers_bounds;
}

}
