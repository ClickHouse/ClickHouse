#include <Processors/Transforms/Window/Peers.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>

namespace DB
{

namespace
{

/// Moves the end past the rows of the peer group, block by block in the stored peer group indexes, to the first row of the next group or to the partition end.
void advancePeerGroupEnd(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t group, PeersBounds & peers)
{
    RowPoint & end = peers.end;
    const RowNumber partition_end = partition.end.location;
    while (end.location < partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(end.location.block);
        const std::vector<int64_t> & groups = block.index.peer_group_indexes;
        const int64_t rows_end = partition_end.block == end.location.block ? partition_end.row : block.rows_count;
        const int64_t row = groups[end.location.row] != group
            ? end.location.row
            : static_cast<int64_t>(findEqualRangeEndAssumeSorted(end.location.row, rows_end, 16, [&](size_t i) { return groups[i] == group; }));

        end.row_index_in_partition += row - end.location.row;
        if (row < rows_end)
        {
            end.location.row = row;
            end.peer_group_index_in_partition = groups[row];
            peers.fully_visible = true;
            return;
        }
        end.location = blocks.move(end.location, row - end.location.row);
    }

    end.peer_group_index_in_partition = group + 1;
    peers.fully_visible = partition.fully_visible;
}

}

void Peers::enterPartition(const PartitionBounds & partition)
{
    current_row = partition.start;
    peers_bounds = PeersBounds{.start = current_row, .end = current_row, .fully_visible = false};
}

void Peers::advancePeerGroupBounds(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    if (!peers_bounds.fully_visible)
        advancePeerGroupEnd(blocks, partition, peers_bounds.start.peer_group_index_in_partition, peers_bounds);

    if (current_row.row_index_in_partition == peers_bounds.end.row_index_in_partition)
    {
        current_row.peer_group_index_in_partition = peers_bounds.end.peer_group_index_in_partition;
        peers_bounds.start = peers_bounds.end;
        advancePeerGroupEnd(blocks, partition, current_row.peer_group_index_in_partition, peers_bounds);
    }
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
