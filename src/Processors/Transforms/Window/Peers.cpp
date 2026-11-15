#include <Processors/Transforms/Window/Peers.h>
#include <Common/VectorWithMemoryTracking.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>

namespace DB
{

namespace
{

int64_t peerGroupAt(const SlidingBlocks & blocks, RowNumber row)
{
    return blocks.blockAt(row.block).index.peer_group_indexes[row.row];
}

/// Moves the end past the rows of the peer group inside the block, up to `rows_end`, and on to the next block when the rows end there; whether the group ends before `rows_end`.
bool advancePeerGroupEndInBlock(const SlidingBlock & block, int64_t group, int64_t rows_end, RowPoint & end)
{
    const VectorWithMemoryTracking<int64_t> & groups = block.index.peer_group_indexes;
    const int64_t row = groups[end.location.row] != group
        ? end.location.row
        : static_cast<int64_t>(findEqualRangeEndAssumeSorted(end.location.row, rows_end, 16, [&](size_t i) { return groups[i] == group; }));

    end.row_index_in_partition += row - end.location.row;
    end.location = row == block.rows_count ? RowNumber{end.location.block + 1, 0} : RowNumber{end.location.block, row};
    return row < rows_end;
}

/// Moves the end past the rows of the peer group, block by block in the stored peer group indexes, to the first row of the next group or to the partition end.
void advancePeerGroupEnd(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t group, PeersBounds & peers)
{
    RowPoint & end = peers.end;
    const RowNumber partition_end = partition.end;
    while (end.location < partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(end.location.block);
        const int64_t rows_end = partition_end.block == end.location.block ? partition_end.row : block.rows_count;
        if (advancePeerGroupEndInBlock(block, group, rows_end, end))
        {
            peers.end_visible = true;
            return;
        }
    }

    peers.end_visible = partition.end_visible;
}

}

void Peers::enterPartition(RowNumber partition_start)
{
    current_row = RowPoint{.location = partition_start};
    peers_bounds.start = RowPoint{.location = partition_start};
    peers_bounds.end = RowPoint{.location = partition_start};
    peers_bounds.group_index = 0;
    peers_bounds.start_visible = true;
    peers_bounds.end_visible = false;
}

void Peers::advancePeerGroupBounds(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    if (!peers_bounds.end_visible)
        advancePeerGroupEnd(blocks, partition, peers_bounds.group_index, peers_bounds);

    if (current_row.row_index_in_partition >= peers_bounds.end.row_index_in_partition)
    {
        peers_bounds.start = peers_bounds.end;
        peers_bounds.group_index = peerGroupAt(blocks, current_row.location);
        advancePeerGroupEnd(blocks, partition, peers_bounds.group_index, peers_bounds);
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
