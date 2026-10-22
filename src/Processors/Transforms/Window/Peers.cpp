#include <Processors/Transforms/Window/Peers.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>

namespace DB
{

namespace
{

/// Moves the end past the rows of the peer group inside the block, up to `rows_end`, and on to the next block when the rows end there; whether the group ends before `rows_end`.
bool advancePeerGroupEndInBlock(const SlidingBlock & block, int64_t group, int64_t rows_end, RowPoint & end)
{
    const std::vector<int64_t> & groups = block.index.peer_group_indexes;
    const int64_t row = groups[end.location.row] != group
        ? end.location.row
        : static_cast<int64_t>(findEqualRangeEndAssumeSorted(end.location.row, rows_end, 16, [&](size_t i) { return groups[i] == group; }));

    end.row_index_in_partition += row - end.location.row;
    end.location = row == block.rows_count ? RowNumber{end.location.block + 1, 0} : RowNumber{end.location.block, row};
    if (row == rows_end)
        return false;

    end.peer_group_index_in_partition = groups[row];
    return true;
}

/// Moves the end past the rows of the peer group, block by block in the stored peer group indexes, to the first row of the next group or to the partition end.
void advancePeerGroupEnd(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t group, PeersBounds & peers)
{
    RowPoint & end = peers.end;
    const RowNumber partition_end = partition.end.location;
    while (end.location < partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(end.location.block);
        const int64_t rows_end = partition_end.block == end.location.block ? partition_end.row : block.rows_count;
        if (advancePeerGroupEndInBlock(block, group, rows_end, end))
        {
            peers.fully_visible = true;
            return;
        }
    }

    end.peer_group_index_in_partition = group + 1;
    peers.fully_visible = partition.fully_visible;
}

}

void Peers::enterPartition(RowNumber partition_start)
{
    current_row = RowPoint{.location = partition_start};
    peers_bounds.start = RowPoint{.location = partition_start};
    peers_bounds.end = RowPoint{.location = partition_start};
    peers_bounds.fully_visible = false;
}

void Peers::advancePeerGroupBounds(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    if (!peers_bounds.fully_visible)
        advancePeerGroupEnd(blocks, partition, peers_bounds.start.peer_group_index_in_partition, peers_bounds);

    if (current_row.row_index_in_partition >= peers_bounds.end.row_index_in_partition)
    {
        current_row.peer_group_index_in_partition = peers_bounds.end.peer_group_index_in_partition;
        peers_bounds.start = peers_bounds.end;
        advancePeerGroupEnd(blocks, partition, current_row.peer_group_index_in_partition, peers_bounds);
    }
}

void Peers::advanceCurrentRow(const SlidingBlocks & blocks, const PartitionBounds & partition)
{
    const SlidingBlock & block = blocks.blockAt(current_row.location.block);
    const int64_t row = current_row.location.row + 1;
    const int64_t row_index = current_row.row_index_in_partition + 1;
    const int64_t rows_end = partition.end.location.block == block.block_number ? partition.end.location.row : block.rows_count;
    if (row == rows_end)
    {
        current_row.location = row == block.rows_count ? RowNumber{block.block_number + 1, 0} : RowNumber{block.block_number, row};
        current_row.row_index_in_partition = row_index;
        return;
    }

    const int64_t group = block.index.peer_group_indexes[row];
    current_row = RowPoint{.location = {block.block_number, row}, .row_index_in_partition = row_index, .peer_group_index_in_partition = group};
    if (group == peers_bounds.start.peer_group_index_in_partition)
        return;

    peers_bounds.start = RowPoint{.location = {block.block_number, row}, .row_index_in_partition = row_index, .peer_group_index_in_partition = group};
    peers_bounds.end = RowPoint{.location = {block.block_number, row}, .row_index_in_partition = row_index, .peer_group_index_in_partition = group};
    peers_bounds.fully_visible = advancePeerGroupEndInBlock(block, group, rows_end, peers_bounds.end);
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
