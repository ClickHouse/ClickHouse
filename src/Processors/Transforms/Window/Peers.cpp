#include <Processors/Transforms/Window/Peers.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>

namespace DB
{

namespace
{

/// Moves the end past the rows of the peer group, block by block in the stored peer group indexes, to the first row of the next group or to the partition end.
void advancePeerGroupEnd(const SlidingBlocks & blocks, const SlidingBlock & current_block, const PartitionBounds & partition, int64_t group, PeersBounds & peers)
{
    RowPoint & end = peers.end;
    const RowNumber partition_end = partition.end.location;
    while (end.location < partition_end)
    {
        const SlidingBlock & block = end.location.block == current_block.block_number ? current_block : blocks.blockAt(end.location.block);
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
        end.location = rows_end == block.rows_count ? RowNumber{end.location.block + 1, 0} : partition_end;
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
    const SlidingBlock & block = blocks.blockAt(current_row.location.block);
    const int64_t row = current_row.location.row;
    const int64_t row_index = current_row.row_index_in_partition;
    const int64_t group = block.index.peer_group_indexes[row];
    current_row.peer_group_index_in_partition = group;

    if (group != peers_bounds.start.peer_group_index_in_partition)
    {
        peers_bounds.start = RowPoint{.location = {block.block_number, row}, .row_index_in_partition = row_index, .peer_group_index_in_partition = group};
        peers_bounds.end = RowPoint{.location = {block.block_number, row}, .row_index_in_partition = row_index, .peer_group_index_in_partition = group};
        peers_bounds.fully_visible = false;
    }

    if (!peers_bounds.fully_visible)
        advancePeerGroupEnd(blocks, block, partition, group, peers_bounds);
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
