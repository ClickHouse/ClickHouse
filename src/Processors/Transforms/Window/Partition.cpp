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
    rows_count = 1;
    last_peer_group_index = 0;
}

void Partition::advance(const SlidingBlocks & blocks)
{
    while (!partition_bounds.fully_visible && partition_bounds.end < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(partition_bounds.end.block);
        const std::vector<bool> & starts = block.index.partition_starts;
        const int64_t next_start = std::find(starts.begin() + partition_bounds.end.row, starts.end(), true) - starts.begin();
        const int64_t rows_passed = next_start - partition_bounds.end.row;
        if (rows_passed > 0)
        {
            last_peer_group_index = block.index.peer_group_indexes[next_start - 1];
            rows_count += rows_passed;
            partition_bounds.end = blocks.move(partition_bounds.end, rows_passed);
        }
        if (next_start < block.rows_count)
            finish();
    }
}

void Partition::finish()
{
    partition_bounds.fully_visible = true;
    end_point = RowPoint{
        .location = partition_bounds.end,
        .row_index_in_partition = rows_count,
        .peer_group_index_in_partition = last_peer_group_index + 1,
    };
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

int64_t Partition::peerGroupIndex(const SlidingBlocks & blocks, RowNumber row) const
{
    return blocks.blockAt(row.block).index.peer_group_indexes[row.row];
}

}
