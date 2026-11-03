#include <Processors/Transforms/Window/Partition.h>

namespace DB
{

Partition::Partition()
    : partition_bounds{.start = RowPoint{.location = RowNumber{0, 0}}, .end = RowPoint{.location = RowNumber{0, 0}}, .fully_visible = true}
{
}

void Partition::beginAt(const SlidingBlocks & blocks, RowNumber first_row)
{
    partition_bounds = PartitionBounds{
        .start = RowPoint{.location = first_row},
        .end = RowPoint{.location = blocks.next(first_row), .row_index_in_partition = 1, .peer_group_index_in_partition = 1},
        .fully_visible = false,
    };
}

void Partition::advance(const SlidingBlocks & blocks)
{
    RowPoint & end = partition_bounds.end;
    while (!partition_bounds.fully_visible && end.location < blocks.end())
    {
        const SlidingBlock & block = blocks.blockAt(end.location.block);

        int64_t next_start = end.location.row;
        while (next_start < block.rows_count && !block.index.partition_starts[next_start])
            ++next_start;

        const int64_t rows_passed = next_start - end.location.row;
        if (rows_passed > 0)
        {
            end.peer_group_index_in_partition = block.index.peer_group_indexes[next_start - 1] + 1;
            end.row_index_in_partition += rows_passed;
            end.location = blocks.move(end.location, rows_passed);
        }
        if (next_start < block.rows_count)
            finish();
    }
}

void Partition::finish()
{
    partition_bounds.fully_visible = true;
}

const PartitionBounds & Partition::bounds() const
{
    return partition_bounds;
}

}
