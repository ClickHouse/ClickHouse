#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

SlidingBlock & SlidingBlocks::add(Chunk chunk, Columns materialized_columns, SlidingIndex index)
{
    int64_t rows_count = chunk.getNumRows();
    Columns input_columns = chunk.detachColumns();

    return blocks.emplace_back(std::move(input_columns), std::move(materialized_columns), rows_count, next_block_number++, std::move(index));
}

void SlidingBlocks::pop()
{
    blocks.pop_front();
    ++first_block_number;
}

const SlidingBlock & SlidingBlocks::blockAt(int64_t block_number) const
{
    chassert(block_number >= first_block_number);
    chassert(block_number < next_block_number);
    return blocks[block_number - first_block_number];
}

RowNumber SlidingBlocks::begin() const
{
    return {first_block_number, 0};
}

RowNumber SlidingBlocks::end() const
{
    return {next_block_number, 0};
}

RowNumber SlidingBlocks::next(RowNumber row) const
{
    if (row.row + 1 >= blockAt(row.block).rows_count)
        return {row.block + 1, 0};
    else
        return {row.block, row.row + 1};
}

RowNumber SlidingBlocks::prev(RowNumber row) const
{
    if (row.row == 0)
        return {row.block - 1, blockAt(row.block - 1).rows_count - 1};
    else
        return {row.block, row.row - 1};
}

RowNumber SlidingBlocks::move(RowNumber row, int64_t offset) const
{
    /// The target row counted from the start of the row's block
    int64_t target = row.row + offset;
    while (target < 0)
    {
        --row.block;
        target += blockAt(row.block).rows_count;
    }

    while (row.block < end().block && target >= blockAt(row.block).rows_count)
    {
        target -= blockAt(row.block).rows_count;
        ++row.block;
    }

    chassert(row.block < end().block || target == 0);
    return RowNumber{row.block, target};
}

}
