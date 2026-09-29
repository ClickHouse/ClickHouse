#include <Processors/Transforms/Window/SlidingBlocks.h>

namespace DB
{

SlidingBlock & SlidingBlocks::add(Chunk chunk, Columns materialized_columns, SlidingIndex index)
{
    return blocks.emplace_back(SlidingBlock{
        .input_columns = chunk.getColumns(),
        .materialized_columns = std::move(materialized_columns),
        .rows_count = static_cast<int64_t>(chunk.getNumRows()),
        .block_number = next_block_number++,
        .index = std::move(index),
        .result_columns = {},
    });
}

void SlidingBlocks::pop()
{
    blocks.pop_front();
}

const SlidingBlock & SlidingBlocks::blockAt(int64_t block_number) const
{
    chassert(!blocks.empty());
    chassert(block_number >= blocks.front().block_number);
    chassert(block_number - blocks.front().block_number < static_cast<int64_t>(blocks.size()));
    return blocks[block_number - blocks.front().block_number];
}

RowNumber SlidingBlocks::begin() const
{
    return {blocks.empty() ? next_block_number : blocks.front().block_number, 0};
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

std::optional<RowNumber> SlidingBlocks::move(RowNumber row, int64_t offset) const
{
    /// The target row counted from the start of the row's block
    int64_t target = row.row + offset;

    while (target < 0 && row.block > begin().block)
    {
        --row.block;
        target += blockAt(row.block).rows_count;
    }

    while (row.block < end().block && target >= blockAt(row.block).rows_count)
    {
        target -= blockAt(row.block).rows_count;
        ++row.block;
    }

    if (target < 0)
        return std::nullopt;

    if (target > 0 && row.block == end().block)
        return std::nullopt;

    return RowNumber{row.block, target};
}

}
