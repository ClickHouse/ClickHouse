#include <Processors/Transforms/Window/SlidingBlocks.h>

#include <base/arithmeticOverflow.h>

namespace DB
{

SlidingBlock & SlidingBlocks::add(Chunk chunk, Columns materialized_columns, SlidingIndex index)
{
    int64_t rows_count = chunk.getNumRows();
    Columns input_columns = chunk.detachColumns();

    return blocks.emplace_back(SlidingBlock{
        .input_columns = std::move(input_columns),
        .materialized_columns = std::move(materialized_columns),
        .rows_count = rows_count,
        .block_number = next_block_number++,
        .index = std::move(index),
        .result_columns = {},
    });
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

std::optional<RowNumber> SlidingBlocks::move(RowNumber row, int64_t offset) const
{
    /// The target row counted from the start of the row's block
    int64_t target = 0;
    if (common::addOverflow(row.row, offset, target))
        return std::nullopt;

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
