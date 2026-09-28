#include <Processors/Transforms/Window/SlidingBlocks.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <ranges>

namespace DB
{

SlidingBlock & SlidingBlocks::add(Chunk chunk, const WindowTransformParams & params)
{
    SlidingBlock block{
        .original_input_columns = chunk.getColumns(),
        .input_columns = chunk.getColumns(),
        .output_columns = {},
        .rows_count = static_cast<int64_t>(chunk.getNumRows()),
        .block_number = next_block_number++,
    };

    /// Materialize all requested columns
    for (auto && [column, should_materialize] : std::views::zip(block.input_columns, params.should_materialize))
        if (should_materialize)
            column = recursiveRemoveLowCardinality(column->convertToFullIfWrapped());

    return blocks.emplace_back(std::move(block));
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
