#pragma once

#include <Columns/IColumn_fwd.h>

#include <Processors/Chunk.h>
#include <Processors/Transforms/Window/SlidingIndexes.h>

#include <deque>
#include <cstdint>
#include <optional>

namespace DB
{

struct RowNumber
{
    int64_t block = 0;
    int64_t row = 0;

    auto operator<=>(const RowNumber &) const noexcept = default;
};

struct RowPoint
{
    RowNumber location;
    int64_t row_index_in_partition = 0;
    int64_t peer_group_index_in_partition = 0;
};

struct SlidingBlock
{
    /// Inputs
    const Columns input_columns;
    const Columns materialized_columns;

    /// Helper data
    const int64_t rows_count = 0;
    const int64_t block_number = 0;
    const SlidingIndex index;

    /// Output
    MutableColumns result_columns;
};

class SlidingBlocks
{
public:
    SlidingBlock & add(Chunk chunk, Columns materialized_columns, SlidingIndex index);
    const SlidingBlock & blockAt(int64_t block_number) const;
    void pop();

    RowNumber begin() const;
    RowNumber end() const;
    RowNumber next(RowNumber row) const;
    RowNumber prev(RowNumber row) const;
    std::optional<RowNumber> move(RowNumber row, int64_t offset) const;

private:
    std::deque<SlidingBlock> blocks;
    int64_t next_block_number = 0;
};

}
