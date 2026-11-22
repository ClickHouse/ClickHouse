#pragma once

#include <Columns/IColumn_fwd.h>
#include <Common/DevectorWithMemoryTracking.h>

#include <Processors/Chunk.h>
#include <Processors/Transforms/Window/SlidingIndexes.h>

#include <cstdint>

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
};

struct SlidingBlock
{
    /// Inputs
    Columns input_columns;
    Columns materialized_columns;

    /// Helper data
    int64_t rows_count = 0;
    int64_t block_number = 0;
    int64_t begin_position = 0;
    int64_t end_position = 0;
    SlidingIndex index;
};

class SlidingBlocks
{
public:
    void add(Chunk chunk, Columns materialized_columns, SlidingIndex index);
    void pop();

    [[nodiscard]] const SlidingBlock & blockAt(int64_t block_number) const;
    [[nodiscard]] const SlidingBlock & front() const;

    [[nodiscard]] RowNumber begin() const;
    [[nodiscard]] RowNumber end() const;
    [[nodiscard]] RowNumber next(RowNumber row) const;
    [[nodiscard]] RowNumber prev(RowNumber row) const;
    [[nodiscard]] RowNumber move(RowNumber row, int64_t offset) const;
    [[nodiscard]] int64_t positionOf(RowNumber row) const;

private:
    DevectorWithMemoryTracking<SlidingBlock> blocks;
    int64_t first_block_number = 0;
    int64_t next_block_number = 0;
    int64_t end_position = 0;
};

}
