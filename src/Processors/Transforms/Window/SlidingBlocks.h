#pragma once

#include <Columns/IColumn_fwd.h>
#include <Processors/Chunk.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>

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

struct SlidingBlock
{
    Columns original_input_columns;
    Columns input_columns;
    MutableColumns output_columns;

    const int64_t rows_count = 0;
    const int64_t block_number = 0;
};

class SlidingBlocks
{
public:
    SlidingBlock & add(Chunk chunk, const WindowTransformParams & params);
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
