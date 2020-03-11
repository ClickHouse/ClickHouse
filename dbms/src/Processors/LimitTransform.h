#pragma once

#include <Processors/IProcessor.h>
#include <Core/SortDescription.h>

namespace DB
{

class LimitTransform : public IProcessor
{
public:
    /// Common counter of read rows. It is shared between several streams.
    /// Is used for pre-limit. Needed to skip main limit phase and avoid the resizing pipeline to single stream.
    struct LimitState
    {
        std::atomic<size_t> total_read_rows = 0;
    };

    using LimitStatePtr = std::shared_ptr<LimitState>;

private:
    InputPort & input;
    OutputPort & output;

    size_t limit;
    size_t offset;
    LimitStatePtr limit_state;
    size_t rows_read = 0; /// including the last read block
    bool always_read_till_end;

    bool has_block = false;
    bool block_processed = false;
    Chunk current_chunk;

    UInt64 rows_before_limit_at_least = 0;

    bool with_ties;
    const SortDescription description;

    Chunk previous_row_chunk;  /// for WITH TIES, contains only sort columns

    std::vector<size_t> sort_column_positions;
    Chunk makeChunkWithPreviousRow(const Chunk & current_chunk, size_t row_num) const;
    ColumnRawPtrs extractSortColumns(const Columns & columns) const;
    bool sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, size_t current_chunk_row_num) const;

public:
    LimitTransform(
        const Block & header_, size_t limit_, size_t offset_, LimitStatePtr limit_state_ = nullptr,
        bool always_read_till_end_ = false, bool with_ties_ = false,
        SortDescription description_ = {});

    String getName() const override { return "Limit"; }

    Status prepare() override;
    void work() override;

    InputPort & getInputPort() { return input; }
    OutputPort & getOutputPort() { return output; }

    UInt64 getRowsBeforeLimitAtLeast() const { return rows_before_limit_at_least; }
};

}
