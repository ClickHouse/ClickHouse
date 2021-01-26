#pragma once
#include <Processors/ISimpleTransform.h>

#include <Interpreters/AggregateDescription.h>

#include <Common/AlignedBuffer.h>

#include <deque>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Arena;

// Runtime data for computing one window function
struct WindowFunctionWorkspace
{
    WindowFunctionDescription window_function;
    AlignedBuffer aggregate_function_state;
    std::vector<size_t> argument_column_indices;

    /*
    // Argument and result columns. Be careful, they are per-chunk.
    std::vector<const IColumn *> argument_columns;
    MutableColumnPtr result_column;
    */
};

struct WindowTransformBlock
{
    Columns input_columns;
    MutableColumns output_columns;

    // Even in case of `count() over ()` we should have a dummy input column.
    // Not sure how reliable this is...
    size_t numRows() const { return input_columns[0]->size(); }
};

struct RowNumber
{
    uint64_t block = 0;
    uint64_t row = 0;

    bool operator < (const RowNumber & other) const
    {
        return block < other.block
            || (block == other.block && row < other.row);
    }

    bool operator == (const RowNumber & other) const
    {
        return block == other.block && row == other.row;
    }
};

/*
 * Computes several window functions that share the same window. The input must
 * be sorted correctly for this window (PARTITION BY, then ORDER BY).
 * We need to track the following pointers:
 * 1) start of partition -- rows that compare equal w/PARTITION BY.
 * 2) current frame boundaries.
 * 3) start of peer group -- rows that compare equal w/ORDER BY (empty ORDER BY
 *    means all rows are equal).
 * These row ranges are (almost) nested -- peer group is inside frame inside
 * partition. The only exception is when the exclusion clause is specified that
 * excludes current peer group, but we don't support it anyway.
 * All pointers only move forward.
 * The value of the function is the same for all rows of the peer group.
 * (partition [frame {group} ] )
 */
class WindowTransform : public IProcessor /* public ISimpleTransform */
{
public:
    WindowTransform(
            const Block & input_header_,
            const Block & output_header_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> &
                functions);

    ~WindowTransform() override;

    String getName() const override
    {
        return "WindowTransform";
    }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

    /*
     * (former) Implementation of ISimpleTransform.
     */
    void appendChunk(Chunk & chunk) /*override*/;

    /*
     * Implementation of IProcessor;
     */
    Status prepare() override;
    void work() override;

private:
    void advancePartitionEnd();
    void advanceGroupEnd();
    void advanceGroupEndGroups();
    void advanceGroupEndRows();
    void advanceGroupEndRange();
    void advanceFrameStart();
    void advanceFrameEnd();
    void writeOutGroup();
    void initPerBlockCaches();

    Columns & inputAt(const RowNumber & x)
    {
        assert(x.block >= first_block_number);
        assert(x.block - first_block_number < blocks.size());
        return blocks[x.block - first_block_number].input_columns;
    }

    const Columns & inputAt(const RowNumber & x) const
    { return const_cast<WindowTransform *>(this)->inputAt(x); }

    size_t blockRowsNumber(const RowNumber & x) const
    {
        return inputAt(x)[0]->size();
    }

    MutableColumns & outputAt(const RowNumber & x)
    {
        assert(x.block >= first_block_number);
        assert(x.block - first_block_number < blocks.size());
        return blocks[x.block - first_block_number].output_columns;
    }

    void advanceRowNumber(RowNumber & x) const
    {
        assert(x.block >= first_block_number);
        assert(x.block - first_block_number < blocks.size());

        const auto block_rows = inputAt(x)[0]->size();
        assert(x.row < block_rows);

        x.row++;
        if (x.row < block_rows)
        {
            return;
        }

        x.row = 0;
        ++x.block;
    }

    void retreatRowNumber(RowNumber & x) const
    {
        if (x.row > 0)
        {
            --x.row;
            return;
        }

        --x.block;
        assert(x.block >= first_block_number);
        assert(x.block < first_block_number + blocks.size());
        assert(inputAt(x)[0]->size() > 0);
        x.row = inputAt(x)[0]->size() - 1;

#ifndef NDEBUG
        auto xx = x;
        advanceRowNumber(xx);
        assert(xx == x);
#endif
    }

    RowNumber blocksEnd() const
    { return RowNumber{first_block_number + blocks.size(), 0}; }

public:
    /*
     * Data (formerly) inherited from ISimpleTransform.
     */
    InputPort & input;
    OutputPort & output;

    bool has_input = false;
    bool input_is_finished = false;
    Port::Data input_data;
    bool has_output = false;
    Port::Data output_data;

    /*
     * Data for window transform itself.
     */
    Block input_header;

    WindowDescription window_description;

    // Indices of the PARTITION BY columns in block.
    std::vector<size_t> partition_by_indices;
    // Indices of the ORDER BY columns in block;
    std::vector<size_t> order_by_indices;

    // Per-window-function scratch spaces.
    std::vector<WindowFunctionWorkspace> workspaces;

    // FIXME Reset it when the partition changes. We only save the temporary
    // states in it (probably?).
    std::unique_ptr<Arena> arena;

    // A sliding window of blocks we currently need. We add the input blocks as
    // they arrive, and discard the blocks we don't need anymore. The blocks
    // have an always-incrementing index. The index of the first block is in
    // `first_block_number`.
    std::deque<WindowTransformBlock> blocks;
    uint64_t first_block_number = 0;
    // The next block we are going to pass to the consumer.
    uint64_t next_output_block_number = 0;
    // The first row for which we still haven't calculated the window functions.
    // Used to determine which resulting blocks we can pass to the consumer.
    RowNumber first_not_ready_row;

    // We don't keep the pointer to start of partition, because we don't really
    // need it, and we want to be able to drop the starting blocks to save memory.
    // The `partition_end` is past-the-end, as usual. When partition_ended = false,
    // it still haven't ended, and partition_end is the next row to check.
    RowNumber partition_end;
    bool partition_ended = false;

    // Current peer group is [group_start, group_end) if group_ended,
    // [group_start, ?) otherwise.
    RowNumber group_start;
    RowNumber group_end;
    bool group_ended = false;

    // After we have found the final boundaries of the frame, we can immediately
    // output the result for the current group, w/o waiting for more data.
    RowNumber frame_start;
    RowNumber frame_end;
    bool frame_ended = false;
};

}

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::RowNumber>
{
    constexpr auto parse(format_parse_context & ctx)
    {
        auto it = ctx.begin();
        auto end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw format_error("invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::RowNumber & x, FormatContext & ctx)
    {
        return format_to(ctx.out(), "{}:{}", x.block, x.row);
    }
};
