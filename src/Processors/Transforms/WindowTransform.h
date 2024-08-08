#pragma once

#include <Interpreters/WindowDescription.h>
#include <AggregateFunctions/WindowFunction.h>

#include <Processors/IProcessor.h>

#include <Common/AlignedBuffer.h>

#include <deque>

/// See https://stackoverflow.com/questions/72533435/error-zero-as-null-pointer-constant-while-comparing-template-class-using-spaces
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wzero-as-null-pointer-constant"


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Arena;


struct WindowTransformBlock
{
    Columns original_input_columns;
    Columns input_columns;
    Columns casted_columns;
    MutableColumns output_columns;

    size_t rows = 0;
};

struct RowNumber
{
    UInt64 block = 0;
    UInt64 row = 0;

    auto operator <=>(const RowNumber &) const = default;
};


/* Computes several window functions that share the same window. The input must
 * be sorted by PARTITION BY (in any order), then by ORDER BY.
 * We need to track the following pointers:
 * 1) boundaries of partition -- rows that compare equal w/PARTITION BY.
 * 2) current row for which we will compute the window functions.
 * 3) boundaries of the frame for this row.
 * Both the peer group and the frame are inside the partition, but can have any
 * position relative to each other.
 * All pointers only move forward. For partition boundaries, this is ensured by
 * the order of input data. This property also trivially holds for the ROWS and
 * GROUPS frames. For the RANGE frame, the proof requires the additional fact
 * that the ranges are specified in terms of (the single) ORDER BY column.
 *
 * `final` is so that the isCancelled() is devirtualized, we call it every row.
 */
class WindowTransform final : public IProcessor
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

    /* (former) Implementation of ISimpleTransform.
     */
    void appendChunk(Chunk & chunk) /*override*/;

    /* Implementation of IProcessor;
     */
    Status prepare() override;
    void work() override;

    /* Implementation details.
     */
    void advancePartitionEnd();

    bool arePeers(const RowNumber & x, const RowNumber & y) const;

    void advanceFrameStartRowsOffset();
    void advanceFrameStartRangeOffset();
    void advanceFrameStart();

    void advanceFrameEndRowsOffset();
    void advanceFrameEndCurrentRow();
    void advanceFrameEndUnbounded();
    void advanceFrameEnd();
    void advanceFrameEndRangeOffset();

    void updateAggregationState();
    void writeOutCurrentRow();

    Columns & inputAt(const RowNumber & x)
    {
        assert(x.block >= first_block_number);
        assert(x.block - first_block_number < blocks.size());
        return blocks[x.block - first_block_number].input_columns;
    }

    const Columns & inputAt(const RowNumber & x) const
    {
        return const_cast<WindowTransform *>(this)->inputAt(x);
    }

    auto & blockAt(const UInt64 block_number)
    {
        assert(block_number >= first_block_number);
        assert(block_number - first_block_number < blocks.size());
        return blocks[block_number - first_block_number];
    }

    const auto & blockAt(const UInt64 block_number) const
    {
        return const_cast<WindowTransform *>(this)->blockAt(block_number);
    }

    auto & blockAt(const RowNumber & x)
    {
        return blockAt(x.block);
    }

    const auto & blockAt(const RowNumber & x) const
    {
        return const_cast<WindowTransform *>(this)->blockAt(x);
    }

    size_t blockRowsNumber(const RowNumber & x) const
    {
        return blockAt(x).rows;
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

        const auto block_rows = blockAt(x).rows;
        assert(x.row < block_rows);

        ++x.row;
        if (x.row < block_rows)
        {
            return;
        }

        x.row = 0;
        ++x.block;
    }

    RowNumber nextRowNumber(const RowNumber & x) const
    {
        RowNumber result = x;
        advanceRowNumber(result);
        return result;
    }

    void retreatRowNumber(RowNumber & x) const
    {
#ifndef NDEBUG
        auto original_x = x;
#endif

        if (x.row > 0)
        {
            --x.row;
            return;
        }

        --x.block;
        assert(x.block >= first_block_number);
        assert(x.block < first_block_number + blocks.size());
        assert(blockAt(x).rows > 0);
        x.row = blockAt(x).rows - 1;

#ifndef NDEBUG
        auto advanced_retreated_x = x;
        advanceRowNumber(advanced_retreated_x);
        assert(advanced_retreated_x == original_x);
#endif
    }

    RowNumber prevRowNumber(const RowNumber & x) const
    {
        RowNumber result = x;
        retreatRowNumber(result);
        return result;
    }

    auto moveRowNumber(const RowNumber & original_row_number, Int64 offset) const;
    auto moveRowNumberNoCheck(const RowNumber & original_row_number, Int64 offset) const;

    void assertValid(const RowNumber & x) const
    {
        assert(x.block >= first_block_number);
        if (x.block == first_block_number + blocks.size())
            assert(x.row == 0);
        else
            assert(x.row < blockRowsNumber(x));
    }

    RowNumber blocksEnd() const
    {
        return RowNumber{first_block_number + blocks.size(), 0};
    }

    RowNumber blocksBegin() const
    {
        return RowNumber{first_block_number, 0};
    }

    /* Data (formerly) inherited from ISimpleTransform, needed for the
     * implementation of the IProcessor interface.
     */
    InputPort & input;
    OutputPort & output;

    bool has_input = false;
    bool input_is_finished = false;
    Port::Data input_data;
    bool has_output = false;
    Port::Data output_data;

    /* Data for window transform itself.
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
    UInt64 first_block_number = 0;
    // The next block we are going to pass to the consumer.
    UInt64 next_output_block_number = 0;
    // The first row for which we still haven't calculated the window functions.
    // Used to determine which resulting blocks we can pass to the consumer.
    RowNumber first_not_ready_row;

    // Boundaries of the current partition.
    // partition_start doesn't point to a valid block, because we want to drop
    // the blocks early to save memory. We still have to track it so that we can
    // cut off a PRECEDING frame at the partition start.
    // The `partition_end` is past-the-end, as usual. When
    // partition_ended = false, it still haven't ended, and partition_end is the
    // next row to check.
    RowNumber partition_start;
    RowNumber partition_end;
    bool partition_ended = false;

    // The row for which we are now computing the window functions.
    RowNumber current_row;
    // The start of current peer group, needed for CURRENT ROW frame start.
    // For ROWS frame, always equal to the current row, and for RANGE and GROUP
    // frames may be earlier.
    RowNumber peer_group_start;

    // Row and group numbers in partition for calculating rank() and friends.
    UInt64 current_row_number = 1;
    UInt64 peer_group_start_row_number = 1;
    UInt64 peer_group_number = 1;

    // The frame is [frame_start, frame_end) if frame_ended && frame_started,
    // and unknown otherwise. Note that when we move to the next row, both the
    // frame_start and the frame_end may jump forward by an unknown amount of
    // blocks, e.g. if we use a RANGE frame. This means that sometimes we don't
    // know neither frame_end nor frame_start.
    // We update the states of the window functions after we find the final frame
    // boundaries.
    // After we have found the final boundaries of the frame, we can immediately
    // output the result for the current row, without waiting for more data.
    RowNumber frame_start;
    RowNumber frame_end;
    bool frame_ended = false;
    bool frame_started = false;

    // The previous frame boundaries that correspond to the current state of the
    // aggregate function. We use them to determine how to update the aggregation
    // state after we find the new frame.
    RowNumber prev_frame_start;
    RowNumber prev_frame_end;

    // Comparison function for RANGE OFFSET frames. We choose the appropriate
    // overload once, based on the type of the ORDER BY column. Choosing it for
    // each row would be slow.
    std::function<int(
        const IColumn * compared_column, size_t compared_row,
        const IColumn * reference_column, size_t reference_row,
        const Field & offset,
        bool offset_is_preceding)> compare_values_with_offset;
};

}

#pragma clang diagnostic pop
