#pragma once

#include <WindowFunctions/IWindowFunction.h>

#include <Interpreters/WindowDescription.h>

#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Core/Block.h>

#include <optional>

namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Arena;

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
            SharedHeader input_header_,
            SharedHeader output_header_,
            const WindowDescription & window_description_,
            const std::vector<WindowFunctionDescription> &
                functions);

    ~WindowTransform() override;

    void initWorkspaces(const std::vector<WindowFunctionDescription> & functions);

    String getName() const override
    {
        return "WindowTransform";
    }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

    /* Implementation of IProcessor;
     */
    Status prepare() override;
    void work() override;
    void addInputBlock(Chunk chunk);
    void computeReadyRows();
    void startNextPartition();
    void releaseUnusedBlocks();

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
    void advanceFrameStartGroupsOffset();
    void advanceFrameEndGroupsOffset();

    // Returns the exclusive end of the peer group containing `start` -- the first row of the next
    // peer group, or `partition_end` if the group is the last one in the partition.
    //
    // `scan_frontier` makes the scan resumable when the group's end cannot be determined yet: it is the
    // last row already proven to be a peer of `start`, so a retry after more input arrives continues
    // from there instead of rescanning the group from its first row (which would make a peer group
    // spanning many blocks quadratic).
    RowNumber findPeerGroupEnd(const RowNumber & start, RowNumber & scan_frontier, bool & need_more_data) const;

    // Advances `pointer` forward, peer group by peer group, until it reaches the first row of the
    // `target_group`-th peer group (1-based) or the partition end.
    bool advanceGroupBoundary(RowNumber & pointer, Int64 & group_counter, RowNumber & scan_frontier, Int64 target_group) const;

    void updateAggregationState();
    void writeOutCurrentRow();

    const Columns & inputAt(const RowNumber & x) const
    {
        return blocks.blockAt(x.block).input_columns;
    }
    const SlidingBlock & blockAt(const RowNumber & x) const
    {
        return blocks.blockAt(x.block);
    }
    Int64 blockRowsNumber(const RowNumber & x) const
    {
        return blocks.blockAt(x.block).rows_count;
    }
    void advanceRowNumber(RowNumber & x) const
    {
        x = blocks.next(x);
    }
    RowNumber nextRowNumber(const RowNumber & x) const
    {
        return blocks.next(x);
    }
    RowNumber prevRowNumber(const RowNumber & x) const
    {
        return blocks.prev(x);
    }
    std::optional<RowNumber> moveRowNumber(const RowNumber & x, Int64 offset) const
    {
        return blocks.move(x, offset);
    }

    /// Data for window transform itself.
    const WindowTransformParams params;

    /// Runtime data.
    InputPort & input;
    OutputPort & output;
    std::optional<Chunk> pending_input;
    bool input_is_finished = false;

    // Per-window-function scratch spaces.
    std::vector<WindowFunctionWorkspace> workspaces;

    // One arena shared by the aggregate function states of the current partition.
    // Results never live in it: plain functions write values into the output
    // column, and -State results are merged into the ColumnAggregateFunction's
    // own arena. It is replaced when the partition changes, right after the
    // states are destroyed, so it does not grow across partitions.
    std::unique_ptr<Arena> arena;

    SlidingBlocks blocks;
    // The next block we are going to pass to the consumer.
    Int64 next_output_block_number = 0;

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
    Int64 current_row_number = 1;
    Int64 peer_group_start_row_number = 1;
    Int64 peer_group_number = 1;

    // Peer group index (1-based) of the row that frame_start / frame_end currently point to. Used
    // by GROUPS offset frames to count peer groups while advancing the boundaries. Reset together
    // with the frame boundaries when a new partition starts.
    Int64 frame_start_group_number = 1;
    Int64 frame_end_group_number = 1;

    // Resume positions for the peer-group scans of the corresponding boundaries (see
    // `findPeerGroupEnd`). Unlike the RANGE offset frames, which resume by advancing the boundary
    // itself, the scan progress must be kept separately: a GROUPS boundary always points at the first
    // row of a peer group.
    RowNumber frame_start_group_scan_frontier;
    RowNumber frame_end_group_scan_frontier;

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
};

}
