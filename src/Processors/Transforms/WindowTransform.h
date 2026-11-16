#pragma once

#include <WindowFunctions/IWindowFunction.h>

#include <Interpreters/WindowDescription.h>

#include <Processors/Transforms/Window/Frame.h>
#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/SlidingIndexes.h>
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
    bool arePeers(const RowNumber & x, const RowNumber & y) const;

    void checkInvariants() const;

    void updateAggregationState();
    void writeOutCurrentRow();

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
    SlidingIndexes indexes;
    // The next block we are going to pass to the consumer.
    Int64 next_output_block_number = 0;

    // The current partition. Its start doesn't point to a valid block, because
    // we want to drop the blocks early to save memory. We still have to track it
    // so that we can cut off a PRECEDING frame at the partition start.
    Partition partition;

    // The row for which we are now computing the window functions.
    RowPoint current;
    // The start of current peer group.
    RowPoint peer_group_start;

    // The frame of the current row and its search. When we move to the next
    // row, both bounds may jump forward by an unknown number of blocks, e.g.
    // under a RANGE frame, so sometimes neither of them is known. We update the
    // states of the window functions once the frame is fully visible, and can
    // then immediately output the result for the current row, without waiting
    // for more data.
    Frame frame;

    // The previous frame that corresponds to the current state of the
    // aggregate function. We use it to determine how to update the aggregation
    // state after we find the new frame.
    FrameBounds prev_frame;
};

}
