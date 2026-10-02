#pragma once

#include <WindowFunctions/IWindowFunction.h>

#include <Interpreters/WindowDescription.h>

#include <Processors/Transforms/Window/Frame.h>
#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/Peers.h>
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

/* Computes several window functions that share one window. The input is sorted by PARTITION BY, then by
 * ORDER BY, and arrives block by block; the transform keeps the blocks it still needs and walks them row by row.
 *
 *   blocks       [ block 7  ][ block 8  ][ block 9  ][ block 10 ] .. block 11 .. block 12 .. block 13 ..
 *                |<------------- stored ----------------------->|<-------- not read yet ------------- ..
 *   partition         |<---------------------------------------- - - - - - - - - - - - - - ->|
 *   frame                        |<------------>|
 *   peers                          |<------->|
 *   current row                           ^
 *
 * - partition: rows range with the current row's PARTITION BY key.
 * - frame: rows range used to calcualte window functions.
 * - peers: rows range with the current row's ORDER BY key.
 * - current row: the row being computed right now.
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

    // Window runtime information: the current row with its peer group, the partition and the frame.
    Partition partition;
    Frame frame;
    Peers peers;

    // The previous frame that corresponds to the current state of the
    // aggregate function. We use it to determine how to update the aggregation
    // state after we find the new frame.
    FrameBounds prev_frame;
};

}
