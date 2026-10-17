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
 *                |<------------------ stored ------------------>|<----------- not read yet ----------- .
 *   partition         |<---------------------------------------- - - - - - - - - - - - - - ->|
 *   frame                        |<------------>|
 *   peers                              |<--------->|
 *   current row                             ^
 *
 * - partition: rows range with the current row's PARTITION BY key.
 * - frame: rows range available to the aggregation window function while it is being evaluated.
 * - peers: rows range with the current row's ORDER BY key.
 * - current row: the row being computed right now.
 */
class WindowTransform final : public IProcessor
{
public:
    WindowTransform(
        SharedHeader input_header_, SharedHeader output_header_,
        const WindowDescription & window_description_, const std::vector<WindowFunctionDescription> & functions);

    ~WindowTransform() override;

    String getName() const override { return "WindowTransform"; }

    static Block transformHeader(Block header, const ExpressionActionsPtr & expression);

    Status prepare() override;
    void work() override;
    void addInputBlock(Chunk chunk);
    void computeReadyRows();
    void startNextPartition();
    void releaseUnusedBlocks();

    void updateAggregationState();
    void writeOutCurrentRow();

    /// Everything fixed at construction.
    const WindowTransformParams params;

    std::optional<Chunk> pending_input;
    bool input_is_finished = false;

    /// One per window function.
    std::vector<WindowFunctionWorkspace> workspaces;

    /// Shared by the aggregate states of the current partition and replaced with it, so it does not grow across
    /// partitions. No result lives in it: a -State result is merged into the arena of the output column.
    std::unique_ptr<Arena> arena;

    SlidingBlocks blocks;
    SlidingIndexes indexes;
    /// The first stored block not yet passed to the consumer.
    Int64 next_output_block_number = 0;

    Partition partition;
    Frame frame;
    Peers peers;

    /// The frame the aggregate states hold; the next frame is compared with it to add only the new rows.
    FrameBounds prev_frame;
};

}
