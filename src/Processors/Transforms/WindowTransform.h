#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/Common/WindowArguments.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/IGeneralWindowFunction.h>

#include <Processors/Transforms/Window/Frame.h>
#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/Peers.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/SlidingIndexes.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Common/AlignedBuffer.h>
#include <Common/Arena.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/Block.h>

#include <optional>

namespace DB
{

struct WindowFunctionWorkspace
{
    AggregateFunctionPtr aggregate_function;
    bool is_aggregate_function_state = false;
    size_t result_column = 0;
    VectorWithMemoryTracking<size_t> argument_column_indices;
    mutable AlignedBuffer aggregate_function_state;
    VectorWithMemoryTracking<const IColumn *> argument_columns;
    Int64 cached_block_number = -1;
};

struct GeneralWindowFunctionSlot
{
    GeneralWindowFunctionPtr function;
    char * state = nullptr;
    size_t result_column = 0;
    WindowArguments arguments;
};

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
    void addInputBlock(Chunk chunk);
    void computeReadyRows();
    void appendCurrentRow();
    void flushBatch();
    void updateAggregationState();
    void writeOutCurrentRow();
    void startNextPartition();
    void releaseUnusedBlocks();

public:
    WindowTransform(
        SharedHeader input_header_, SharedHeader output_header_,
        const WindowDescription & window_description_, const std::vector<WindowFunctionDescription> & functions);

    ~WindowTransform() override;

    String getName() const override { return "WindowTransform"; }

    /// Preview chunks are computed standalone by a fresh one-shot transform, out of band
    /// (see `QueryResultPreview.h`): the window functions apply to the preview alone, so e.g.
    /// `max(x) OVER ()` is the maximum of the intermediate result and converges to the final value.
    /// A preview on which a window function throws is dropped.
    bool supportsQueryResultPreviews() const override { return true; }

    Status prepare() override;
    void work() override;

private:
    /// Everything fixed at construction.
    const WindowTransformParams params;

    std::optional<Chunk> pending_input;
    bool input_is_finished = false;

    /// For the standalone evaluation of query result previews (see `QueryResultPreview.h`):
    /// a copy of the constructor argument to build the fresh one-shot transform, the diverted
    /// input chunk, and the computed preview waiting for the output port.
    std::vector<WindowFunctionDescription> window_functions_for_preview;
    Chunk query_result_preview_input;
    Chunk pending_query_result_preview;

    /// Set on the one-shot transform computing a query result preview: the cancellation flag of the
    /// transform in the pipeline, which the one-shot one is not a part of, so the executor never cancels it.
    const std::atomic<bool> * outer_cancellation_flag = nullptr;

    void computeQueryResultPreview();
    bool isCancelledOrOuterCancelled() const
    {
        return isCancelled() || (outer_cancellation_flag && outer_cancellation_flag->load(std::memory_order_acquire));
    }

    /// The general functions with their states, and the aggregates; each names its output column.
    std::vector<GeneralWindowFunctionSlot> general_functions;
    std::vector<WindowFunctionWorkspace> workspaces;
    Arena states_arena;

    /// The batch being collected, starting at `batch_start`.
    WindowBounds bounds;
    RowNumber batch_start;

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
