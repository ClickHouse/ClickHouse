#pragma once

#include <Interpreters/WindowDescription.h>
#include <WindowFunctions/Common/WindowBounds.h>
#include <WindowFunctions/Common/WindowFunctionRequirements.h>
#include <WindowFunctions/IAggregateWindowFunction.h>
#include <WindowFunctions/IGeneralWindowFunction.h>

#include <Processors/Transforms/Window/Frame.h>
#include <Processors/Transforms/Window/Partition.h>
#include <Processors/Transforms/Window/Peers.h>
#include <Processors/Transforms/Window/SlidingArguments.h>
#include <Processors/Transforms/Window/SlidingBlocks.h>
#include <Processors/Transforms/Window/SlidingIndexes.h>
#include <Processors/Transforms/Window/WindowTransformParams.h>
#include <Processors/IProcessor.h>
#include <Processors/Port.h>

#include <Common/AlignedBuffer.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/DequeWithMemoryTracking.h>
#include <Core/Block.h>

#include <optional>

namespace DB
{

struct GeneralWindowFunctionSlot
{
    GeneralWindowFunctionPtr function;
    AlignedBuffer state;
    DataTypePtr result_type;
    size_t function_index = 0;
};

struct AggregateWindowFunctionSlot
{
    AggregateWindowFunctionPtr function;
    DataTypePtr result_type;
    size_t function_index = 0;
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
    void recordCurrentRowBounds();
    void evaluateRowsBatch();
    void startNextPartition();
    void releaseUnusedBlocks();

public:
    WindowTransform(
        SharedHeader input_header_, SharedHeader output_header_,
        const WindowDescription & window_description_, const VectorWithMemoryTracking<WindowFunctionDescription> & functions);

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
    const WindowTransformParams params;

    /// Processors contract.
    std::optional<Chunk> pending_input;
    DequeWithMemoryTracking<Chunk> pending_outputs;
    bool input_is_finished = false;

    /// For the standalone evaluation of query result previews (see `QueryResultPreview.h`):
    /// a copy of the constructor argument to build the fresh one-shot transform, the diverted
    /// input chunk, and the computed preview waiting for the output port.
    VectorWithMemoryTracking<WindowFunctionDescription> window_functions_for_preview;
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

    /// Row walk.
    SlidingBlocks blocks;
    SlidingIndexes indexes;
    SlidingArguments arguments;
    Partition partition;
    Peers peers;
    Frame frame;

    /// Functions.
    VectorWithMemoryTracking<GeneralWindowFunctionSlot> general_functions;
    VectorWithMemoryTracking<AggregateWindowFunctionSlot> aggregate_functions;
    MutableColumns current_results;
    Int64 first_row_read_by_functions = 0;

    /// Pending batch for evaluation.
    WindowBounds bounds;
    RowNumber batch_start;
    /// The number of rows the aggregate functions add to their states to evaluate the pending batch. The batch
    /// is evaluated early when it exceeds `max_batch_frame_rows`, so that the row walk checks cancellation
    /// between the batches (see `computeReadyRows`). The frame of the previous row in the partition is needed
    /// to compute it.
    Int64 batch_frame_rows = 0;
    Int64 previous_frame_start = 0;
    Int64 previous_frame_end = 0;
    static constexpr Int64 max_batch_frame_rows = 1 << 20;
};

}
