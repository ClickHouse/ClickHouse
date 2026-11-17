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
    void packReadyBlock();
    void startNextPartition();
    void releaseUnusedBlocks();

public:
    WindowTransform(
        SharedHeader input_header_, SharedHeader output_header_,
        const WindowDescription & window_description_, const VectorWithMemoryTracking<WindowFunctionDescription> & functions);

    ~WindowTransform() override;

    String getName() const override { return "WindowTransform"; }

    Status prepare() override;
    void work() override;

private:
    const WindowTransformParams params;

    /// Processors contract.
    std::optional<Chunk> pending_input;
    DequeWithMemoryTracking<Chunk> pending_outputs;
    bool input_is_finished = false;

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
};

}
