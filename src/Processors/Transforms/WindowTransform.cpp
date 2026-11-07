#include <Processors/Transforms/WindowTransform.h>
#include <Common/VectorWithMemoryTracking.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <algorithm>
#include <iterator>
#include <limits>
#include <ranges>
#include <utility>

namespace DB
{

namespace
{

Columns materializeColumns(Columns columns, const VectorWithMemoryTracking<bool> & should_materialize)
{
    for (auto && [column, materialize] : std::views::zip(columns, should_materialize))
        if (materialize)
            column = recursiveRemoveLowCardinality(column->convertToFullIfWrapped());

    return columns;
}

GeneralWindowFunctionSlot createGeneralSlot(GeneralWindowFunctionPtr general, size_t function_index)
{
    GeneralWindowFunctionSlot slot{
        .function = general,
        .state = {},
        .result_type = general->getResultType(),
        .function_index = function_index,
    };

    if (slot.function->sizeOfState() != 0)
    {
        slot.state.reset(slot.function->sizeOfState(), slot.function->alignOfState());
        slot.function->createState(slot.state.data());
    }

    return slot;
}

AggregateWindowFunctionSlot createAggregateSlot(AggregateFunctionPtr function, const WindowDescription & window, size_t function_index)
{
    return AggregateWindowFunctionSlot{
        .function = createAggregateWindowFunction(function, window),
        .result_type = function->getResultType(),
        .function_index = function_index,
    };
}

void destroyStates(VectorWithMemoryTracking<GeneralWindowFunctionSlot> & general_functions)
{
    for (auto & slot : general_functions)
        if (slot.state.data())
            slot.function->destroyState(slot.state.data());
}

void resetPartitions(VectorWithMemoryTracking<GeneralWindowFunctionSlot> & general_functions, VectorWithMemoryTracking<AggregateWindowFunctionSlot> & aggregate_functions)
{
    for (auto & slot : general_functions)
        slot.function->resetPartition(slot.state.data());

    for (auto & slot : aggregate_functions)
        slot.function->resetPartition();
}

MutableColumns initializeResultColumns(const VectorWithMemoryTracking<GeneralWindowFunctionSlot> & general_functions, const VectorWithMemoryTracking<AggregateWindowFunctionSlot> & aggregate_functions, int64_t rows_count)
{
    MutableColumns columns(general_functions.size() + aggregate_functions.size());

    for (const auto & slot : general_functions)
        columns[slot.function_index] = slot.result_type->createColumn();

    for (const auto & slot : aggregate_functions)
        columns[slot.function_index] = slot.result_type->createColumn();

    for (auto & column : columns)
        column->reserve(rows_count);

    return columns;
}

Int64 evaluateFunctions(
    VectorWithMemoryTracking<GeneralWindowFunctionSlot> & general_functions,
    VectorWithMemoryTracking<AggregateWindowFunctionSlot> & aggregate_functions,
    const WindowBounds & bounds, SlidingArguments & arguments, MutableColumns & results)
{
    Int64 first_row = std::numeric_limits<Int64>::max();

    for (auto & slot : general_functions)
        first_row = std::min(first_row, slot.function->evaluate(slot.state.data(), bounds, arguments.view(slot.function_index), *results[slot.function_index]));

    for (auto & slot : aggregate_functions)
        first_row = std::min(first_row, slot.function->evaluate(bounds, arguments.view(slot.function_index), *results[slot.function_index]));

    return first_row;
}

Chunk packBlock(const SlidingBlock & block, MutableColumns results)
{
    Columns columns = block.input_columns;
    columns.insert(columns.end(), std::make_move_iterator(results.begin()), std::make_move_iterator(results.end()));

    Chunk chunk;
    chunk.setColumns(std::move(columns), block.rows_count);
    return chunk;
}

bool isRowReady(const WindowFunctionRequirements & required, const Frame & frame, const Peers & peers, const Partition & partition)
{
    return (!required.frame || frame.bounds().fully_visible)
        && (!required.peers || peers.bounds().fully_visible)
        && (!required.partition || partition.bounds().fully_visible);
}

}

WindowTransform::WindowTransform(
    SharedHeader input_header_, SharedHeader output_header_,
    const WindowDescription & window_description_, const VectorWithMemoryTracking<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , params(WindowTransformParams::create(*input_header_, window_description_, functions))
    , indexes(params)
    , arguments(params.input_header, functions)
    , frame(params)
{
    try
    {
        general_functions.reserve(functions.size());
        aggregate_functions.reserve(functions.size());
        for (size_t function_index = 0; function_index < functions.size(); ++function_index)
        {
            const WindowFunctionDescription & function = functions[function_index];
            if (auto general = asGeneralWindowFunction(function.aggregate_function))
            {
                required = unionOf(required, general->getRequirements(params.window_description));
                general_functions.push_back(createGeneralSlot(std::move(general), function_index));
            }
            else
            {
                required = unionOf(required, WindowFunctionRequirements{.frame = true});
                aggregate_functions.push_back(createAggregateSlot(function.aggregate_function, params.window_description, function_index));
            }
        }
    }
    catch (...)
    {
        destroyStates(general_functions);
        throw;
    }
}

WindowTransform::~WindowTransform()
{
    destroyStates(general_functions);
}

void WindowTransform::addInputBlock(Chunk chunk)
{
    auto rows_count = static_cast<int64_t>(chunk.getNumRows());
    auto materialized_columns = materializeColumns(chunk.getColumns(), params.should_materialize);
    auto index = indexes.calculate(materialized_columns, rows_count);
    arguments.add(materialized_columns, rows_count);
    blocks.add(std::move(chunk), std::move(materialized_columns), std::move(index));
    partition.advance(blocks);
}

void WindowTransform::computeReadyRows()
{
    /// Cancellation is checked per row here; window functions evaluate each batch without cancellation checks
    while (!isCancelled())
    {
        if (bounds.rows != 0 && peers.current().location.block != batch_start.block)
            evaluateRowsBatch();

        if (peers.current().location == partition.bounds().end.location)
        {
            /// The rest of this partition is not read yet, or the last partition is done.
            if (!partition.bounds().fully_visible || input_is_finished)
                break;

            evaluateRowsBatch();
            startNextPartition();
        }

        if (required.peers || required.frame)
            peers.advancePeerGroupBounds(blocks, partition.bounds());
        if (required.frame)
            frame.advance(blocks, peers.current(), partition.bounds());
        if (!isRowReady(required, frame, peers, partition))
            break;

        recordCurrentRowBounds();
        peers.advanceCurrentRow(blocks, partition.bounds());
    }

    evaluateRowsBatch();
}

void WindowTransform::recordCurrentRowBounds()
{
    const RowPoint & current = peers.current();
    if (bounds.rows == 0)
        batch_start = current.location;

    bounds.row.push_back(current.row_index_in_partition);
    if (required.frame)
    {
        bounds.frame_start.push_back(frame.bounds().start.row_index_in_partition);
        bounds.frame_end.push_back(frame.bounds().end.row_index_in_partition);
    }
    if (required.peers)
    {
        bounds.peers_start.push_back(peers.bounds().start.row_index_in_partition);
        bounds.peers_end.push_back(peers.bounds().end.row_index_in_partition);
        bounds.peer_group_index.push_back(current.peer_group_index_in_partition);
    }
    if (required.partition)
    {
        bounds.partition_end.push_back(partition.bounds().end.row_index_in_partition);
    }
    ++bounds.rows;
}

void WindowTransform::evaluateRowsBatch()
{
    if (bounds.rows == 0)
        return;

    const SlidingBlock & block = blocks.blockAt(batch_start.block);
    if (current_results.empty())
        current_results = initializeResultColumns(general_functions, aggregate_functions, block.rows_count);

    first_row_read_by_functions = evaluateFunctions(general_functions, aggregate_functions, bounds, arguments, current_results);

    const bool block_complete = batch_start.row + bounds.rows == block.rows_count;
    if (block_complete)
        pending_outputs.push_back(packBlock(block, std::exchange(current_results, {})));

    clearBounds(bounds);
}

void WindowTransform::startNextPartition()
{
    const RowNumber partition_start = partition.bounds().end.location;
    chassert(peers.current().location == partition_start);

    arguments.startPartition(partition.bounds().end.row_index_in_partition);
    partition.beginAt(blocks, partition_start);
    peers.enterPartition(partition_start);
    frame.enterPartition(partition_start);
    first_row_read_by_functions = 0;

    resetPartitions(general_functions, aggregate_functions);
}

IProcessor::Status WindowTransform::prepare()
{
    InputPort & input = inputs.front();
    OutputPort & output = outputs.front();

    /// The consumer closed the port early, or the query was cancelled: nothing more to produce.
    if (output.isFinished() || isCancelled())
    {
        input.close();
        return Status::Finished;
    }

    if (!pending_outputs.empty())
    {
        if (output.canPush())
        {
            output.push(std::move(pending_outputs.front()));
            pending_outputs.pop_front();
        }
        return Status::PortFull;
    }

    /// The input ended at the previous cycle and no output is queued, so everything is out.
    if (input_is_finished)
    {
        output.finish();
        return Status::Finished;
    }

    if (pending_input)
        return Status::Ready;

    /// No more input: one more work closes the last partition.
    if (input.isFinished())
    {
        input_is_finished = true;
        return Status::Ready;
    }

    if (input.hasData())
    {
        pending_input = input.pull(/*set_not_needed=*/true);
        return Status::Ready;
    }

    input.setNeeded();
    return Status::NeedData;
}

void WindowTransform::work()
{
    chassert(pending_input || input_is_finished);

    if (pending_input)
    {
        Chunk chunk = std::exchange(pending_input, std::nullopt).value();
        if (!chunk.hasRows())
            return;

        addInputBlock(std::move(chunk));
    }
    else
    {
        partition.finish();
    }

    computeReadyRows();
    releaseUnusedBlocks();
}

void WindowTransform::releaseUnusedBlocks()
{
    const RowPoint & current = peers.current();
    const Int64 first_row_read_by_frame = required.frame ? frame.firstRowToRead() : std::numeric_limits<Int64>::max();
    const Int64 first_row_to_keep = std::min({current.row_index_in_partition, first_row_read_by_frame, first_row_read_by_functions});
    const Int64 first_position_to_keep = blocks.positionOf(current.location) - (current.row_index_in_partition - first_row_to_keep);

    while (blocks.begin() != blocks.end() && blocks.front().end_position <= first_position_to_keep)
    {
        blocks.pop();
        arguments.pop();
    }
}

}
