#include <Processors/Transforms/WindowTransform.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <algorithm>
#include <iterator>
#include <ranges>
#include <utility>

namespace DB
{

namespace
{

Columns materializeColumns(Columns columns, const std::vector<bool> & should_materialize)
{
    for (auto && [column, materialize] : std::views::zip(columns, should_materialize))
        if (materialize)
            column = recursiveRemoveLowCardinality(column->convertToFullIfWrapped());

    return columns;
}

WindowFunctionWorkspace createWorkspace(const Block & input_header, const WindowFunctionDescription & function, size_t result_column)
{
    WindowFunctionWorkspace workspace;
    workspace.aggregate_function = function.aggregate_function;
    workspace.result_column = result_column;
    const auto & aggregate_function = workspace.aggregate_function;

    workspace.argument_column_indices.reserve(function.argument_names.size());
    for (const auto & argument_name : function.argument_names)
        workspace.argument_column_indices.push_back(input_header.getPositionByName(argument_name));
    workspace.argument_columns.assign(function.argument_names.size(), nullptr);

    workspace.is_aggregate_function_state = aggregate_function->isState();
    workspace.aggregate_function_state.reset(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
    aggregate_function->create(workspace.aggregate_function_state.data());
    return workspace;
}

GeneralWindowFunctionSlot createSlot(GeneralWindowFunctionPtr general, size_t function_index, Arena & states_arena)
{
    GeneralWindowFunctionSlot slot{
        .function = std::move(general),
        .state = nullptr,
        .function_index = function_index,
    };

    if (slot.function->sizeOfState() != 0)
    {
        slot.state = states_arena.alignedAlloc(slot.function->sizeOfState(), slot.function->alignOfState());
        slot.function->createState(slot.state);
    }

    return slot;
}

void destroyStates(std::vector<GeneralWindowFunctionSlot> & general_functions, std::vector<WindowFunctionWorkspace> & workspaces)
{
    for (auto & slot : general_functions)
        if (slot.state)
            slot.function->destroyState(slot.state);

    for (auto & ws : workspaces)
        ws.aggregate_function->destroy(ws.aggregate_function_state.data());
}

}

WindowTransform::WindowTransform(
    SharedHeader input_header_, SharedHeader output_header_,
    const WindowDescription & window_description_, const std::vector<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , params(WindowTransformParams::create(*input_header_, window_description_, functions))
    , indexes(params)
    , arguments(params.input_header, functions)
    , frame(params)
{
    for (const auto & function : functions)
        if (const auto general = asGeneralWindowFunction(function.aggregate_function))
            general->checkWindowFrameType(params.window_description);

    try
    {
        general_functions.reserve(functions.size());
        workspaces.reserve(functions.size());
        for (size_t function_index = 0; function_index < functions.size(); ++function_index)
        {
            const WindowFunctionDescription & function = functions[function_index];
            if (auto general = asGeneralWindowFunction(function.aggregate_function))
            {
                general_functions.push_back(createSlot(std::move(general), function_index, states_arena));
            }
            else
            {
                workspaces.push_back(createWorkspace(params.input_header, function, function_index));
                if (!arena && function.aggregate_function->allocatesMemoryInArena())
                    arena = std::make_unique<Arena>();
            }
        }
    }
    catch (...)
    {
        destroyStates(general_functions, workspaces);
        throw;
    }
}

WindowTransform::~WindowTransform()
{
    destroyStates(general_functions, workspaces);
}

void WindowTransform::updateAggregationState()
{
    const FrameBounds & current_frame = frame.bounds();

    /// While the frame start stands still only the new tail rows are added; once it moves the whole frame is aggregated again.
    const bool frame_start_moved = current_frame.start.row_index_in_partition != prev_frame.start.row_index_in_partition;
    const RowNumber rows_to_add_start = frame_start_moved ? current_frame.start.location : prev_frame.end.location;
    const RowNumber rows_to_add_end = current_frame.end.location;

    for (auto & ws : workspaces)
    {
        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();
        if (frame_start_moved)
        {
            a->destroy(buf);
            a->create(buf);
        }

        for (int64_t block_number = rows_to_add_start.block; RowNumber{block_number, 0} < rows_to_add_end; ++block_number)
        {
            const SlidingBlock & block = blocks.blockAt(block_number);
            if (ws.cached_block_number != block_number)
            {
                for (size_t i = 0; i < ws.argument_column_indices.size(); ++i)
                    ws.argument_columns[i] = block.materialized_columns[ws.argument_column_indices[i]].get();
                ws.cached_block_number = block_number;
            }

            const int64_t first_row = block_number == rows_to_add_start.block ? rows_to_add_start.row : 0;
            const int64_t past_the_end_row = block_number == rows_to_add_end.block ? rows_to_add_end.row : block.rows_count;
            a->addBatchSinglePlace(first_row, past_the_end_row, buf, ws.argument_columns.data(), arena.get());
        }
    }
}

void WindowTransform::writeOutCurrentRow()
{
    const RowPoint & current = peers.current();
    const FrameBounds & current_frame = frame.bounds();

    /// The first row of a partition has no previous frame to compare with.
    const bool frame_unchanged = current.row_index_in_partition > 0
        && current_frame.start.row_index_in_partition == prev_frame.start.row_index_in_partition
        && current_frame.end.row_index_in_partition == prev_frame.end.row_index_in_partition;

    const SlidingBlock & block = blocks.blockAt(current.location.block);
    for (auto & ws : workspaces)
    {
        IColumn & result_column = *block.result_columns[ws.result_column];
        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();
        if (frame_unchanged && !ws.is_aggregate_function_state && current.location.row > 0)
        {
            /// Same frame as the previous row of this block, so its result is copied instead of finalized again.
            /// insertRangeFrom is safe on the column itself: it appends from a disjoint range, also for nested columns.
            chassert(std::cmp_equal(result_column.size(), current.location.row));
            result_column.insertRangeFrom(result_column, current.location.row - 1, 1);
        }
        else if (ws.is_aggregate_function_state)
        {
            /// A -State result is merged into the arena of the output column.
            a->insertMergeResultInto(buf, result_column, arena.get());
        }
        else
        {
            a->insertResultInto(buf, result_column, arena.get());
        }
    }
}

void WindowTransform::addInputBlock(Chunk chunk)
{
    auto rows_count = static_cast<int64_t>(chunk.getNumRows());
    auto materialized_columns = materializeColumns(chunk.getColumns(), params.should_materialize);
    auto index = indexes.calculate(materialized_columns, rows_count);
    arguments.add(materialized_columns, rows_count);
    auto & block = blocks.add(std::move(chunk), std::move(materialized_columns), std::move(index));
    partition.advance(blocks);

    block.result_columns.resize(general_functions.size() + workspaces.size());
    for (const auto & slot : general_functions)
        block.result_columns[slot.function_index] = slot.function->getResultType()->createColumn();
    for (const auto & ws : workspaces)
        block.result_columns[ws.result_column] = ws.aggregate_function->getResultType()->createColumn();
    for (auto & column : block.result_columns)
        column->reserve(block.rows_count);
}

void WindowTransform::computeReadyRows()
{
    /// Cancellation is checked per row here; general window functions evaluate each batch without cancellation checks
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

        peers.advancePeerGroupBounds(blocks, partition.bounds());
        frame.advance(blocks, peers.current(), partition.bounds());
        if (!frame.bounds().fully_visible)
            break;

        updateAggregationState();
        writeOutCurrentRow();
        recordCurrentRowBounds();

        prev_frame = frame.bounds();
        peers.advanceCurrentRow(blocks, partition.bounds());
    }

    evaluateRowsBatch();
}

void WindowTransform::recordCurrentRowBounds()
{
    const RowPoint & current = peers.current();
    const FrameBounds & current_frame = frame.bounds();
    const PeersBounds & current_peers = peers.bounds();
    if (bounds.rows == 0)
        batch_start = current.location;

    bounds.row.push_back(current.row_index_in_partition);
    bounds.frame_start.push_back(current_frame.start.row_index_in_partition);
    bounds.frame_end.push_back(current_frame.end.row_index_in_partition);
    bounds.peers_start.push_back(current_peers.start.row_index_in_partition);
    bounds.peers_end.push_back(current_peers.end.row_index_in_partition);
    bounds.peer_group_index.push_back(current.peer_group_index_in_partition);
    if (partition.bounds().fully_visible)
        bounds.partition_end.push_back(partition.bounds().end.row_index_in_partition);
    ++bounds.rows;
}

void WindowTransform::evaluateRowsBatch()
{
    if (bounds.rows == 0)
        return;

    const SlidingBlock & block = blocks.blockAt(batch_start.block);
    for (const auto & slot : general_functions)
        slot.function->evaluate(slot.state, bounds, arguments.view(slot.function_index), *block.result_columns[slot.function_index]);

    const bool block_complete = batch_start.row + bounds.rows == block.rows_count;
    if (block_complete)
        packReadyBlock();

    clearBounds(bounds);
}

void WindowTransform::packReadyBlock()
{
    const SlidingBlock & block = blocks.blockAt(batch_start.block);
    Columns columns = block.input_columns;
    columns.insert(columns.end(), std::make_move_iterator(block.result_columns.begin()), std::make_move_iterator(block.result_columns.end()));

    Chunk chunk;
    chunk.setColumns(std::move(columns), block.rows_count);
    pending_outputs.push_back(std::move(chunk));
}

void WindowTransform::startNextPartition()
{
    const RowNumber partition_start = partition.bounds().end.location;
    chassert(peers.current().location == partition_start);

    arguments.startPartition(partition.bounds().end.row_index_in_partition);
    partition.beginAt(blocks, partition_start);
    peers.enterPartition(partition_start);
    frame.enterPartition(partition_start);
    prev_frame = FrameBounds{.start = {.location = partition_start}, .end = {.location = partition_start}, .fully_visible = true};

    for (const auto & slot : general_functions)
        slot.function->resetPartition(slot.state);

    for (auto & ws : workspaces)
        ws.aggregate_function->destroy(ws.aggregate_function_state.data());

    /// Replaced between destroy and create, so it does not grow across partitions.
    if (arena)
        arena = std::make_unique<Arena>();

    for (auto & ws : workspaces)
        ws.aggregate_function->create(ws.aggregate_function_state.data());
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
    else if (!partition.bounds().fully_visible)
    {
        partition.finish();
    }

    computeReadyRows();
    releaseUnusedBlocks();
}

void WindowTransform::releaseUnusedBlocks()
{
    const auto first_used_block = std::min(prev_frame.start.location.block, peers.current().location.block);
    while (blocks.begin().block < first_used_block)
    {
        blocks.pop();
        arguments.pop();
    }
}

}
