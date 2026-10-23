#include <Processors/Transforms/WindowTransform.h>

#include <DataTypes/DataTypeLowCardinality.h>

#include <Common/Arena.h>

#include <algorithm>
#include <utility>
#include <ranges>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

Columns materializeColumns(Columns columns, const std::vector<bool> & should_materialize)
{
    for (auto && [column, materialize] : std::views::zip(columns, should_materialize))
        if (materialize)
            column = recursiveRemoveLowCardinality(column->convertToFullIfWrapped());

    return columns;
}

WindowFunctionWorkspace createWorkspace(const Block & input_header, const WindowFunctionDescription & function)
{
    WindowFunctionWorkspace workspace;
    workspace.aggregate_function = function.aggregate_function;
    const auto & aggregate_function = workspace.aggregate_function;

    workspace.argument_column_indices.reserve(function.argument_names.size());
    for (const auto & argument_name : function.argument_names)
        workspace.argument_column_indices.push_back(input_header.getPositionByName(argument_name));
    workspace.argument_columns.assign(function.argument_names.size(), nullptr);

    /// Window and aggregate functions still share one interface, so the window part is found by a cast.
    workspace.window_function_impl = dynamic_cast<IWindowFunction *>(const_cast<IAggregateFunction *>(aggregate_function.get()));
    workspace.is_aggregate_function_state = aggregate_function->isState();
    workspace.aggregate_function_state.reset(aggregate_function->sizeOfData(), aggregate_function->alignOfData());
    aggregate_function->create(workspace.aggregate_function_state.data());
    return workspace;
}

}

WindowTransform::WindowTransform(
    SharedHeader input_header_, SharedHeader output_header_,
    const WindowDescription & window_description_, const std::vector<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , params(WindowTransformParams::create(*input_header_, window_description_, functions))
    , indexes(params)
    , frame(params)
{
    workspaces.reserve(functions.size());
    try
    {
        for (const auto & function : functions)
        {
            const auto & workspace = workspaces.emplace_back(createWorkspace(params.input_header, function));
            if (workspace.window_function_impl && !workspace.window_function_impl->checkWindowFrameType(this))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported window frame type for function '{}'", workspace.aggregate_function->getName());

            if (!arena && workspace.aggregate_function->allocatesMemoryInArena())
                arena = std::make_unique<Arena>();
        }
    }
    catch (...)
    {
        for (auto & ws : workspaces)
            ws.aggregate_function->destroy(ws.aggregate_function_state.data());
        throw;
    }
}

WindowTransform::~WindowTransform()
{
    for (auto & ws : workspaces)
        ws.aggregate_function->destroy(ws.aggregate_function_state.data());
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
        if (ws.window_function_impl)
            continue;

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
    for (size_t function_index = 0; function_index < workspaces.size(); ++function_index)
    {
        auto & ws = workspaces[function_index];
        if (ws.window_function_impl)
        {
            ws.window_function_impl->windowInsertResultInto(this, function_index);
            continue;
        }

        IColumn & result_column = *block.result_columns[function_index];
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
    auto & block = blocks.add(std::move(chunk), std::move(materialized_columns), std::move(index));
    partition.advance(blocks);

    for (auto & ws : workspaces)
    {
        block.result_columns.push_back(ws.aggregate_function->getResultType()->createColumn());
        block.result_columns.back()->reserve(block.rows_count);
    }
}

void WindowTransform::computeReadyRows()
{
    /// The cancel check is per row, since one block can take long on quadratic frames; prepare picks up the rest.
    while (!isCancelled())
    {
        if (peers.current().location == partition.bounds().end.location)
        {
            /// The rest of this partition is not read yet, or the last partition is done.
            if (!partition.bounds().fully_visible || input_is_finished)
                return;

            startNextPartition();
        }

        peers.advancePeerGroupBounds(blocks, partition.bounds());
        frame.advance(blocks, peers.current(), partition.bounds());
        if (!frame.bounds().fully_visible)
            return;

        updateAggregationState();
        writeOutCurrentRow();

        prev_frame = frame.bounds();
        peers.advanceCurrentRow(blocks, partition.bounds());
    }
}

void WindowTransform::startNextPartition()
{
    const RowNumber partition_start = partition.bounds().end.location;
    chassert(peers.current().location == partition_start);

    partition.beginAt(blocks, partition_start);
    partition.advance(blocks);

    peers.enterPartition(partition_start);
    frame.enterPartition(partition_start);
    prev_frame = FrameBounds{.start = {.location = partition_start}, .end = {.location = partition_start}, .fully_visible = true};

    for (auto & ws : workspaces)
        if (!ws.window_function_impl)
            ws.aggregate_function->destroy(ws.aggregate_function_state.data());

    /// Replaced between destroy and create, so it does not grow across partitions.
    if (arena)
        arena = std::make_unique<Arena>();

    for (auto & ws : workspaces)
        if (!ws.window_function_impl)
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

    /// A block is ready once the current row has left it, because rows are computed in order.
    if (next_output_block_number < peers.current().location.block)
    {
        if (output.canPush())
        {
            const SlidingBlock & block = blocks.blockAt(next_output_block_number);
            auto columns = block.input_columns;
            for (auto & res : block.result_columns)
                columns.push_back(std::move(res));

            Chunk chunk;
            chunk.setColumns(columns, block.rows_count);
            ++next_output_block_number;
            output.push(std::move(chunk));
        }
        return Status::PortFull;
    }

    /// The input ended at the previous cycle and no output is ready, so everything is out.
    if (input_is_finished)
    {
        chassert(next_output_block_number == blocks.end().block);
        chassert(peers.current().location == blocks.end());
        output.finish();
        return Status::Finished;
    }

    /// Pulling with set_not_needed and an explicit setNeeded later makes the producer generate the next block in
    /// this thread right before our work, so the data is still hot.
    if (!pending_input && input.hasData())
    {
        pending_input = input.pull(true /* set_not_needed */);
        return Status::Ready;
    }

    /// No ready output and no more input: finalize once in work if the input is over, wait otherwise.
    if (input.isFinished())
    {
        chassert(!input_is_finished);
        input_is_finished = true;
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
    /// A block is dropped once it is output and both the previous frame start and the current row have left it.
    /// The previous frame start is never after the current one, and the frame can start after the current row.
    const auto first_used_block = std::min({next_output_block_number, prev_frame.start.location.block, peers.current().location.block});
    while (blocks.begin().block < first_used_block)
        blocks.pop();
}

}
