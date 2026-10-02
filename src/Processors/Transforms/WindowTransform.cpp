#include <Processors/Transforms/WindowTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>


#include <Functions/FunctionHelpers.h>

#include <Common/Arena.h>

#include <algorithm>
#include <utility>
#include <ranges>

/// See https://fmt.dev/latest/api.html#formatting-user-defined-types
template <>
struct fmt::formatter<DB::RowNumber>
{
    static constexpr auto parse(format_parse_context & ctx)
    {
        const auto * it = ctx.begin();
        const auto * end = ctx.end();

        /// Only support {}.
        if (it != end && *it != '}')
            throw fmt::format_error("Invalid format");

        return it;
    }

    template <typename FormatContext>
    auto format(const DB::RowNumber & x, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}:{}", x.block, x.row);
    }
};

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

/// The order of everything the row loop keeps; every check is a no-op in release builds.
void checkInvariants(
    const SlidingBlocks & blocks,
    int64_t next_output_block_number,
    const PartitionBounds & partition,
    const Peers & peers,
    const FrameBounds & frame,
    const FrameBounds & prev_frame)
{
    const RowPoint & current = peers.current();
    const PeersBounds & peer_group = peers.bounds();

    chassert(partition.start <= peer_group.start.location);
    chassert(peer_group.start.location <= current.location);
    chassert(current.location <= partition.end);
    chassert(peer_group.start.peer_group_index_in_partition == current.peer_group_index_in_partition);
    chassert(current.peer_group_index_in_partition <= current.row_index_in_partition);
    if (peer_group.fully_visible)
    {
        chassert(current.location < peer_group.end.location || current.location == partition.end);
        chassert(peer_group.end.location <= partition.end);
    }

    chassert(partition.start <= prev_frame.start.location);
    chassert(prev_frame.start.location <= prev_frame.end.location);
    chassert(prev_frame.end.location <= partition.end);
    if (frame.fully_visible)
    {
        chassert(prev_frame.start.location <= frame.start.location);
        chassert(frame.start.location <= frame.end.location);
        chassert(prev_frame.end.location <= frame.end.location);
        chassert(frame.end.location <= partition.end);
    }

    chassert(blocks.begin().block <= std::min(prev_frame.start.location.block, current.location.block));
    chassert(next_output_block_number <= current.location.block);
}

}

WindowTransform::WindowTransform(SharedHeader input_header_,
        SharedHeader output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & functions)
    : IProcessor({input_header_}, {output_header_})
    , params(WindowTransformParams::create(*input_header_, window_description_, functions))
    , input(inputs.front())
    , output(outputs.front())
    , indexes(params)
    , frame(params)
{
    initWorkspaces(functions);
}

void WindowTransform::initWorkspaces(const std::vector<WindowFunctionDescription> & functions)
{
    workspaces.reserve(functions.size());
    for (const auto & f : functions)
    {
        WindowFunctionWorkspace workspace;
        workspace.aggregate_function = f.aggregate_function;
        const auto & aggregate_function = workspace.aggregate_function;
        if (!arena && aggregate_function->allocatesMemoryInArena())
        {
            arena = std::make_unique<Arena>();
        }

        workspace.argument_column_indices.reserve(f.argument_names.size());
        for (const auto & argument_name : f.argument_names)
        {
            workspace.argument_column_indices.push_back(
                params.input_header.getPositionByName(argument_name));
        }
        workspace.argument_columns.assign(f.argument_names.size(), nullptr);

        /// Currently we have slightly wrong mixup of the interfaces of Window and Aggregate functions.
        workspace.window_function_impl = dynamic_cast<IWindowFunction *>(const_cast<IAggregateFunction *>(aggregate_function.get()));

        if (workspace.window_function_impl && !workspace.window_function_impl->checkWindowFrameType(this))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported window frame type for function '{}'", workspace.aggregate_function->getName());

        workspace.is_aggregate_function_state = workspace.aggregate_function->isState();
        workspace.aggregate_function_state.reset(
            aggregate_function->sizeOfData(),
            aggregate_function->alignOfData());
        aggregate_function->create(workspace.aggregate_function_state.data());

        workspaces.push_back(std::move(workspace));
    }
}

WindowTransform::~WindowTransform()
{
    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        ws.aggregate_function->destroy(
            ws.aggregate_function_state.data());
    }
}

// Update the aggregation states after the frame has changed.
void WindowTransform::updateAggregationState()
{
    const FrameBounds current_frame = frame.bounds();
    chassert(current_frame.fully_visible);

    // We might have to reset aggregation state and/or add some rows to it.
    // Figure out what to do.
    bool reset_aggregation = false;
    RowNumber rows_to_add_start;
    RowNumber rows_to_add_end;
    if (current_frame.start.location == prev_frame.start.location)
    {
        // The frame start didn't change, add the tail rows.
        reset_aggregation = false;
        rows_to_add_start = prev_frame.end.location;
        rows_to_add_end = current_frame.end.location;
    }
    else
    {
        // The frame start changed, reset the state and aggregate over the
        // entire frame. This can be made per-function after we learn to
        // subtract rows from some types of aggregation states, but for now we
        // always have to reset when the frame start changes.
        reset_aggregation = true;
        rows_to_add_start = current_frame.start.location;
        rows_to_add_end = current_frame.end.location;
    }

    for (auto & ws : workspaces)
    {
        if (ws.window_function_impl)
        {
            // No need to do anything for true window functions.
            continue;
        }

        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (reset_aggregation)
        {
            a->destroy(buf);
            a->create(buf);
        }

        // To achieve better performance, we will have to loop over blocks and
        // rows manually, instead of using advanceRowNumber().
        // For this purpose, the past-the-end block can be different than the
        // block of the past-the-end row (it's usually the next block).
        const auto past_the_end_block = rows_to_add_end.row == 0
            ? rows_to_add_end.block
            : rows_to_add_end.block + 1;

        for (auto block_number = rows_to_add_start.block;
             block_number < past_the_end_block;
             ++block_number)
        {
            const auto & block = blocks.blockAt(block_number);

            if (ws.cached_block_number != block_number)
            {
                for (size_t i = 0; i < ws.argument_column_indices.size(); ++i)
                {
                    ws.argument_columns[i] = block.materialized_columns[
                        ws.argument_column_indices[i]].get();
                }
                ws.cached_block_number = block_number;
            }

            // First and last blocks may be processed partially, and other blocks
            // are processed in full.
            const auto first_row = block_number == rows_to_add_start.block
                ? rows_to_add_start.row : 0;
            const auto past_the_end_row = block_number == rows_to_add_end.block
                ? rows_to_add_end.row : block.rows_count;

            // We should add an addBatch analog that can accept a starting offset.
            // For now, add the values one by one.
            auto * columns = ws.argument_columns.data();
            // Removing arena.get() from the loop makes it faster somehow...
            auto * arena_ptr = arena.get();
            a->addBatchSinglePlace(first_row, past_the_end_row, buf, columns, arena_ptr);
        }
    }
}

void WindowTransform::writeOutCurrentRow()
{
    chassert(peers.current().location < partition.bounds().end);
    chassert(peers.current().location.block >= blocks.begin().block);

    // Whether this row's frame equals the previous row's. The first row of the partition has no
    // previous row in this partition (and thus no previous frame) to compare against.
    const FrameBounds current_frame = frame.bounds();
    const bool frame_unchanged = peers.current().row_index_in_partition > 0 && current_frame.start.location == prev_frame.start.location && current_frame.end.location == prev_frame.end.location;

    const auto & block = blocks.blockAt(peers.current().location.block);
    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];

        if (ws.window_function_impl)
        {
            ws.window_function_impl->windowInsertResultInto(this, wi);
            continue;
        }

        IColumn * result_column = block.result_columns[wi].get();
        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        if (frame_unchanged && !ws.is_aggregate_function_state && peers.current().location.row > 0)
        {
            // Same frame as the previous row -> same result. When that row is in this same block its
            // result is already in result_column one position back, so copy it instead of
            // re-finalizing. We copy the column into itself with insertRangeFrom (not insertFrom):
            // insertRangeFrom appends via resize + memcpy from a disjoint source range, which is
            // self-safe even if the append reallocates and even for nested columns (Array, Variant,
            // Dynamic, JSON) whose sub-columns are not covered by the top-level reserve.
            chassert(std::cmp_equal(result_column->size(), peers.current().location.row));
            result_column->insertRangeFrom(*result_column, peers.current().location.row - 1, 1);
        }
        else if (ws.is_aggregate_function_state)
        {
            /// We should use insertMergeResultInto to insert result into ColumnAggregateFunction
            /// correctly if result contains AggregateFunction's states
            a->insertMergeResultInto(buf, *result_column, arena.get());
        }
        else
        {
            a->insertResultInto(buf, *result_column, arena.get());
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

    // Initialize output columns.
    for (auto & ws : workspaces)
    {
        block.result_columns.push_back(ws.aggregate_function->getResultType()->createColumn());
        block.result_columns.back()->reserve(block.rows_count);
    }
}

void WindowTransform::computeReadyRows()
{
    for (;;)
    {
        // Either we ran out of data or we found the end of partition (maybe
        // both, but this only happens at the total end of data).
        const RowNumber partition_end = partition.bounds().end;
        chassert(partition.bounds().fully_visible || partition_end == blocks.end());
        if (partition.bounds().fully_visible && partition_end == blocks.end())
        {
            chassert(input_is_finished);
        }

        // After that, try to calculate window functions for each next row.
        // We can continue until the end of partition or current end of data,
        // which is precisely the definition of the known end of the partition.
        while (peers.current().location < partition_end)
        {
            peers.advancePeerGroupBounds(blocks, partition.bounds());
            checkInvariants(blocks, next_output_block_number, partition.bounds(), peers, frame.bounds(), prev_frame);

            frame.advance(blocks, peers.current(), partition.bounds());
            checkInvariants(blocks, next_output_block_number, partition.bounds(), peers, frame.bounds(), prev_frame);

            if (!frame.bounds().fully_visible)
            {
                // Wait for more input data to find the frame.
                chassert(!input_is_finished);
                chassert(!partition.bounds().fully_visible);
                return;
            }

            // Now that we know the new frame boundaries, update the aggregation
            // states. Theoretically we could do this simultaneously with moving
            // the frame boundaries, but it would require some care not to
            // perform unnecessary work while we are still looking for the frame
            // start, so do it the simple way for now.
            updateAggregationState();

            // Write out the aggregation results.
            writeOutCurrentRow();

            if (isCancelled())
            {
                // Good time to check if the query is cancelled. Checking once
                // per block might not be enough in severe quadratic cases.
                // Just leave the work halfway through and return, the 'prepare'
                // method will figure out what to do. Note that this doesn't
                // handle 'max_execution_time' and other limits, because these
                // limits are only updated between blocks. Eventually we should
                // start updating them in background and canceling the processor,
                // like we do for Ctrl+C handling.
                //
                // This class is final, so the check should hopefully be
                // devirtualized and become a single never-taken branch that is
                // basically free.
                return;
            }

            prev_frame = frame.bounds();

            // Move to the next row. The frame will have to be recalculated.
            peers.advanceCurrentRow(blocks, partition.bounds());
            checkInvariants(blocks, next_output_block_number, partition.bounds(), peers, frame.bounds(), prev_frame);
        }

        if (input_is_finished)
        {
            // We finalized the last partition in the above loop, and don't have
            // to do anything else.
            return;
        }

        if (!partition.bounds().fully_visible)
        {
            // Wait for more input data to find the end of partition.
            // Assert that we processed all the data we currently have, and that
            // we are going to receive more data.
            chassert(partition_end == blocks.end());
            chassert(!input_is_finished);
            return;
        }

        startNextPartition();
        checkInvariants(blocks, next_output_block_number, partition.bounds(), peers, frame.bounds(), prev_frame);
    }
}

void WindowTransform::startNextPartition()
{
    const RowNumber partition_start = partition.bounds().end;
    chassert(peers.current().location == partition_start);

    partition.beginAt(blocks, partition_start);
    partition.advance(blocks);

    peers.enterPartition(blocks, partition.bounds());
    frame.enterPartition(blocks, peers.current(), partition.bounds());
    prev_frame = FrameBounds{.start = peers.current(), .end = peers.current(), .fully_visible = true};

    // Reinitialize the aggregate function states because the new partition
    // has started.
    for (auto & ws : workspaces)
    {
        if (ws.window_function_impl)
        {
            continue;
        }

        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        a->destroy(buf);
    }

    // Replace the arena so that it does not grow across partitions. All states
    // were destroyed above and no result lives in it, see the field comment.
    if (arena)
    {
        arena = std::make_unique<Arena>();
    }

    for (auto & ws : workspaces)
    {
        if (ws.window_function_impl)
        {
            continue;
        }

        const auto * a = ws.aggregate_function.get();
        auto * buf = ws.aggregate_function_state.data();

        a->create(buf);
    }
}

IProcessor::Status WindowTransform::prepare()
{
    if (output.isFinished() || isCancelled())
    {
        // output.isFinished(): the consumer closed the port early, e.g. LIMIT is
        // satisfied. isCancelled(): KILL QUERY, a client disconnect or Ctrl+C
        // cancelled the processor. Either way there is nothing more to produce.
        input.close();
        return Status::Finished;
    }

    chassert(peers.current().location.block >= blocks.begin().block);
    // The the current row might be past-the-end if we have already calculated the
    // window functions for all input rows. That's why the equality is also
    // valid here.
    chassert(peers.current().location.block <= blocks.end().block);

    // Output the ready data prepared by work(). A block is ready when the
    // current row has left it, because rows are computed in order.
    // We inspect the calculation state and create the output chunk right here,
    // because this is pretty lightweight.
    if (next_output_block_number < peers.current().location.block)
    {
        if (output.canPush())
        {
            // Output the ready block.
            const auto & block = blocks.blockAt(next_output_block_number);
            auto columns = block.input_columns;
            for (auto & res : block.result_columns)
                columns.push_back(std::move(res));

            Chunk chunk;
            chunk.setColumns(columns, block.rows_count);

            ++next_output_block_number;

            output.push(std::move(chunk));
        }

        // We don't need input.setNotNeeded() here, because we already pull with
        // the set_not_needed flag.
        return Status::PortFull;
    }

    if (input_is_finished)
    {
        // The input data ended at the previous prepare() + work() cycle,
        // and we don't have ready output data (checked above). We must be
        // finished.
        chassert(next_output_block_number == blocks.end().block);
        chassert(peers.current().location == blocks.end());

        // The consumer learns that the data ended only from the closed output port.
        output.finish();

        return Status::Finished;
    }

    // Consume input data if we have any ready.
    if (!pending_input && input.hasData())
    {
        // Pulling with set_not_needed = true and using an explicit setNeeded()
        // later is somewhat more efficient, because after the setNeeded(), the
        // required input block will be generated in the same thread and passed
        // to our prepare() + work() methods in the same thread right away, so
        // hopefully we will work on hot (cached) data.
        pending_input = input.pull(true /* set_not_needed */);

        // Now we have new input and can try to generate more output in work().
        return Status::Ready;
    }

    // We 1) don't have any ready output (checked above),
    // 2) don't have any more input (also checked above).
    // Will we get any more input?
    if (input.isFinished())
    {
        // We won't, time to finalize the calculation in work(). We should only
        // do this once.
        chassert(!input_is_finished);
        input_is_finished = true;
        return Status::Ready;
    }

    // We have to wait for more input.
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
        partition.finish(blocks.end());
    }

    computeReadyRows();
    releaseUnusedBlocks();
}

void WindowTransform::releaseUnusedBlocks()
{
    // We don't really have to keep the entire partition, and it can be big, so
    // we want to drop the starting blocks to save memory. We can drop the old
    // blocks if we already returned them as output, and the frame and the
    // current row are already past them. The previous frame start is never
    // after the current frame start, so we don't have to check the latter. Note
    // that the frame start can be further than current row for some frame specs
    // (e.g. EXCLUDE CURRENT ROW), so we have to check both.
    const auto first_used_block = std::min({next_output_block_number, prev_frame.start.location.block, peers.current().location.block});
    while (blocks.begin().block < first_used_block)
        blocks.pop();
}

}
