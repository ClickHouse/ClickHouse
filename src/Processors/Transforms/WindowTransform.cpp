#include <Processors/Transforms/WindowTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>


#include <Functions/FunctionHelpers.h>

#include <Core/SortCursor.h>

#include <Common/Arena.h>

#include <algorithm>
#include <ranges>
#include <utility>

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

void WindowTransform::advanceFrameStartRowsOffset()
{
    // Just recalculate it each time by walking blocks.
    const Int64 offset = static_cast<Int64>(params.window_description.frame.begin_offset.safeGet<UInt64>()) * (params.window_description.frame.begin_preceding ? -1 : 1);
    const std::optional<RowNumber> moved_row = blocks.move(current.location, offset);

    if (!moved_row && offset < 0)
    {
        // Walking back ran off the start of the stored blocks, so the logical
        // position is before the partition start, which may itself point to a
        // block that has already been freed.
        frame_start = partition.bounds().start;
        frame_started = true;
        return;
    }

    if (!moved_row || partition.bounds().end <= *moved_row)
    {
        // A FOLLOWING frame start ran into the end of partition.
        frame_start = partition.bounds().end;
        frame_started = partition.bounds().fully_visible;
        return;
    }

    if (*moved_row <= partition.bounds().start)
    {
        // Got to the beginning of partition and can't go further back.
        frame_start = partition.bounds().start;
        frame_started = true;
        return;
    }

    // Inside the partition, and we walked the whole offset, so it's final.
    frame_start = *moved_row;
    frame_started = true;
}


void WindowTransform::advanceFrameStartRangeOffset()
{
    const RowNumber partition_end = partition.bounds().end;
    // See the comment for advanceFrameEndRangeOffset().
    const int direction = params.window_description.order_by[0].direction;
    const bool preceding = params.window_description.frame.begin_preceding
        == (direction > 0);
    const auto * reference_column
        = blocks.blockAt(current.location.block).materialized_columns[params.order_by_indices[0]].get();
    for (; frame_start < partition_end; frame_start = blocks.next(frame_start))
    {
        // The first frame value is [the current row] with offset, so we advance
        // while [frames_start] < [the current row] with offset.
        const auto * compared_column
            = blocks.blockAt(frame_start.block).materialized_columns[params.order_by_indices[0]].get();
        if (params.range_offset_comparator(compared_column, frame_start.row,
            reference_column, current.location.row,
            params.window_description.frame.begin_offset,
            preceding)
                * direction >= 0)
        {
            frame_started = true;
            return;
        }
    }

    frame_started = partition.bounds().fully_visible;
}

void WindowTransform::advanceFrameStart()
{
    if (frame_started)
    {
        return;
    }

    const auto frame_start_before = frame_start;

    switch (params.window_description.frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            // UNBOUNDED PRECEDING, just mark it valid. It is initialized when
            // the new partition starts.
            // The partition start is in the first group.
            frame_start_group_number = 1;
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Current:
            // CURRENT ROW differs between frame types only in how the peer
            // groups are accounted.
            chassert(partition.bounds().start <= peer_group_start.location);
            chassert(peer_group_start.location < partition.bounds().end);
            chassert(peer_group_start.location <= current.location);
            frame_start = peer_group_start.location;
            // peer_group_start is in the current group.
            frame_start_group_number = current.peer_group_index_in_partition + 1;
            frame_started = true;
            break;
        case WindowFrame::BoundaryType::Offset:
            switch (params.window_description.frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    advanceFrameStartRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameStartRangeOffset();
                    break;
                case WindowFrame::FrameType::GROUPS:
                    advanceFrameStartGroupsOffset();
                    break;
            }
            break;
    }

    chassert(frame_start_before <= frame_start);
    if (frame_start == frame_start_before)
    {
        // The frame start didn't move. Usually this means we re-validated a
        // position reached on an earlier call, so the frame is now started.
        // This happens in degenerate cases where the frame start is further than
        // the end of partition, and the partition ends at the last row of the
        // block, but we can only tell for sure after a new block arrives.
        // A GROUPS frame with a FOLLOWING-offset start is the exception: it can
        // leave frame_start at its previous position when it still needs more
        // input to locate the target peer group. Then the frame is not started
        // yet and the partition cannot have ended -- the main loop waits for
        // more data and retries.
        chassert(frame_started || !partition.bounds().fully_visible);
    }

    chassert(partition.bounds().start <= frame_start);
    chassert(frame_start <= partition.bounds().end);
    if (partition.bounds().fully_visible && frame_start == partition.bounds().end)
    {
        // Check that if the start of frame (e.g. FOLLOWING) runs into the end
        // of partition, it is marked as valid -- we can't advance it any
        // further.
        chassert(frame_started);
    }
}

bool WindowTransform::arePeers(const RowNumber & x, const RowNumber & y) const
{
    if (x == y)
    {
        // For convenience, a row is always its own peer.
        return true;
    }

    return params.arePeers(blocks.blockAt(x.block).materialized_columns, x.row, blocks.blockAt(y.block).materialized_columns, y.row);
}

void WindowTransform::advanceFrameEndCurrentRow()
{
    const RowNumber partition_end = partition.bounds().end;
    // We only process one block here, and frame_end must be already in it: if
    // we didn't find the end in the previous block, frame_end is now the first
    // row of the current block. We need this knowledge to write a simpler loop
    // (only loop over rows and not over blocks), that should hopefully be more
    // efficient.
    // The partition end is either in this new block or past-the-end.
    chassert(frame_end.block  == partition_end.block
        || frame_end.block + 1 == partition_end.block);

    if (frame_end == partition_end)
    {
        // The case when we get a new block and find out that the partition has
        // ended.
        chassert(partition.bounds().fully_visible);
        frame_ended = partition.bounds().fully_visible;
        return;
    }

    // We advance until the partition end. It's either in the current block or
    // in the next one, which is also the past-the-end block. Figure out how
    // many rows we have to process.
    Int64 rows_end = 0;
    if (partition_end.row == 0)
    {
        chassert(partition_end == blocks.end());
        rows_end = blocks.blockAt(frame_end.block).rows_count;
    }
    else
    {
        chassert(frame_end.block == partition_end.block);
        rows_end = partition_end.row;
    }
    // Equality would mean "no data to process", for which we checked above.
    chassert(frame_end.row < rows_end);

    // Advance frame_end to the end of the current row's peer group.
    if (params.window_description.frame.type != WindowFrame::FrameType::ROWS)
    {
        // RANGE/GROUPS: peers are the rows whose ORDER BY values equal the current row's (or all rows if
        // there is no ORDER BY). The input is sorted by ORDER BY within the partition, so we find the
        // peer group's end with a fast equal-range scan.
        // First check whether frame_end is still a peer of the current row -- the reference (the current row)
        // may be in a different block, so we compare against it directly.
        const size_t order_by_columns = params.order_by_indices.size();
        size_t i = 0;
        for (; i < order_by_columns; ++i)
        {
            const auto * reference_column = blocks.blockAt(current.location.block).materialized_columns[params.order_by_indices[i]].get();
            const auto * compared_column = blocks.blockAt(frame_end.block).materialized_columns[params.order_by_indices[i]].get();
            if (compared_column->compareAt(frame_end.row, current.location.row, *reference_column, 1 /* nan_direction_hint */) != 0)
            {
                break;
            }
        }

        if (i < order_by_columns)
        {
            // frame_end is already past the current row's peer group.
            frame_ended = true;
            return;
        }

        // frame_end is a peer; extend over the run of equal ORDER BY values within this block,
        // narrowing key by key (the data is sorted lexicographically). With no ORDER BY, all rows are peers,
        // so the scan will just return the end of the block.
        const Int64 peer_group_end_row = findPeerRunEnd(frame_end.block, frame_end.row, rows_end);

        if (peer_group_end_row < rows_end)
        {
            frame_end.row = peer_group_end_row;
            frame_ended = true;
            return;
        }
        frame_end.row = rows_end;
    }
    else
    {
        // ROWS frame: a row is only its own peer, so the peer group is just the current row, and
        // frame_end sits at the current row on entry -- advancing it one row reaches the peer group's
        // end.
        if (frame_end == current.location)
            ++frame_end.row;

        if (frame_end.row < rows_end)
        {
            frame_ended = true;
            return;
        }
    }

    // Might have gotten to the end of the current block, have to properly
    // update the row number.
    if (frame_end.row == blocks.blockAt(frame_end.block).rows_count)
    {
        ++frame_end.block;
        frame_end.row = 0;
    }

    // Got to the end of partition (frame ended as well then) or end of data.
    chassert(frame_end == partition_end);
    frame_ended = partition.bounds().fully_visible;
}

void WindowTransform::advanceFrameEndUnbounded()
{
    // The UNBOUNDED FOLLOWING frame ends when the partition ends.
    frame_end = partition.bounds().end;
    frame_ended = partition.bounds().fully_visible;
}

void WindowTransform::advanceFrameEndRowsOffset()
{
    // Walk the specified offset from the current row. The "+1" is needed
    // because the frame_end is a past-the-end pointer.
    const Int64 offset = static_cast<Int64>(params.window_description.frame.end_offset.safeGet<UInt64>()) * (params.window_description.frame.end_preceding ? -1 : 1) + 1;
    const std::optional<RowNumber> moved_row = blocks.move(current.location, offset);

    if (!moved_row && offset < 0)
    {
        // Walking back ran off the start of the stored blocks, so the logical
        // position is before the partition start, which may itself point to a
        // block that has already been freed.
        frame_end = partition.bounds().start;
        frame_ended = true;
        return;
    }

    if (!moved_row || partition.bounds().end <= *moved_row)
    {
        // Clamp to the end of partition. It might not have ended yet, in which
        // case wait for more data.
        frame_end = partition.bounds().end;
        frame_ended = partition.bounds().fully_visible;
        return;
    }

    if (*moved_row <= partition.bounds().start)
    {
        // Clamp to the start of partition.
        frame_end = partition.bounds().start;
        frame_ended = true;
        return;
    }

    // Frame end inside partition, and we walked the whole offset, so it's final.
    frame_end = *moved_row;
    frame_ended = true;
}

void WindowTransform::advanceFrameEndRangeOffset()
{
    const RowNumber partition_end = partition.bounds().end;
    // PRECEDING/FOLLOWING change direction for DESC order.
    // See CD 9075-2:201?(E) 7.14 <window clause> p. 429.
    const int direction = params.window_description.order_by[0].direction;
    const bool preceding = params.window_description.frame.end_preceding
        == (direction > 0);
    const auto * reference_column
        = blocks.blockAt(current.location.block).materialized_columns[params.order_by_indices[0]].get();
    for (; frame_end < partition_end; frame_end = blocks.next(frame_end))
    {
        // The last frame value is the current row with offset, and we need a
        // past-the-end pointer, so we advance while
        // [frame_end] <= [the current row] with offset.
        const auto * compared_column
            = blocks.blockAt(frame_end.block).materialized_columns[params.order_by_indices[0]].get();
        if (params.range_offset_comparator(compared_column, frame_end.row,
            reference_column, current.location.row,
            params.window_description.frame.end_offset,
            preceding)
                * direction > 0)
        {
            frame_ended = true;
            return;
        }
    }

    frame_ended = partition.bounds().fully_visible;
}

ALWAYS_INLINE Int64 WindowTransform::findPeerRunEnd(Int64 block, Int64 begin, Int64 end)
{
    const auto & columns = blocks.blockAt(block).materialized_columns;
    if (params.order_by_indices.size() < 2)
        return getEqualRangeEndAssumeSorted(columns, params.order_by_indices, begin, end, 1 /* nan_direction_hint */);

    if (block != peer_runs_block || end != peer_runs_end)
    {
        peer_runs.reset(params.order_by_indices.size());
        peer_runs_block = block;
        peer_runs_end = end;
    }
    return getEqualRangeEndAssumeSorted(peer_runs, columns, params.order_by_indices, begin, end, 1 /* nan_direction_hint */);
}

RowNumber WindowTransform::findPeerGroupEnd(const RowNumber & start, RowNumber & scan_frontier, bool & need_more_data)
{
    const RowNumber partition_end = partition.bounds().end;
    need_more_data = false;

    if (start == partition_end)
        return partition_end;

    // Resume from the frontier of a previous, unfinished scan of the same peer group: every row in
    // [start, scan_frontier] is already known to be a peer of `start`. A frontier before `start` is
    // stale (the boundary has moved to another group or partition since the last scan).
    if (scan_frontier < start)
        scan_frontier = start;

    // Walk forward block by block while the peer group keeps extending.
    const Int64 blocks_end_block = blocks.end().block;
    for (RowNumber cur = scan_frontier; cur.block < blocks_end_block; cur = RowNumber{cur.block + 1, 0})
    {
        const Int64 block_rows = blocks.blockAt(cur.block).rows_count;
        const bool partition_ends_in_block = partition_end.block == cur.block;
        const Int64 end_bound = partition_ends_in_block ? partition_end.row : block_rows;

        // `cur` is a valid row inside the partition, so the equal-range search has at least one row.
        chassert(cur.row < end_bound);

        // Try to jump over the whole peer group at once: the end of the run of rows equal to `cur` across
        // all ORDER BY columns, within the sorted, partition-bounded range [cur.row, end_bound).
        const Int64 run_end = findPeerRunEnd(cur.block, cur.row, end_bound);

        if (run_end < end_bound)
            return RowNumber{cur.block, run_end};   // a real peer-group boundary inside this block

        // No earlier boundary, so the run of peers reached the bound. getEqualRangeEndAssumeSorted
        // never returns past `end_bound`, so the group extends exactly to the end of what we scanned
        // in this block -- the precondition for both the partition-end and cross-block cases below.
        chassert(run_end == end_bound);

        if (partition_ends_in_block)
            return partition_end;                   // the peer group reaches the partition end

        // The group extends to the end of `cur`'s block. It continues into the next block only if
        // that block is buffered, is still in this partition, and its first row is a peer.
        const RowNumber next_block_start{cur.block + 1, 0};

        // We cannot extend the scan into the next block when it has not arrived yet, or when the next
        // row is the partition boundary (a peer group never crosses partitions). In both cases the
        // group's end depends on whether the partition has ended, which is decided after the loop.
        // Remember the proven scan progress so a retry does not rescan the group from its first row.
        if (next_block_start.block >= blocks_end_block || next_block_start == partition_end)
        {
            scan_frontier = RowNumber{cur.block, block_rows - 1};
            break;
        }

        if (!arePeers({cur.block, block_rows - 1}, next_block_start))
            return next_block_start;                // the peer group ends exactly at the block boundary

        // Otherwise the group spans the boundary; the loop advances `cur` into the next block.
    }

    // We broke out because the group either reaches a partition boundary that sits on a block edge,
    // or extends past the rows we can currently resolve. If the partition has ended, the group ends
    // at the partition end.
    if (partition.bounds().fully_visible)
        return partition_end;

    // The partition has not ended and we ran past the buffered rows wait for more input.
    chassert(partition_end == blocks.end());
    need_more_data = true;
    return start;
}

bool WindowTransform::advanceGroupBoundary(RowNumber & pointer, Int64 & group_counter, RowNumber & scan_frontier, Int64 target_group)
{
    const RowNumber partition_end = partition.bounds().end;
    chassert(target_group >= 1);

    while (group_counter < target_group)
    {
        bool need_more_data = false;
        const RowNumber group_end = findPeerGroupEnd(pointer, scan_frontier, need_more_data);

        if (need_more_data)
        {
            // Leave `pointer` and `group_counter` untouched so we can resume later.
            return false;
        }

        if (group_end == partition_end)
        {
            // The target peer group is past the last group in the partition; clamp to the end.
            pointer = partition_end;
            return true;
        }

        // Move to the first row of the next peer group.
        pointer = group_end;
        ++group_counter;
    }

    return true;
}

void WindowTransform::advanceFrameStartGroupsOffset()
{
    const Int64 offset
        = static_cast<Int64>(params.window_description.frame.begin_offset.safeGet<UInt64>()) * (params.window_description.frame.begin_preceding ? -1 : 1);

    // The frame starts at the first row of the peer group `offset` groups away from the current one.
    const Int64 peer_group_number = current.peer_group_index_in_partition + 1;
    const Int64 target_group = peer_group_number + offset;

    if (target_group <= 1)
    {
        // The target peer group is at or before the first group: clamp to the partition start.
        frame_start = partition.bounds().start;
        frame_start_group_number = 1;
        frame_started = true;
        return;
    }

    frame_started = advanceGroupBoundary(frame_start, frame_start_group_number, frame_start_group_scan_frontier, target_group);
}

void WindowTransform::advanceFrameEndGroupsOffset()
{
    if (frame_end == frame_start)
        frame_end_group_number = frame_start_group_number;

    const Int64 offset
        = static_cast<Int64>(params.window_description.frame.end_offset.safeGet<UInt64>()) * (params.window_description.frame.end_preceding ? -1 : 1);

    // frame_end is not inclusive, so it must reach the first row of the group after the target one.
    const Int64 peer_group_number = current.peer_group_index_in_partition + 1;
    const Int64 target_group = peer_group_number + offset + 1;

    if (target_group <= 1)
    {
        // The frame ends before the first peer group: it is empty.
        frame_end = frame_start;
        frame_end_group_number = frame_start_group_number;
        frame_ended = true;
        return;
    }

    frame_ended = advanceGroupBoundary(frame_end, frame_end_group_number, frame_end_group_scan_frontier, target_group);
}

void WindowTransform::advanceFrameEnd()
{
    // No reason for this function to be called again after it succeeded.
    chassert(!frame_ended);

    const auto frame_end_before = frame_end;

    switch (params.window_description.frame.end_type)
    {
        case WindowFrame::BoundaryType::Current:
            advanceFrameEndCurrentRow();
            break;
        case WindowFrame::BoundaryType::Unbounded:
            advanceFrameEndUnbounded();
            break;
        case WindowFrame::BoundaryType::Offset:
            switch (params.window_description.frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    advanceFrameEndRowsOffset();
                    break;
                case WindowFrame::FrameType::RANGE:
                    advanceFrameEndRangeOffset();
                    break;
                case WindowFrame::FrameType::GROUPS:
                    advanceFrameEndGroupsOffset();
                    break;
            }
            break;
    }

    // We might not have advanced the frame end if we found out we reached the
    // end of input or the partition, or if we still don't know the frame start.
    if (frame_end_before == frame_end)
    {
        return;
    }
}

// Update the aggregation states after the frame has changed.
void WindowTransform::updateAggregationState()
{
    // Assert that the frame boundaries are known, have proper order wrt each
    // other, and have not gone back wrt the previous frame.
    chassert(frame_started);
    chassert(frame_ended);
    chassert(frame_start <= frame_end);
    chassert(prev_frame_start <= prev_frame_end);
    chassert(prev_frame_start <= frame_start);
    chassert(prev_frame_end <= frame_end);
    chassert(partition.bounds().start <= frame_start);
    chassert(frame_end <= partition.bounds().end);

    // We might have to reset aggregation state and/or add some rows to it.
    // Figure out what to do.
    bool reset_aggregation = false;
    RowNumber rows_to_add_start;
    RowNumber rows_to_add_end;
    if (frame_start == prev_frame_start)
    {
        // The frame start didn't change, add the tail rows.
        reset_aggregation = false;
        rows_to_add_start = prev_frame_end;
        rows_to_add_end = frame_end;
    }
    else
    {
        // The frame start changed, reset the state and aggregate over the
        // entire frame. This can be made per-function after we learn to
        // subtract rows from some types of aggregation states, but for now we
        // always have to reset when the frame start changes.
        reset_aggregation = true;
        rows_to_add_start = frame_start;
        rows_to_add_end = frame_end;
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
    chassert(current.location < partition.bounds().end);
    chassert(current.location.block >= blocks.begin().block);

    // Whether this row's frame equals the previous row's. The first row of the partition has no
    // previous row in this partition (and thus no previous frame) to compare against.
    const bool frame_unchanged = current.row_index_in_partition > 0 && frame_start == prev_frame_start && frame_end == prev_frame_end;

    const auto & block = blocks.blockAt(current.location.block);
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

        if (frame_unchanged && !ws.is_aggregate_function_state && current.location.row > 0)
        {
            // Same frame as the previous row -> same result. When that row is in this same block its
            // result is already in result_column one position back, so copy it instead of
            // re-finalizing. We copy the column into itself with insertRangeFrom (not insertFrom):
            // insertRangeFrom appends via resize + memcpy from a disjoint source range, which is
            // self-safe even if the append reallocates and even for nested columns (Array, Variant,
            // Dynamic, JSON) whose sub-columns are not covered by the top-level reserve.
            chassert(std::cmp_equal(result_column->size(), current.location.row));
            result_column->insertRangeFrom(*result_column, current.location.row - 1, 1);
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
        while (current.location < partition_end)
        {
            // We now know that the current row is valid, so we can update the peer group start.
            if (peer_group_start.location != current.location && blocks.blockAt(current.location.block).index.peer_group_starts[current.location.row])
            {
                ++current.peer_group_index_in_partition;
                peer_group_start = current;
            }

            // Advance the frame start.
            advanceFrameStart();

            if (!frame_started)
            {
                // Wait for more input data to find the start of frame.
                chassert(!input_is_finished);
                chassert(!partition.bounds().fully_visible);
                return;
            }

            // frame_end must be greater or equal than frame_start, so if the
            // frame_start is already past the current frame_end, we can start
            // from it to save us some work.
            if (frame_end < frame_start)
            {
                frame_end = frame_start;
            }

            // Advance the frame end.
            advanceFrameEnd();

            if (!frame_ended)
            {
                // Wait for more input data to find the end of frame.
                chassert(!input_is_finished);
                chassert(!partition.bounds().fully_visible);
                return;
            }

            // The frame can be empty sometimes, e.g. the boundaries coincide
            // or the start is after the partition end. But hopefully start is
            // not after end.
            chassert(frame_started);
            chassert(frame_ended);
            chassert(frame_start <= frame_end);

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

            prev_frame_start = frame_start;
            prev_frame_end = frame_end;

            // Move to the next row. The frame will have to be recalculated.
            // The peer group start is updated at the beginning of the loop,
            // because the current row might now be past-the-end.
            current.location = blocks.next(current.location);
            ++current.row_index_in_partition;
            frame_ended = false;
            frame_started = false;
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
    }
}

void WindowTransform::startNextPartition()
{
    const RowNumber partition_start = partition.bounds().end;
    partition.beginAt(blocks, partition_start);
    partition.advance(blocks);
    // We have to reset the frame and other pointers when the new partition
    // starts.
    frame_start = partition_start;
    frame_end = partition_start;
    prev_frame_start = partition_start;
    prev_frame_end = partition_start;
    chassert(current.location == partition_start);
    current = RowPoint{.location = partition_start};
    peer_group_start = current;
    frame_start_group_number = 1;
    frame_end_group_number = 1;

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

    chassert(current.location.block >= blocks.begin().block);
    // The the current row might be past-the-end if we have already calculated the
    // window functions for all input rows. That's why the equality is also
    // valid here.
    chassert(current.location.block <= blocks.end().block);

    // Output the ready data prepared by work(). A block is ready when the
    // current row has left it, because rows are computed in order.
    // We inspect the calculation state and create the output chunk right here,
    // because this is pretty lightweight.
    if (next_output_block_number < current.location.block)
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
        chassert(current.location == blocks.end());

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
    chassert(prev_frame_start <= frame_start);
    const auto first_used_block = std::min({next_output_block_number, prev_frame_start.block, current.location.block});
    while (blocks.begin().block < first_used_block)
        blocks.pop();

    chassert(frame_start.block >= blocks.begin().block);
    chassert(prev_frame_start.block >= blocks.begin().block);
    chassert(current.location.block >= blocks.begin().block);
}

}
