#include <Processors/Transforms/WindowTransform.h>

#include <Columns/ColumnAggregateFunction.h>
#include <DataTypes/DataTypeLowCardinality.h>


#include <Functions/FunctionHelpers.h>

#include <Core/SortCursor.h>

#include <Common/Arena.h>

#include <Common/memory.h>

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
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

}

bool WindowTransform::aggregateFunctionSupportsFrameTree(const IAggregateFunction & function)
{
    // Zero-sized states (the Nothing placeholders for only-NULL arguments) would
    // make every segment slot alias the same address, and functions with a
    // constant-time batch add re-aggregate any frame for free; both keep the
    // recompute path.
    return function.sizeOfData() != 0
        && function.mergeIsEquivalentToAddingRows()
        && !function.addBatchSinglePlaceIsConstant();
}

WindowTransform::WindowTransform(SharedHeader input_header_,
        SharedHeader output_header_,
        const WindowDescription & window_description_,
        const std::vector<WindowFunctionDescription> & functions,
        UInt64 min_frame_rows_for_aggregate_tree_)
    : IProcessor({input_header_}, {output_header_})
    , params(WindowTransformParams::create(*input_header_, window_description_, functions))
    , input(inputs.front())
    , output(outputs.front())
    , min_frame_rows_for_aggregate_tree(min_frame_rows_for_aggregate_tree_)
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
    workspace_frame_trees.resize(workspaces.size());
    for (size_t i = 0; i < workspaces.size(); ++i)
    {
        if (workspaces[i].window_function_impl)
            continue;
        workspace_frame_trees[i].merge_equivalent = aggregateFunctionSupportsFrameTree(*workspaces[i].aggregate_function);
        workspace_frame_trees[i].has_trivial_destructor = workspaces[i].aggregate_function->hasTrivialDestructor();
        any_workspace_supports_frame_tree |= workspace_frame_trees[i].merge_equivalent;
    }
}

WindowTransform::~WindowTransform()
{
    // The tree states may only be destroyed while the arena is still alive.
    resetFrameTrees();

    // Some states may be not created yet if the creation failed.
    for (auto & ws : workspaces)
    {
        ws.aggregate_function->destroy(
            ws.aggregate_function_state.data());
    }
}

void WindowTransform::advanceFrameStartRowsOffset()
{
    const auto & frame = params.window_description.frame;
    const Int64 offset = static_cast<Int64>(frame.begin_offset.safeGet<UInt64>()) * (frame.begin_preceding ? -1 : 1);

    std::optional<RowNumber> moved_row;
    if (frame.begin_preceding)
    {
        // The unclamped frame start is current_row - N and current_row advances one row
        // per call, so the position is maintained incrementally instead of re-walking the
        // blocks every time. A FOLLOWING frame start is not cached: its walk may stop at
        // not-yet-arrived blocks and must be recomputed until fully walked.
        const bool advanced_one_row = frame_start_rows_cache_valid
            && ((current_row.block == frame_start_rows_cache_current.block
                    && current_row.row == frame_start_rows_cache_current.row + 1)
                || (current_row.block == frame_start_rows_cache_current.block + 1 && current_row.row == 0
                    && frame_start_rows_cache_current_at_block_end));
        // While clamped with a deficit of more than one row the pinned row is not consulted
        // (the result is clamped to the partition start below), so it may lie in a freed block;
        // the moment it becomes the real position its block must still exist, otherwise
        // recompute from current_row.
        bool cached = false;
        if (advanced_one_row)
        {
            if (frame_start_rows_cache_offset_left < -1)
            {
                ++frame_start_rows_cache_offset_left;
                cached = true;
            }
            else if (frame_start_rows_cache_row.block >= blocks.begin().block)
            {
                if (frame_start_rows_cache_offset_left == -1)
                    ++frame_start_rows_cache_offset_left;
                else
                    frame_start_rows_cache_row = blocks.next(frame_start_rows_cache_row);
                cached = true;
            }
        }
        if (!cached)
        {
            if (const auto walked_row = blocks.move(current_row, offset))
            {
                frame_start_rows_cache_row = *walked_row;
                frame_start_rows_cache_offset_left = 0;
            }
            else
            {
                // Walking back ran off the start of the stored blocks: pin the position at the
                // first stored row and remember how many rows are still left to walk back.
                frame_start_rows_cache_row = blocks.begin();
                frame_start_rows_cache_offset_left = offset + static_cast<Int64>(countRowsBetween(blocks.begin(), current_row));
                chassert(frame_start_rows_cache_offset_left < 0);
            }
        }
        frame_start_rows_cache_valid = true;
        frame_start_rows_cache_current = current_row;
        frame_start_rows_cache_current_at_block_end = current_row.row + 1 == blocks.blockAt(current_row.block).rows_count;

        if (frame_start_rows_cache_offset_left == 0)
            moved_row = frame_start_rows_cache_row;
    }
    else
    {
        frame_start_rows_cache_valid = false;
        moved_row = blocks.move(current_row, offset);
    }

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
        = blocks.blockAt(current_row.block).materialized_columns[params.order_by_indices[0]].get();
    for (; frame_start < partition_end; frame_start = blocks.next(frame_start))
    {
        // The first frame value is [current_row] with offset, so we advance
        // while [frames_start] < [current_row] with offset.
        const auto * compared_column
            = blocks.blockAt(frame_start.block).materialized_columns[params.order_by_indices[0]].get();
        if (params.range_offset_comparator(compared_column, frame_start.row,
            reference_column, current_row.row,
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
            chassert(partition.bounds().start <= peer_group_start);
            chassert(peer_group_start < partition.bounds().end);
            chassert(peer_group_start <= current_row);
            frame_start = peer_group_start;
            // peer_group_start is in the current group.
            frame_start_group_number = peer_group_number;
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
        // RANGE/GROUPS: peers are the rows whose ORDER BY values equal current_row's (or all rows if
        // there is no ORDER BY). The input is sorted by ORDER BY within the partition, so we find the
        // peer group's end with a fast equal-range scan.
        // First check whether frame_end is still a peer of current_row -- the reference (current_row)
        // may be in a different block, so we compare against it directly.
        const size_t order_by_columns = params.order_by_indices.size();
        size_t i = 0;
        for (; i < order_by_columns; ++i)
        {
            const auto * reference_column = blocks.blockAt(current_row.block).materialized_columns[params.order_by_indices[i]].get();
            const auto * compared_column = blocks.blockAt(frame_end.block).materialized_columns[params.order_by_indices[i]].get();
            if (compared_column->compareAt(frame_end.row, current_row.row, *reference_column, 1 /* nan_direction_hint */) != 0)
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
        const Int64 peer_group_end_row
            = getEqualRangeEndAssumeSorted(blocks.blockAt(frame_end.block).materialized_columns, params.order_by_indices, frame_end.row, rows_end, 1 /* nan_direction_hint */);

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
        // ROWS frame: a row is only its own peer, so the peer group is just current_row, and
        // frame_end sits at current_row on entry -- advancing it one row reaches the peer group's
        // end.
        if (frame_end == current_row)
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
    const std::optional<RowNumber> moved_row = blocks.move(current_row, offset);

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
        = blocks.blockAt(current_row.block).materialized_columns[params.order_by_indices[0]].get();
    for (; frame_end < partition_end; frame_end = blocks.next(frame_end))
    {
        // The last frame value is current_row with offset, and we need a
        // past-the-end pointer, so we advance while
        // [frame_end] <= [current_row] with offset.
        const auto * compared_column
            = blocks.blockAt(frame_end.block).materialized_columns[params.order_by_indices[0]].get();
        if (params.range_offset_comparator(compared_column, frame_end.row,
            reference_column, current_row.row,
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

RowNumber WindowTransform::findPeerGroupEnd(const RowNumber & start, RowNumber & scan_frontier, bool & need_more_data) const
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
        const Int64 run_end = getEqualRangeEndAssumeSorted(
            blocks.blockAt(cur.block).materialized_columns, params.order_by_indices, cur.row, end_bound, 1 /* nan_direction_hint */);

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

bool WindowTransform::advanceGroupBoundary(RowNumber & pointer, Int64 & group_counter, RowNumber & scan_frontier, Int64 target_group) const
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

// Calls func(argument_columns, first_row, past_the_end_row) for each stretch of rows
// [rows_begin, rows_end) that lies within one block.
template <typename F>
void WindowTransform::forEachRowsRangeInBlocks(WindowFunctionWorkspace & ws, RowNumber rows_begin, RowNumber rows_end, F && func)
{
    if (rows_begin == rows_end)
        return;

    // To achieve better performance, we will have to loop over blocks and
    // rows manually, instead of using `SlidingBlocks::next`.
    // For this purpose, the past-the-end block can be different than the
    // block of the past-the-end row (it's usually the next block).
    const auto past_the_end_block = rows_end.row == 0
        ? rows_end.block
        : rows_end.block + 1;

    for (auto block_number = rows_begin.block;
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
        const auto first_row = block_number == rows_begin.block
            ? rows_begin.row : 0;
        const auto past_the_end_row = block_number == rows_end.block
            ? rows_end.row : block.rows_count;

        func(ws.argument_columns.data(), static_cast<size_t>(first_row), static_cast<size_t>(past_the_end_row));
    }
}

// Adds rows [rows_begin, rows_end) to the given aggregate state.
void WindowTransform::addRowsToAggregationState(WindowFunctionWorkspace & ws, AggregateDataPtr state, RowNumber rows_begin, RowNumber rows_end)
{
    const auto * a = ws.aggregate_function.get();
    // Removing arena.get() from the loop makes it faster somehow...
    auto * arena_ptr = arena.get();
    forEachRowsRangeInBlocks(ws, rows_begin, rows_end,
        [&](const IColumn ** columns, size_t first_row, size_t past_the_end_row)
        {
            a->addBatchSinglePlace(first_row, past_the_end_row, state, columns, arena_ptr);
        });
}

// The result saturates at `limit`, so counting large spans just to compare against a
// threshold stays cheap.
UInt64 WindowTransform::countRowsBetween(RowNumber from, RowNumber to, UInt64 limit) const
{
    chassert(from <= to);
    if (from.block == to.block)
        return std::min(static_cast<UInt64>(to.row - from.row), limit);

    UInt64 count = static_cast<UInt64>(blocks.blockAt(from.block).rows_count - from.row);
    for (auto block_number = from.block + 1; block_number < to.block && count < limit; ++block_number)
        count += static_cast<UInt64>(blocks.blockAt(block_number).rows_count);
    return std::min(count + static_cast<UInt64>(to.row), limit);
}

char * WindowTransform::FrameAggregateTree::segmentState(size_t level, UInt64 segment)
{
    auto & tree_level = levels[level];
    chassert(segment >= tree_level.begin_segment);
    // Equality only for the slot being created in createSegment.
    chassert(segment <= tree_level.end_segment);
    chassert(segment >= tree_level.chunk_begin_segment);
    const UInt64 slot = segment - tree_level.chunk_begin_segment;
    chassert(slot / segments_per_chunk < tree_level.chunks.size());
    return tree_level.chunks[slot / segments_per_chunk].data() + (slot % segments_per_chunk) * padded_state_size;
}

char * WindowTransform::FrameAggregateTree::createSegment(size_t level)
{
    auto & tree_level = levels[level];
    const UInt64 segment = tree_level.end_segment;
    if (tree_level.chunks.empty())
        tree_level.chunk_begin_segment = segment;
    if (segment == tree_level.chunk_begin_segment + tree_level.chunks.size() * segments_per_chunk)
        tree_level.chunks.emplace_back(segments_per_chunk * padded_state_size, state_align);
    auto * state = segmentState(level, segment);
    function->create(state);
    tree_level.end_segment = segment + 1;
    return state;
}

void WindowTransform::FrameAggregateTree::destroySegments(size_t level, UInt64 begin, UInt64 end)
{
    if (function->hasTrivialDestructor())
        return;
    for (UInt64 segment = begin; segment < end; ++segment)
        function->destroy(segmentState(level, segment));
}

void WindowTransform::FrameAggregateTree::trailingAdd(size_t level, UInt64 segment, Arena * arena_ptr)
{
    if (!use_accel)
        return;
    if (accel.size() <= level)
        accel.resize(level + 1);
    auto & acc = accel[level];
    if (!acc.trailing.data())
        acc.trailing.reset(padded_state_size, state_align);
    if (!acc.trailing_created || acc.trailing_end != segment)
    {
        // No destroy of the previous contents: use_accel implies a trivial destructor.
        function->create(acc.trailing.data());
        acc.trailing_created = true;
        acc.trailing_begin = segment;
        acc.trailing_end = segment;
    }
    function->merge(acc.trailing.data(), segmentState(level, segment), arena_ptr);
    ++acc.trailing_end;
}

void WindowTransform::FrameAggregateTree::trailingReset(size_t level)
{
    if (use_accel && level < accel.size())
        accel[level].trailing_created = false;
}

void WindowTransform::FrameAggregateTree::tableMerge(size_t level, UInt64 begin, UInt64 end, AggregateDataPtr result, Arena * arena_ptr)
{
    chassert(use_accel);
    if (accel.size() <= level)
        accel.resize(level + 1);
    auto & acc = accel[level];
    const UInt64 group_begin = begin / fanout * fanout;
    chassert(end - group_begin <= fanout);
    const UInt64 slot = begin - group_begin;
    if (acc.table_group_begin != group_begin || acc.table_covered_end != end || slot < acc.table_first_slot)
    {
        if (!acc.table.data())
            acc.table.reset(fanout * padded_state_size, state_align);
        char * slots = acc.table.data();
        const UInt64 last_slot = end - group_begin - 1;
        for (UInt64 k = last_slot + 1; k-- > slot;)
        {
            char * suffix_state = slots + k * padded_state_size;
            function->create(suffix_state);
            function->merge(suffix_state, segmentState(level, group_begin + k), arena_ptr);
            if (k != last_slot)
                function->merge(suffix_state, suffix_state + padded_state_size, arena_ptr);
        }
        acc.table_group_begin = group_begin;
        acc.table_covered_end = end;
        acc.table_first_slot = slot;
    }
    function->merge(result, acc.table.data() + slot * padded_state_size, arena_ptr);
}

// Builds the parent of the just-completed segment group, cascading up the tree.
void WindowTransform::FrameAggregateTree::buildParents(Arena * arena_ptr)
{
    size_t level = 0;
    UInt64 complete_end = frame_end_index / fanout;
    while (complete_end != 0 && complete_end % fanout == 0)
    {
        const UInt64 parent = complete_end / fanout - 1;

        if (levels[level].begin_segment > parent * fanout)
        {
            // Some children were already evicted, so the parent would start before the
            // frame start and can never be queried; skip it. Previously built parents
            // start even earlier, so they are stale too.
            if (level + 1 < levels.size())
            {
                auto & parent_level = levels[level + 1];
                destroySegments(level + 1, parent_level.begin_segment, parent_level.end_segment);
                parent_level = Level(parent + 1);
            }
            return;
        }

        if (level + 1 == levels.size())
            levels.emplace_back(Level(parent));

        auto & parent_level = levels[level + 1];
        if (parent_level.end_segment != parent)
        {
            // A skip below also skipped the completion trigger of parents at this level;
            // everything built here starts before that gap, hence before the frame start.
            chassert(parent_level.end_segment < parent);
            destroySegments(level + 1, parent_level.begin_segment, parent_level.end_segment);
            parent_level = Level(parent);
        }

        auto * parent_state = createSegment(level + 1);
        for (UInt64 child = parent * fanout; child < (parent + 1) * fanout; ++child)
            function->merge(parent_state, segmentState(level, child), arena_ptr);

        trailingReset(level);
        trailingAdd(level + 1, parent, arena_ptr);

        ++level;
        complete_end = parent + 1;
    }
}

void WindowTransform::FrameAggregateTree::activate(const IAggregateFunction & function_)
{
    chassert(!isActive());
    chassert(frame_end_index == 0);
    function = &function_;
    state_align = function_.alignOfData();
    padded_state_size = ::Memory::alignUp(function_.sizeOfData(), state_align);
    // The suffix caches pay off when the query cost is dominated by the number of
    // merge calls, i.e. for cheap fixed-size states. The trivial destructor also makes
    // the cache lifecycle trivial: slots are reused by calling create over the
    // previous contents, and reset just drops the buffers. A trivial destructor is not
    // enough on its own: a state such as `sumForEach` still suballocates from the arena
    // on every merge, and rebuilding the cache slots on successive rows would leave those
    // allocations abandoned until the end of the partition.
    use_accel = function_.hasTrivialDestructor() && !function_.allocatesMemoryInArena() && padded_state_size <= 64;
    levels.emplace_back();
}

void WindowTransform::FrameAggregateTree::append(const IColumn ** columns, size_t first_row, size_t past_the_end_row, Arena * arena_ptr)
{
    size_t row = first_row;
    while (row < past_the_end_row)
    {
        const UInt64 offset_in_segment = frame_end_index % fanout;
        auto * state = offset_in_segment == 0
            ? createSegment(0)
            : segmentState(0, frame_end_index / fanout);
        const size_t rows_to_add = std::min<UInt64>(fanout - offset_in_segment, past_the_end_row - row);
        function->addBatchSinglePlace(row, row + rows_to_add, state, columns, arena_ptr);
        row += rows_to_add;
        frame_end_index += rows_to_add;
        if (frame_end_index % fanout == 0)
        {
            trailingAdd(0, frame_end_index / fanout - 1, arena_ptr);
            buildParents(arena_ptr);
        }
    }
}

void WindowTransform::FrameAggregateTree::evict(UInt64 rows_passed)
{
    frame_start_index += rows_passed;

    UInt64 span = fanout;
    for (size_t level = 0; level < levels.size(); ++level, span *= fanout)
    {
        auto & tree_level = levels[level];
        // A segment starting before the frame start can never be queried again; the
        // trailing (possibly incomplete) level-0 segment is kept regardless.
        UInt64 new_begin = std::min((frame_start_index + span - 1) / span, tree_level.end_segment);
        if (level == 0)
            new_begin = std::min(new_begin, frame_end_index / fanout);
        if (new_begin <= tree_level.begin_segment)
            continue;
        destroySegments(level, tree_level.begin_segment, new_begin);
        tree_level.begin_segment = new_begin;
        while (!tree_level.chunks.empty() && tree_level.chunk_begin_segment + segments_per_chunk <= tree_level.begin_segment)
        {
            tree_level.chunks.pop_front();
            tree_level.chunk_begin_segment += segments_per_chunk;
        }
    }
}

UInt64 WindowTransform::FrameAggregateTree::leadRowCount() const
{
    const UInt64 aligned_start = ::Memory::alignUp(frame_start_index, fanout);
    return std::min(frame_end_index, aligned_start) - frame_start_index;
}

void WindowTransform::FrameAggregateTree::mergeFrame(AggregateDataPtr result, Arena * arena_ptr)
{
    const UInt64 covered_start = frame_start_index + leadRowCount();
    if (covered_start == frame_end_index)
        return;

    // Leading partial groups are merged while ascending the tree, trailing ones are
    // collected and merged in reverse afterwards, keeping everything in frame order.
    UInt64 range_begin = covered_start / fanout;
    UInt64 range_end = (frame_end_index + fanout - 1) / fanout;

    auto merge_segments = [&](size_t level, UInt64 begin, UInt64 end)
    {
        // Stride within each chunk instead of recomputing the address per segment.
        while (begin < end)
        {
            const UInt64 slot = begin - levels[level].chunk_begin_segment;
            const UInt64 run_end = std::min(end, begin + (segments_per_chunk - slot % segments_per_chunk));
            const char * state = segmentState(level, begin);
            for (; begin < run_end; ++begin, state += padded_state_size)
                function->merge(result, state, arena_ptr);
        }
    };

    auto merge_leading = [&](size_t level, UInt64 begin, UInt64 end)
    {
        if (use_accel)
            tableMerge(level, begin, end, result, arena_ptr);
        else
            merge_segments(level, begin, end);
    };

    struct SegmentRange
    {
        size_t level;
        UInt64 begin;
        UInt64 end;
        // When set, the precombined trailing state to merge instead of the raw range.
        const char * state;
    };
    // At most one trailing range per level; levels cannot exceed log_fanout(2^64).
    // Not value-initialized: only [0, trailing_count) is ever read.
    std::array<SegmentRange, 16> trailing; // NOLINT(cppcoreguidelines-pro-type-member-init, hicpp-member-init)
    size_t trailing_count = 0;

    size_t level = 0;
    while (range_begin < range_end)
    {
        chassert(level < levels.size());
        chassert(range_begin >= levels[level].begin_segment);
        chassert(range_end <= levels[level].end_segment);

        const UInt64 begin_aligned = ::Memory::alignUp(range_begin, fanout);
        // Parents past the last built one keep their children represented at this level.
        const UInt64 end_aligned = level + 1 < levels.size()
            ? std::min(range_end / fanout, levels[level + 1].end_segment) * fanout
            : begin_aligned;
        if (begin_aligned >= end_aligned)
        {
            // The suffix table may only cover immutable segments: the still-incomplete
            // trailing level-0 segment mutates per row while the cache key would not.
            const UInt64 complete_end = level == 0 ? std::min(range_end, frame_end_index / fanout) : range_end;
            if (use_accel && range_begin < complete_end && complete_end - range_begin / fanout * fanout <= fanout)
            {
                merge_leading(level, range_begin, complete_end);
                merge_segments(level, complete_end, range_end);
            }
            else
                merge_segments(level, range_begin, range_end);
            break;
        }
        if (range_begin != begin_aligned)
            merge_leading(level, range_begin, begin_aligned);
        if (end_aligned != range_end)
        {
            chassert(trailing_count < trailing.size());
            // Whatever the trailing state does not cover (usually just the incomplete
            // level-0 segment) is merged raw after it.
            const char * trailing_state = nullptr;
            UInt64 raw_begin = end_aligned;
            if (use_accel && level < accel.size())
            {
                const auto & acc = accel[level];
                if (acc.trailing_created && acc.trailing_begin == end_aligned
                    && acc.trailing_end > end_aligned && acc.trailing_end <= range_end)
                {
                    trailing_state = acc.trailing.data();
                    raw_begin = acc.trailing_end;
                }
            }
            trailing[trailing_count] = {level, raw_begin, range_end, trailing_state};
            ++trailing_count;
        }
        range_begin = begin_aligned / fanout;
        range_end = end_aligned / fanout;
        ++level;
    }

    for (size_t i = trailing_count; i > 0; --i)
    {
        if (trailing[i - 1].state != nullptr)
            function->merge(result, trailing[i - 1].state, arena_ptr);
        merge_segments(trailing[i - 1].level, trailing[i - 1].begin, trailing[i - 1].end);
    }
}

void WindowTransform::FrameAggregateTree::reset()
{
    if (!isActive())
        return;

    for (size_t level = 0; level < levels.size(); ++level)
        destroySegments(level, levels[level].begin_segment, levels[level].end_segment);

    function = nullptr;
    frame_start_index = 0;
    frame_end_index = 0;
    levels.clear();
    accel.clear();
}

void WindowTransform::frameTreeActivate(size_t workspace_index)
{
    auto & workspace_tree = workspace_frame_trees[workspace_index];
    workspace_tree.tree.activate(*workspaces[workspace_index].aggregate_function);
    workspace_tree.appended_end = frame_start;
}

// Appends rows [appended_end, frame_end) to the tree. The pending rows are still in
// memory: appended_end is never behind prev_frame_start, which bounds block retention.
void WindowTransform::frameTreeAppend(size_t workspace_index)
{
    auto & workspace_tree = workspace_frame_trees[workspace_index];
    auto * arena_ptr = arena.get();
    forEachRowsRangeInBlocks(workspaces[workspace_index], workspace_tree.appended_end, frame_end,
        [&](const IColumn ** columns, size_t first_row, size_t past_the_end_row)
        {
            workspace_tree.tree.append(columns, first_row, past_the_end_row, arena_ptr);
        });
    workspace_tree.appended_end = frame_end;
}

// Rebuilds ws's aggregation state as the combination of rows [frame_start, frame_end).
void WindowTransform::frameTreeQuery(size_t workspace_index)
{
    auto & tree = workspace_frame_trees[workspace_index].tree;
    auto & ws = workspaces[workspace_index];
    const auto * a = ws.aggregate_function.get();
    auto * buf = ws.aggregate_function_state.data();

    if (!workspace_frame_trees[workspace_index].has_trivial_destructor)
        a->destroy(buf);
    a->create(buf);

    if (const UInt64 lead_rows = tree.leadRowCount())
    {
        const auto lead_end_row = blocks.move(frame_start, static_cast<Int64>(lead_rows));
        chassert(lead_end_row && *lead_end_row <= frame_end);
        addRowsToAggregationState(ws, buf, frame_start, *lead_end_row);
    }
    tree.mergeFrame(buf, arena.get());
}

void WindowTransform::resetFrameTrees()
{
    for (auto & workspace_tree : workspace_frame_trees)
    {
        workspace_tree.tree.reset();
        workspace_tree.appended_end = {};
    }
    frame_trees_active = false;
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

    // The frame boundaries are shared by all workspaces, so the per-row facts are too:
    // all trees activate, evict, and deactivate together.
    const bool frame_start_moved = frame_start != prev_frame_start;
    bool activate_trees = false;
    bool deactivate_trees = false;
    UInt64 evicted_rows = 0;
    if (frame_start_moved && any_workspace_supports_frame_tree)
    {
        const bool frame_is_large = countRowsBetween(frame_start, frame_end, min_frame_rows_for_aggregate_tree)
            >= min_frame_rows_for_aggregate_tree;
        if (frame_is_large == frame_trees_active)
        {
            if (frame_trees_active)
                evicted_rows = countRowsBetween(prev_frame_start, frame_start);
        }
        else if (frame_is_large)
        {
            activate_trees = true;
            frame_trees_active = true;
        }
        else
        {
            deactivate_trees = true;
            frame_trees_active = false;
        }
    }

    for (size_t wi = 0; wi < workspaces.size(); ++wi)
    {
        auto & ws = workspaces[wi];
        if (ws.window_function_impl)
        {
            // No need to do anything for true window functions.
            continue;
        }

        auto * buf = ws.aggregate_function_state.data();

        if (!frame_start_moved)
        {
            // The frame start didn't change, add the tail rows. (An active tree is
            // left lagging; frameTreeAppend catches it up when the start moves again.)
            addRowsToAggregationState(ws, buf, prev_frame_end, frame_end);
            continue;
        }

        if (deactivate_trees && workspace_frame_trees[wi].tree.isActive())
        {
            workspace_frame_trees[wi].tree.reset();
            workspace_frame_trees[wi].appended_end = {};
        }
        else if (activate_trees && workspace_frame_trees[wi].merge_equivalent)
            frameTreeActivate(wi);

        auto & tree = workspace_frame_trees[wi].tree;
        chassert(tree.isActive() == (frame_trees_active && workspace_frame_trees[wi].merge_equivalent));
        if (tree.isActive())
        {
            frameTreeAppend(wi);
            tree.evict(evicted_rows);
            frameTreeQuery(wi);
        }
        else
        {
            const auto * a = ws.aggregate_function.get();
            a->destroy(buf);
            a->create(buf);
            addRowsToAggregationState(ws, buf, frame_start, frame_end);
        }
    }
}

void WindowTransform::writeOutCurrentRow()
{
    chassert(current_row < partition.bounds().end);
    chassert(current_row.block >= blocks.begin().block);

    // Whether this row's frame equals the previous row's. When current_row_number == 1 it's the first
    // row of the partition, so there's no previous row in this partition (and thus no previous frame)
    // to compare against.
    const bool frame_unchanged = current_row_number > 1 && frame_start == prev_frame_start && frame_end == prev_frame_end;

    const auto & block = blocks.blockAt(current_row.block);
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

        if (frame_unchanged && !ws.is_aggregate_function_state && current_row.row > 0)
        {
            // Same frame as the previous row -> same result. When that row is in this same block its
            // result is already in result_column one position back, so copy it instead of
            // re-finalizing. We copy the column into itself with insertRangeFrom (not insertFrom):
            // insertRangeFrom appends via resize + memcpy from a disjoint source range, which is
            // self-safe even if the append reallocates and even for nested columns (Array, Variant,
            // Dynamic, JSON) whose sub-columns are not covered by the top-level reserve.
            chassert(std::cmp_equal(result_column->size(), current_row.row));
            result_column->insertRangeFrom(*result_column, current_row.row - 1, 1);
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
        while (current_row < partition_end)
        {
            // We now know that the current row is valid, so we can update the
            // peer group start.
            if (!arePeers(peer_group_start, current_row))
            {
                peer_group_start = current_row;
                peer_group_start_row_number = current_row_number;
                ++peer_group_number;
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
            // because current_row might now be past-the-end.
            current_row = blocks.next(current_row);
            ++current_row_number;
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
    chassert(current_row == partition_start);
    current_row_number = 1;
    peer_group_start = partition_start;
    peer_group_start_row_number = 1;
    peer_group_number = 1;
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

    resetFrameTrees();

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

    chassert(current_row.block >= blocks.begin().block);
    // The current_row might be past-the-end if we have already calculated the
    // window functions for all input rows. That's why the equality is also
    // valid here.
    chassert(current_row.block <= blocks.end().block);

    // Output the ready data prepared by work(). A block is ready when the
    // current row has left it, because rows are computed in order.
    // We inspect the calculation state and create the output chunk right here,
    // because this is pretty lightweight.
    if (next_output_block_number < current_row.block)
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
        chassert(current_row == blocks.end());

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
    // We also keep the start of the current peer group: it can lag behind the
    // current row (its group may have started in an earlier block), and it is
    // dereferenced by arePeers() on the next row. A FOLLOWING frame pushes the
    // frame pointers ahead of the current row, so peer_group_start can be the
    // trailing pointer.
    chassert(prev_frame_start <= frame_start);
    const auto first_used_block = std::min({next_output_block_number, prev_frame_start.block, current_row.block, peer_group_start.block});
    while (blocks.begin().block < first_used_block)
        blocks.pop();

    chassert(frame_start.block >= blocks.begin().block);
    chassert(prev_frame_start.block >= blocks.begin().block);
    chassert(current_row.block >= blocks.begin().block);
    chassert(peer_group_start.block >= blocks.begin().block);
}

}
