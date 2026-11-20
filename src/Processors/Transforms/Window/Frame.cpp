#include <Processors/Transforms/Window/Frame.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/defines.h>

#include <algorithm>
#include <limits>

namespace DB
{

namespace
{

int64_t signedOffset(const Field & offset, bool preceding)
{
    const auto value = static_cast<int64_t>(offset.safeGet<UInt64>());
    if (preceding)
        return -value;

    return value;
}

int64_t peerGroupAt(const SlidingBlocks & blocks, RowNumber row)
{
    return blocks.blockAt(row.block).index.peer_group_indexes[row.row];
}

bool advanceToPartitionEnd(const PartitionBounds & partition, RowPoint & bound)
{
    if (!partition.end_visible)
        return false;

    bound.location = partition.end;
    bound.row_index_in_partition = partition.rows_count;
    return true;
}

/// The bound of a ROWS frame at a row index, cut to the partition.
bool advanceToRow(const PartitionBounds & partition, int64_t target_row_index, RowPoint & bound)
{
    if (target_row_index >= partition.rows_count)
        return advanceToPartitionEnd(partition, bound);

    const int64_t row_index = std::max<int64_t>(target_row_index, 0);
    chassert(bound.row_index_in_partition <= row_index);
    bound.row_index_in_partition = row_index;
    return true;
}

/// The first row of the peer group, found block by block in the stored peer group indexes; the partition end when the group is past the last one.
bool advanceToPeerGroup(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t group, RowPoint & bound)
{
    const RowNumber partition_end = partition.end;
    if (bound.location == partition_end)
        return partition.end_visible;

    if (peerGroupAt(blocks, bound.location) >= group)
        return true;

    while (bound.location != partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(bound.location.block);
        const VectorWithMemoryTracking<int64_t> & groups = block.index.peer_group_indexes;
        const int64_t rows_end = partition_end.block == bound.location.block ? partition_end.row : block.rows_count;
        const int64_t row = groups[bound.location.row] >= group
            ? bound.location.row
            : static_cast<int64_t>(findEqualRangeEndAssumeSorted(bound.location.row, rows_end, 16, [&](size_t i) { return groups[i] < group; }));

        bound.row_index_in_partition += row - bound.location.row;
        if (row < rows_end)
        {
            bound.location.row = row;
            return true;
        }
        bound.location = blocks.move(bound.location, row - bound.location.row);
    }

    return partition.end_visible;
}

/// The first row whose ORDER BY key is at the offset from the current row's key, or past it when `past`.
bool advanceToRangeBound(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, const Field & offset, bool frame_preceding, bool past, RowPoint & bound)
{
    const int direction = params.window_description.order_by[0].direction;
    const bool offset_is_preceding = frame_preceding == (direction > 0);
    const size_t key_position = params.order_by_indices[0];
    const IColumn * current_key_column = blocks.blockAt(current.location.block).materialized_columns[key_position].get();
    const RowNumber partition_end = partition.end;
    while (bound.location != partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(bound.location.block);
        const IColumn * key_column = block.materialized_columns[key_position].get();
        const int64_t rows_end = partition_end.block == bound.location.block ? partition_end.row : block.rows_count;
        int64_t row = bound.location.row;
        while (row < rows_end && params.range_offset_comparator(key_column, row, current_key_column, current.location.row, offset, offset_is_preceding) * direction < past)
            ++row;

        bound.row_index_in_partition += row - bound.location.row;
        if (row < rows_end)
        {
            bound.location.row = row;
            return true;
        }
        bound.location = blocks.move(bound.location, row - bound.location.row);
    }

    return partition.end_visible;
}

/// The start of a RANGE or GROUPS frame; `offset` is the signed GROUPS offset.
bool advanceFrameStart(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, int64_t offset, RowPoint & start)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return true;
        case WindowFrame::BoundaryType::Current:
            if (peerGroupAt(blocks, start.location) != peerGroupAt(blocks, current.location))
                start = current;
            return true;
        case WindowFrame::BoundaryType::Offset:
            if (frame.type == WindowFrame::FrameType::RANGE)
                return advanceToRangeBound(params, blocks, partition, current, frame.begin_offset, frame.begin_preceding, false, start);
            return advanceToPeerGroup(blocks, partition, peerGroupAt(blocks, current.location) + offset, start);
    }
    UNREACHABLE();
}

/// The end of a RANGE or GROUPS frame; `offset` is the signed GROUPS offset plus one.
bool advanceFrameEnd(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, int64_t offset, RowPoint & end)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.end_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return advanceToPartitionEnd(partition, end);
        case WindowFrame::BoundaryType::Current:
            return advanceToPeerGroup(blocks, partition, peerGroupAt(blocks, current.location) + 1, end);
        case WindowFrame::BoundaryType::Offset:
            if (frame.type == WindowFrame::FrameType::RANGE)
                return advanceToRangeBound(params, blocks, partition, current, frame.end_offset, frame.end_preceding, true, end);
            return advanceToPeerGroup(blocks, partition, peerGroupAt(blocks, current.location) + offset, end);
    }
    UNREACHABLE();
}

}

template <bool start_unbounded, bool end_unbounded>
void Frame::advanceRows(Frame & frame, const SlidingBlocks & /*blocks*/, const RowPoint & current, const PartitionBounds & partition)
{
    FrameBounds & bounds = frame.frame_bounds;
    bounds.start_visible = start_unbounded || advanceToRow(partition, current.row_index_in_partition + frame.start_offset, bounds.start);
    bounds.end_visible = false;
    if (!bounds.start_visible)
        return;

    /// An unbounded start stays at the first row of the partition, which no end precedes.
    if (!start_unbounded && bounds.end.row_index_in_partition < bounds.start.row_index_in_partition)
        bounds.end = bounds.start;

    if constexpr (end_unbounded)
        bounds.end_visible = advanceToPartitionEnd(partition, bounds.end);
    else
        bounds.end_visible = advanceToRow(partition, current.row_index_in_partition + frame.end_offset, bounds.end);
}

void Frame::advanceRangeOrGroups(Frame & frame, const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition)
{
    FrameBounds & bounds = frame.frame_bounds;
    bounds.start_visible = advanceFrameStart(frame.params, blocks, partition, current, frame.start_offset, bounds.start);
    bounds.end_visible = false;
    if (!bounds.start_visible)
        return;

    if (bounds.end.row_index_in_partition < bounds.start.row_index_in_partition)
        bounds.end = bounds.start;

    bounds.end_visible = advanceFrameEnd(frame.params, blocks, partition, current, frame.end_offset, bounds.end);
}

Frame::Frame(const WindowTransformParams & params_)
    : params(params_)
{
    const WindowFrame & window_frame = params.window_description.frame;
    if (window_frame.type != WindowFrame::FrameType::RANGE)
    {
        if (window_frame.begin_type == WindowFrame::BoundaryType::Offset)
            start_offset = signedOffset(window_frame.begin_offset, window_frame.begin_preceding);
        if (window_frame.end_type == WindowFrame::BoundaryType::Offset)
            end_offset += signedOffset(window_frame.end_offset, window_frame.end_preceding);
    }

    const bool start_unbounded = window_frame.begin_type == WindowFrame::BoundaryType::Unbounded;
    const bool end_unbounded = window_frame.end_type == WindowFrame::BoundaryType::Unbounded;
    if (window_frame.type != WindowFrame::FrameType::ROWS)
        advance_bounds = &advanceRangeOrGroups;
    else if (start_unbounded)
        advance_bounds = end_unbounded ? &advanceRows<true, true> : &advanceRows<true, false>;
    else
        advance_bounds = end_unbounded ? &advanceRows<false, true> : &advanceRows<false, false>;
}

void Frame::enterPartition(RowNumber partition_start)
{
    frame_bounds.start = RowPoint{.location = partition_start};
    frame_bounds.end = RowPoint{.location = partition_start};
    frame_bounds.start_visible = false;
    frame_bounds.end_visible = false;
}

const FrameBounds & Frame::bounds() const
{
    return frame_bounds;
}

Int64 Frame::firstRowToRead() const
{
    const WindowFrame & window_frame = params.window_description.frame;

    if (window_frame.begin_type != WindowFrame::BoundaryType::Unbounded)
        return frame_bounds.start.row_index_in_partition;

    if (window_frame.end_type != WindowFrame::BoundaryType::Unbounded)
        return frame_bounds.end.row_index_in_partition;

    return std::numeric_limits<Int64>::max();
}

}
