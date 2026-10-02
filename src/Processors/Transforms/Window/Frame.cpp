#include <Processors/Transforms/Window/Frame.h>

#include <base/defines.h>

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

/// -1, 0 or 1 for the ORDER BY key of the row against the key of the current row moved by the offset.
int compareWithOffset(const WindowTransformParams & params, const SlidingBlocks & blocks, RowNumber row, RowNumber current, const Field & offset, bool frame_preceding)
{
    const int direction = params.window_description.order_by[0].direction;
    const bool offset_is_preceding = frame_preceding == (direction > 0);
    const size_t key_position = params.order_by_indices[0];
    const IColumn * row_key_column = blocks.blockAt(row.block).materialized_columns[key_position].get();
    const IColumn * current_key_column = blocks.blockAt(current.block).materialized_columns[key_position].get();
    return params.range_offset_comparator(row_key_column, row.row, current_key_column, current.row, offset, offset_is_preceding) * direction;
}

/// Whether the row lies before the frame of the current row, so the frame start must step over it.
bool isBeforeFrame(const WindowTransformParams & params, const SlidingBlocks & blocks, const RowPoint & row, const RowPoint & current)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return false;
        case WindowFrame::BoundaryType::Current:
            return row.peer_group_index_in_partition < current.peer_group_index_in_partition;
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return row.row_index_in_partition < current.row_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding);
                case WindowFrame::FrameType::RANGE:
                    return compareWithOffset(params, blocks, row.location, current.location, frame.begin_offset, frame.begin_preceding) < 0;
                case WindowFrame::FrameType::GROUPS:
                    return row.peer_group_index_in_partition < current.peer_group_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding);
            }
    }
    UNREACHABLE();
}

/// Whether the row lies inside the frame of the current row, so the frame end must step over it.
bool isInsideFrame(const WindowTransformParams & params, const SlidingBlocks & blocks, const RowPoint & row, const RowPoint & current)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.end_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return true;
        case WindowFrame::BoundaryType::Current:
            return row.peer_group_index_in_partition <= current.peer_group_index_in_partition;
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return row.row_index_in_partition <= current.row_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding);
                case WindowFrame::FrameType::RANGE:
                    return compareWithOffset(params, blocks, row.location, current.location, frame.end_offset, frame.end_preceding) <= 0;
                case WindowFrame::FrameType::GROUPS:
                    return row.peer_group_index_in_partition <= current.peer_group_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding);
            }
    }
    UNREACHABLE();
}

}

Frame::Frame(const WindowTransformParams & params_)
    : params(params_)
{
}

void Frame::enterPartition(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition)
{
    frame_bounds = FrameBounds{.start = RowPoint{.location = partition.start}, .end = RowPoint{.location = partition.start}, .fully_visible = false};
    advance(blocks, current, partition);
}

void Frame::advance(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition)
{
    const auto advance_cursor = [&](RowPoint & cursor, auto && should_step)
    {
        while (cursor.location < partition.end && should_step(cursor))
        {
            const RowNumber next = blocks.next(cursor.location);
            if (next == partition.end && !partition.fully_visible)
                return false;

            cursor.row_index_in_partition += 1;
            if (next < partition.end && blocks.blockAt(next.block).index.peer_group_starts[next.row])
                cursor.peer_group_index_in_partition += 1;

            cursor.location = next;
        }

        return true;
    };

    frame_bounds.fully_visible = false;
    const bool start_found = advance_cursor(frame_bounds.start, [&](const RowPoint & row) { return isBeforeFrame(params, blocks, row, current); });
    if (!start_found)
        return;

    if (frame_bounds.end.location < frame_bounds.start.location)
        frame_bounds.end = frame_bounds.start;

    const bool end_found = advance_cursor(frame_bounds.end, [&](const RowPoint & row) { return isInsideFrame(params, blocks, row, current); });
    frame_bounds.fully_visible = end_found;
}

const FrameBounds & Frame::bounds() const
{
    return frame_bounds;
}

}
