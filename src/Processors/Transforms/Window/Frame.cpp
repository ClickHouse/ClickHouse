#include <Processors/Transforms/Window/Frame.h>

#include <base/defines.h>

#include <algorithm>
#include <numeric>

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

bool findPartitionEnd(const SlidingBlocks & blocks, const Partition & partition, RowPoint & bound)
{
    if (!partition.bounds().fully_visible)
        return false;

    if (bound.location != partition.bounds().end)
        bound = partition.pointAt(blocks, partition.bounds().end);

    return true;
}

/// The bound of a ROWS frame at a row index, cut to the partition; under ROWS every row is its own peer group.
bool findRowsBound(const SlidingBlocks & blocks, const Partition & partition, int64_t row_index, RowPoint & bound)
{
    if (row_index >= partition.size())
        return findPartitionEnd(blocks, partition, bound);

    const int64_t index = std::max<int64_t>(row_index, 0);
    chassert(bound.row_index_in_partition <= index);
    bound.location = blocks.move(bound.location, index - bound.row_index_in_partition);
    bound.row_index_in_partition = index;
    bound.peer_group_index_in_partition = index;
    return true;
}

/// The first row from `from` on where `is_bound` holds and stays true to the partition end
template <typename IsBound>
NO_INLINE bool findBound(const SlidingBlocks & blocks, const Partition & partition, const RowPoint & from, IsBound && is_bound, RowPoint & bound)
{
    const int64_t rows_left = partition.size() - from.row_index_in_partition;
    if (rows_left == 0)
    {
        bound = from;
        return partition.bounds().fully_visible;
    }

    const auto is_bound_at = [&](int64_t distance) { return is_bound(blocks.move(from.location, distance)); };
    int64_t missed = -1;
    int64_t found = rows_left;
    for (int64_t probe = 0; probe < rows_left; probe = std::max<int64_t>(1, 2 * probe))
    {
        if (is_bound_at(probe))
        {
            found = probe;
            break;
        }
        missed = probe;
    }

    while (found - missed > 1)
    {
        const int64_t middle = std::midpoint(missed, found);
        if (is_bound_at(middle))
            found = middle;
        else
            missed = middle;
    }

    if (found == rows_left)
        return findPartitionEnd(blocks, partition, bound);

    bound = partition.pointAt(blocks, blocks.move(from.location, found));
    return true;
}

bool findFrameStart(const WindowTransformParams & params, const SlidingBlocks & blocks, const Partition & partition, const RowPoint & current, RowPoint & start)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return true;
        case WindowFrame::BoundaryType::Current:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    start = current;
                    return true;
                case WindowFrame::FrameType::RANGE:
                case WindowFrame::FrameType::GROUPS:
                    return findBound(blocks, partition, start, [&, group = current.peer_group_index_in_partition](RowNumber row) { return partition.peerGroupIndex(blocks, row) >= group; }, start);
            }
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return findRowsBound(blocks, partition, current.row_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding), start);
                case WindowFrame::FrameType::RANGE:
                    return findBound(blocks, partition, start, [&](RowNumber row) { return compareWithOffset(params, blocks, row, current.location, frame.begin_offset, frame.begin_preceding) >= 0; }, start);
                case WindowFrame::FrameType::GROUPS:
                    return findBound(blocks, partition, start, [&, group = current.peer_group_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding)](RowNumber row) { return partition.peerGroupIndex(blocks, row) >= group; }, start);
            }
    }
    UNREACHABLE();
}

bool findFrameEnd(const WindowTransformParams & params, const SlidingBlocks & blocks, const Partition & partition, const RowPoint & current, RowPoint & end)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.end_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return findPartitionEnd(blocks, partition, end);
        case WindowFrame::BoundaryType::Current:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return findRowsBound(blocks, partition, current.row_index_in_partition + 1, end);
                case WindowFrame::FrameType::RANGE:
                case WindowFrame::FrameType::GROUPS:
                    return findBound(blocks, partition, end, [&, group = current.peer_group_index_in_partition + 1](RowNumber row) { return partition.peerGroupIndex(blocks, row) >= group; }, end);
            }
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return findRowsBound(blocks, partition, current.row_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding) + 1, end);
                case WindowFrame::FrameType::RANGE:
                    return findBound(blocks, partition, end, [&](RowNumber row) { return compareWithOffset(params, blocks, row, current.location, frame.end_offset, frame.end_preceding) > 0; }, end);
                case WindowFrame::FrameType::GROUPS:
                    return findBound(blocks, partition, end, [&, group = current.peer_group_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding) + 1](RowNumber row) { return partition.peerGroupIndex(blocks, row) >= group; }, end);
            }
    }
    UNREACHABLE();
}

}

Frame::Frame(const WindowTransformParams & params_)
    : params(params_)
{
}

void Frame::enterPartition(const SlidingBlocks & blocks, const RowPoint & current, const Partition & partition)
{
    frame_bounds = FrameBounds{.start = current, .end = current, .fully_visible = false};
    advance(blocks, current, partition);
}

void Frame::advance(const SlidingBlocks & blocks, const RowPoint & current, const Partition & partition)
{
    frame_bounds.fully_visible = false;

    if (!findFrameStart(params, blocks, partition, current, frame_bounds.start))
        return;

    if (frame_bounds.end.row_index_in_partition < frame_bounds.start.row_index_in_partition)
        frame_bounds.end = frame_bounds.start;

    frame_bounds.fully_visible = findFrameEnd(params, blocks, partition, current, frame_bounds.end);
}

const FrameBounds & Frame::bounds() const
{
    return frame_bounds;
}

}
