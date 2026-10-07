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

bool advanceToPartitionEnd(const SlidingBlocks & blocks, const Partition & partition, RowPoint & bound)
{
    if (!partition.bounds().fully_visible)
        return false;

    if (bound.location != partition.bounds().end)
        bound = partition.pointAt(blocks, partition.bounds().end);

    return true;
}

bool advanceToRow(const SlidingBlocks & blocks, const Partition & partition, int64_t target_row_index, RowPoint & bound)
{
    if (target_row_index >= partition.size())
        return advanceToPartitionEnd(blocks, partition, bound);

    const int64_t row_index = std::max<int64_t>(target_row_index, 0);
    chassert(bound.row_index_in_partition <= row_index);
    bound.location = blocks.move(bound.location, row_index - bound.row_index_in_partition);
    bound.row_index_in_partition = row_index;
    bound.peer_group_index_in_partition = row_index;
    return true;
}

template <typename Reached>
NO_INLINE bool advanceUntil(const SlidingBlocks & blocks, const Partition & partition, Reached && reached, RowPoint & bound)
{
    const int64_t rows_ahead = partition.size() - bound.row_index_in_partition;
    if (rows_ahead == 0)
        return partition.bounds().fully_visible;

    if (reached(bound.location))
        return true;

    const auto reached_at = [&](int64_t distance) { return reached(blocks.move(bound.location, distance)); };
    int64_t last_miss = 0;
    int64_t first_hit = rows_ahead;
    for (int64_t probe = 1; probe < rows_ahead; probe *= 2)
    {
        if (reached_at(probe))
        {
            first_hit = probe;
            break;
        }
        last_miss = probe;
    }

    while (first_hit - last_miss > 1)
    {
        const int64_t middle = std::midpoint(last_miss, first_hit);
        if (reached_at(middle))
            first_hit = middle;
        else
            last_miss = middle;
    }

    if (first_hit == rows_ahead)
        return advanceToPartitionEnd(blocks, partition, bound);

    bound = partition.pointAt(blocks, blocks.move(bound.location, first_hit));
    return true;
}

bool advanceToPeerGroup(const SlidingBlocks & blocks, const Partition & partition, int64_t group, RowPoint & bound)
{
    if (bound.peer_group_index_in_partition >= group)
        return true;

    return advanceUntil(blocks, partition, [&](RowNumber row) { return partition.peerGroupIndex(blocks, row) >= group; }, bound);
}

bool advanceFrameStart(const WindowTransformParams & params, const SlidingBlocks & blocks, const Partition & partition, const RowPoint & current, RowPoint & start)
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
                    return advanceToPeerGroup(blocks, partition, current.peer_group_index_in_partition, start);
            }
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return advanceToRow(blocks, partition, current.row_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding), start);
                case WindowFrame::FrameType::RANGE:
                    return advanceUntil(blocks, partition, [&](RowNumber row) { return compareWithOffset(params, blocks, row, current.location, frame.begin_offset, frame.begin_preceding) >= 0; }, start);
                case WindowFrame::FrameType::GROUPS:
                    return advanceToPeerGroup(blocks, partition, current.peer_group_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding), start);
            }
    }
    UNREACHABLE();
}

bool advanceFrameEnd(const WindowTransformParams & params, const SlidingBlocks & blocks, const Partition & partition, const RowPoint & current, RowPoint & end)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.end_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return advanceToPartitionEnd(blocks, partition, end);
        case WindowFrame::BoundaryType::Current:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return advanceToRow(blocks, partition, current.row_index_in_partition + 1, end);
                case WindowFrame::FrameType::RANGE:
                case WindowFrame::FrameType::GROUPS:
                    return advanceToPeerGroup(blocks, partition, current.peer_group_index_in_partition + 1, end);
            }
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return advanceToRow(blocks, partition, current.row_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding) + 1, end);
                case WindowFrame::FrameType::RANGE:
                    return advanceUntil(blocks, partition, [&](RowNumber row) { return compareWithOffset(params, blocks, row, current.location, frame.end_offset, frame.end_preceding) > 0; }, end);
                case WindowFrame::FrameType::GROUPS:
                    return advanceToPeerGroup(blocks, partition, current.peer_group_index_in_partition + signedOffset(frame.end_offset, frame.end_preceding) + 1, end);
            }
    }
    UNREACHABLE();
}

}

Frame::Frame(const WindowTransformParams & params_)
    : params(params_)
{
}

void Frame::enterPartition(const RowPoint & current)
{
    frame_bounds = FrameBounds{.start = current, .end = current, .fully_visible = false};
}

void Frame::advance(const SlidingBlocks & blocks, const RowPoint & current, const Partition & partition)
{
    frame_bounds.fully_visible = false;

    if (!advanceFrameStart(params, blocks, partition, current, frame_bounds.start))
        return;

    if (frame_bounds.end.row_index_in_partition < frame_bounds.start.row_index_in_partition)
        frame_bounds.end = frame_bounds.start;

    frame_bounds.fully_visible = advanceFrameEnd(params, blocks, partition, current, frame_bounds.end);
}

const FrameBounds & Frame::bounds() const
{
    return frame_bounds;
}

}
