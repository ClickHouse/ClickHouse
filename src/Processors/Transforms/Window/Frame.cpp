#include <Processors/Transforms/Window/Frame.h>

#include <Columns/findEqualRangeEndAssumeSorted.h>
#include <base/defines.h>

#include <algorithm>

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

bool advanceToPartitionEnd(const PartitionBounds & partition, RowPoint & bound)
{
    if (!partition.fully_visible)
        return false;

    bound = partition.end;
    return true;
}

/// The bound of a ROWS frame at a row index, cut to the partition; under ROWS every row is its own peer group.
bool advanceToRow(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t target_row_index, RowPoint & bound)
{
    if (target_row_index >= partition.end.row_index_in_partition)
        return advanceToPartitionEnd(partition, bound);

    const int64_t row_index = std::max<int64_t>(target_row_index, 0);
    chassert(bound.row_index_in_partition <= row_index);
    bound.location = blocks.move(bound.location, row_index - bound.row_index_in_partition);
    bound.row_index_in_partition = row_index;
    bound.peer_group_index_in_partition = row_index;
    return true;
}

/// The first row of the peer group, found block by block in the stored peer group indexes; the partition end when the group is past the last one.
bool advanceToPeerGroup(const SlidingBlocks & blocks, const PartitionBounds & partition, int64_t group, RowPoint & bound)
{
    if (bound.peer_group_index_in_partition >= group)
        return true;

    const RowNumber partition_end = partition.end.location;
    if (partition.fully_visible && group >= partition.end.peer_group_index_in_partition)
        return advanceToPartitionEnd(partition, bound);

    while (bound.location != partition_end)
    {
        const SlidingBlock & block = blocks.blockAt(bound.location.block);
        const std::vector<int64_t> & groups = block.index.peer_group_indexes;
        const int64_t rows_end = partition_end.block == bound.location.block ? partition_end.row : block.rows_count;
        const int64_t row = groups[bound.location.row] >= group
            ? bound.location.row
            : static_cast<int64_t>(findEqualRangeEndAssumeSorted(bound.location.row, rows_end, 16, [&](size_t i) { return groups[i] < group; }));

        bound.row_index_in_partition += row - bound.location.row;
        if (row < rows_end)
        {
            bound.location.row = row;
            bound.peer_group_index_in_partition = groups[row];
            return true;
        }
        bound.location = blocks.move(bound.location, row - bound.location.row);
    }
    return advanceToPartitionEnd(partition, bound);
}

/// The first row whose ORDER BY key is at the offset from the current row's key, or past it when `past`.
bool advanceToRangeBound(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, const Field & offset, bool frame_preceding, bool past, RowPoint & bound)
{
    const int direction = params.window_description.order_by[0].direction;
    const bool offset_is_preceding = frame_preceding == (direction > 0);
    const size_t key_position = params.order_by_indices[0];
    const IColumn * current_key_column = blocks.blockAt(current.location.block).materialized_columns[key_position].get();
    const RowNumber partition_end = partition.end.location;
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
            bound.peer_group_index_in_partition = block.index.peer_group_indexes[row];
            return true;
        }
        bound.location = blocks.move(bound.location, row - bound.location.row);
    }

    return advanceToPartitionEnd(partition, bound);
}

bool advanceFrameStart(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, RowPoint & start)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.begin_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return true;
        case WindowFrame::BoundaryType::Current:
            if (start.peer_group_index_in_partition != current.peer_group_index_in_partition)
                start = current;
            return true;
        case WindowFrame::BoundaryType::Offset:
            switch (frame.type)
            {
                case WindowFrame::FrameType::ROWS:
                    return advanceToRow(blocks, partition, current.row_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding), start);
                case WindowFrame::FrameType::RANGE:
                    return advanceToRangeBound(params, blocks, partition, current, frame.begin_offset, frame.begin_preceding, false, start);
                case WindowFrame::FrameType::GROUPS:
                    return advanceToPeerGroup(blocks, partition, current.peer_group_index_in_partition + signedOffset(frame.begin_offset, frame.begin_preceding), start);
            }
    }
    UNREACHABLE();
}

bool advanceFrameEnd(const WindowTransformParams & params, const SlidingBlocks & blocks, const PartitionBounds & partition, const RowPoint & current, RowPoint & end)
{
    const WindowFrame & frame = params.window_description.frame;
    switch (frame.end_type)
    {
        case WindowFrame::BoundaryType::Unbounded:
            return advanceToPartitionEnd(partition, end);
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
                    return advanceToRangeBound(params, blocks, partition, current, frame.end_offset, frame.end_preceding, true, end);
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

void Frame::enterPartition(RowNumber partition_start)
{
    frame_bounds = FrameBounds{.start = {.location = partition_start}, .end = {.location = partition_start}, .fully_visible = false};
}

void Frame::advance(const SlidingBlocks & blocks, const RowPoint & current, const PartitionBounds & partition)
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
