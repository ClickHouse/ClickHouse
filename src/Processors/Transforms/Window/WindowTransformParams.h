#pragma once

#include <WindowFunctions/Common/WindowFunctionRequirements.h>

#include <Interpreters/WindowDescription.h>

#include <Common/VectorWithMemoryTracking.h>

#include <Core/Block.h>

class DateLUTImpl;

namespace DB
{

/// -1, 0 or 1 for the ORDER BY key of the lhs row against the key of the rhs row shifted by the offset.
/// `time_zone` is used only by the comparators of calendar INTERVAL offsets.
using RangeOffsetComparator = int (*)(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset,
    bool offset_is_preceding,
    const DateLUTImpl * time_zone);

struct WindowTransformParams
{
    const Block input_header;
    const WindowDescription window_description;
    const VectorWithMemoryTracking<size_t> partition_by_indices;
    const VectorWithMemoryTracking<size_t> order_by_indices;
    const VectorWithMemoryTracking<bool> should_materialize;
    /// The start and the end of the frame differ when only one of them is a calendar INTERVAL.
    const RangeOffsetComparator range_begin_offset_comparator;
    const RangeOffsetComparator range_end_offset_comparator;
    /// Null unless a frame bound is a calendar INTERVAL.
    const DateLUTImpl * const time_zone;
    const WindowFunctionRequirements requirements;
    const bool needs_peer_group_indexes;

public:
    static WindowTransformParams create(
        const Block & input_header,
        const WindowDescription & window_description,
        const VectorWithMemoryTracking<WindowFunctionDescription> & functions);
};

}
