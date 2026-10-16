#pragma once

#include <Interpreters/WindowDescription.h>

#include <Core/Block.h>

#include <vector>

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
    const std::vector<size_t> partition_by_indices;
    const std::vector<size_t> order_by_indices;
    const std::vector<bool> should_materialize;
    /// The start and the end of the frame differ when only one of them is a calendar INTERVAL.
    const RangeOffsetComparator range_begin_offset_comparator;
    const RangeOffsetComparator range_end_offset_comparator;
    /// Null unless a frame bound is a calendar INTERVAL.
    const DateLUTImpl * const time_zone;

public:
    static WindowTransformParams create(
        const Block & input_header,
        const WindowDescription & window_description,
        const std::vector<WindowFunctionDescription> & functions);

    bool arePeers(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row) const;
};

}
