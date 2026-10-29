#pragma once

#include <Interpreters/WindowDescription.h>

#include <Core/Block.h>

#include <vector>

namespace DB
{

/// -1, 0 or 1 for the ORDER BY key of the lhs row against the key of the rhs row shifted by the offset.
using RangeOffsetComparator = int (*)(
    const IColumn * lhs_column, size_t lhs_row,
    const IColumn * rhs_column, size_t rhs_row,
    const Field & offset,
    bool offset_is_preceding);

struct WindowTransformParams
{
    const Block input_header;
    const WindowDescription window_description;
    const std::vector<size_t> partition_by_indices;
    const std::vector<size_t> order_by_indices;
    const std::vector<bool> should_materialize;
    const RangeOffsetComparator range_offset_comparator;
    /// Under ROWS every row is its own peer group, so the ORDER BY peer groups are tracked separately
    /// when a function requires peers.
    const bool separate_order_by_peer_groups;

public:
    static WindowTransformParams create(
        const Block & input_header,
        const WindowDescription & window_description,
        const std::vector<WindowFunctionDescription> & functions);
};

}
