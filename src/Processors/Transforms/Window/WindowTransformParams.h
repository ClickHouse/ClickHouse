#pragma once

#include <Interpreters/WindowDescription.h>
#include <Common/VectorWithMemoryTracking.h>

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
    const VectorWithMemoryTracking<size_t> partition_by_indices;
    const VectorWithMemoryTracking<size_t> order_by_indices;
    const VectorWithMemoryTracking<bool> should_materialize;
    const RangeOffsetComparator range_offset_comparator;

public:
    static WindowTransformParams create(
        const Block & input_header,
        const WindowDescription & window_description,
        const VectorWithMemoryTracking<WindowFunctionDescription> & functions);

    bool arePeers(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row) const;
};

}
