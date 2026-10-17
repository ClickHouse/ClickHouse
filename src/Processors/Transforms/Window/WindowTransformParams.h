#pragma once

#include <Interpreters/WindowDescription.h>

#include <Core/Block.h>
#include <Core/SortCursor.h>

#include <base/defines.h>

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
    /// Collation-equal values may differ in bytes, so such keys cannot be compared raw.
    const bool has_order_by_collation;

public:
    static WindowTransformParams create(
        const Block & input_header,
        const WindowDescription & window_description,
        const std::vector<WindowFunctionDescription> & functions);

    /// Compares the ORDER BY keys the way the input was sorted, i.e. with the collation if there is one.
    ALWAYS_INLINE bool haveEqualOrderByValues(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row) const
    {
        if (unlikely(has_order_by_collation))
            return haveEqualCollatedOrderByValues(lhs, lhs_row, rhs, rhs_row);

        for (const size_t key : order_by_indices)
            if (lhs[key]->compareAt(lhs_row, rhs_row, *rhs[key], /*nan_direction_hint=*/1) != 0)
                return false;

        return true;
    }

    /// End of the run of rows in [begin, end) whose ORDER BY values equal those at `begin`.
    ALWAYS_INLINE size_t getOrderByEqualRangeEnd(const Columns & columns, size_t begin, size_t end) const
    {
        if (unlikely(has_order_by_collation))
            return getCollatedOrderByEqualRangeEnd(columns, begin, end);

        return getEqualRangeEndAssumeSorted(columns, order_by_indices, begin, end, /*nan_direction_hint=*/1);
    }

private:
    bool haveEqualCollatedOrderByValues(const Columns & lhs, size_t lhs_row, const Columns & rhs, size_t rhs_row) const;
    size_t getCollatedOrderByEqualRangeEnd(const Columns & columns, size_t begin, size_t end) const;
};

}
