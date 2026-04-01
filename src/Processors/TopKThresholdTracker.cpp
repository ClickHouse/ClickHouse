#include <Processors/TopKThresholdTracker.h>
#include <Columns/Collator.h>

#include <cmath>

namespace DB
{

namespace
{

/// ClickHouse columns treat NaN the same as NULL for ordering purposes,
/// using nan_direction_hint (which maps to nulls_direction from the query).
/// Field::operator< does NOT handle this — NaN compares as "equal" to
/// everything — so we must detect NaN explicitly and apply the same
/// semantics as IColumn::compareAt to keep the shared threshold consistent
/// with column-level pruning in TopNDirectAggregatingTransform and
/// FunctionTopKFilter.
bool fieldIsNaN(const Field & f)
{
    if (f.getType() == Field::Types::Float64)
        return std::isnan(f.safeGet<Float64>());
    return false;
}

}

int TopKThresholdTracker::compareFields(const Field & lhs, const Field & rhs) const
{
    bool lhs_null = lhs.isNull();
    bool rhs_null = rhs.isNull();

    bool lhs_nan = !lhs_null && fieldIsNaN(lhs);
    bool rhs_nan = !rhs_null && fieldIsNaN(rhs);

    bool lhs_special = lhs_null || lhs_nan;
    bool rhs_special = rhs_null || rhs_nan;

    if (lhs_special && rhs_special)
        return 0;
    if (lhs_special)
        return sort_desc.nulls_direction;
    if (rhs_special)
        return -sort_desc.nulls_direction;

    if (sort_desc.collator && lhs.getType() == Field::Types::String)
    {
        const auto & l = lhs.safeGet<String>();
        const auto & r = rhs.safeGet<String>();
        return sort_desc.collator->compare(l.data(), l.size(), r.data(), r.size());
    }

    if (lhs < rhs)
        return -1;
    if (rhs < lhs)
        return 1;
    return 0;
}

}
