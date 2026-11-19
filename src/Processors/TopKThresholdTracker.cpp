#include <Processors/TopKThresholdTracker.h>
#include <Columns/Collator.h>

namespace DB
{

int TopKThresholdTracker::compareFields(const Field & lhs, const Field & rhs) const
{
    bool lhs_null = lhs.isNull();
    bool rhs_null = rhs.isNull();

    if (lhs_null && rhs_null)
        return 0;
    if (lhs_null)
        return sort_desc.nulls_direction;
    if (rhs_null)
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
