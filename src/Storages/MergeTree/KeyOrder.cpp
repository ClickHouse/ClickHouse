#include <Storages/MergeTree/KeyOrder.h>

#include <Common/FieldAccurateComparison.h>

namespace DB
{

int KeyOrder::compareTuples(const FieldRef * left, const FieldRef * right, size_t size) const
{
    for (size_t i = 0; i < size; ++i)
    {
        /// Field comparison of non-scalar values can diverge from the columnar sort order (composite
        /// values compare NULL or NaN elements by type index), so nothing can be concluded about
        /// such coordinates.
        if (!Field::isScalar(left[i].getType()) || !Field::isScalar(right[i].getType()))
            return 0;

        /// The (physicalStartExtreme, physicalEndExtreme) pair marks a column whose value is unknown
        /// at both boundaries; nothing can be concluded about the order of tuples that differ first
        /// at such a column.
        if (left[i] == physicalStartExtreme(i) && right[i] == physicalEndExtreme(i))
            return 0;

        /// Equality must follow the total sort order, not value-space semantics: accurateLess places
        /// NaN above every number (the writer sorts with the same convention), so two NaNs are equal
        /// here, while accurateEquals would report them as unequal.
        if (accurateLess(left[i], right[i]))
            return isReversed(i) ? 1 : -1;

        if (accurateLess(right[i], left[i]))
            return isReversed(i) ? -1 : 1;
    }
    return 0;
}

}
