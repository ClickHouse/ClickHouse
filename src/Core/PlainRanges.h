#pragma once

#include <Core/Range.h>

namespace DB
{

/** A plain ranges is a series of ranges who
 *      1. have no intersection in any two of the ranges
 *      2. ordered by left side
 *      3. does not contain blank range
 *
 * Example:
 *      query: (k > 1 and key < 5) or (k > 3 and k < 10) or key in (2, 12)
 *      original ranges: (1, 5), (3, 10), [2, 2], [12, 12]
 *      plain ranges: (1, 10), [12, 12]
 *
 * If it is blank, ranges is empty.
 */
struct PlainRanges
{
    Ranges ranges;

    explicit PlainRanges(const Range & range);

    explicit PlainRanges(const Ranges & ranges_, bool may_have_intersection = false, bool ordered = true);

    PlainRanges unionWith(const PlainRanges & other);
    PlainRanges intersectWith(const PlainRanges & other);

    /// Union ranges and return a new plain(ordered and no intersection) ranges.
    /// Example:
    ///         [1, 3], [2, 4], [6, 8] -> [1, 4], [6, 8]
    ///         [1, 3], [2, 4], (4, 5] -> [1, 4], [5, 5]
    static Ranges makePlainFromUnordered(Ranges ranges_);
    static Ranges makePlainFromOrdered(const Ranges & ranges_);

    static bool compareByLeftBound(const Range & lhs, const Range & rhs);
    static bool compareByRightBound(const Range & lhs, const Range & rhs);

    static std::vector<Ranges> invert(const Ranges & to_invert_ranges);

    static PlainRanges makeBlank() { return PlainRanges({}); }
    static PlainRanges makeUniverse() { return PlainRanges({Range::createWholeUniverseWithoutNull()}); }
};
}
