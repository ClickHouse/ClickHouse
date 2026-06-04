#pragma once

#include <algorithm>
#include <cstddef>

#include <base/defines.h>

namespace DB
{

/** Returns the end (exclusive) of the run of values equal to the value at `begin`, within the sorted
  * range `[begin, end)`. The predicate `equals(i)` must report whether the value at row `i` equals the
  * value at `begin`.
  *
  * The search first does a short linear probe, which is cheap for the common case of short,
  * high-cardinality runs. If the run extends past that probe, it switches to galloping (an exponential
  * probe) followed by a binary search, so for a long run it issues only O(log run) calls to `equals`
  * instead of O(run).
  *
  * The search relies only on an equality predicate, not on ordering: because `[begin, end)` is sorted,
  * the positions equal to `begin` form a contiguous prefix, and that contiguity is the only property the
  * galloping and binary-search steps depend on.
  */
template <typename Equals>
size_t findEqualRangeEndAssumeSorted(size_t begin, size_t end, size_t linear_probe, Equals && equals)
{
    chassert(linear_probe >= 1);

    /// An empty range contains no run, so its end is `begin`.
    if (begin >= end)
        return begin;

    /// First scan a short window linearly, which resolves short runs cheaply.
    const size_t probe_end = std::min(begin + linear_probe, end);
    for (size_t r = begin + 1; r < probe_end; ++r)
        if (!equals(r))
            return r;
    if (probe_end == end)
        return end;

    /// Gallop forward with an exponentially growing step to bracket the run end between `lo` (still
    /// equal, as established by the earlier linear scan) and `hi` (the first probe past it).
    size_t lo = probe_end; /// rows in [begin, lo) all equal the value at `begin`
    size_t hi = end;
    size_t step = linear_probe;
    while (lo < end)
    {
        const size_t probe = std::min(lo + step, end);
        if (equals(probe - 1))
        {
            lo = probe;
            if (probe == end)
                return end;
            step <<= 1;
        }
        else
        {
            hi = probe;
            break;
        }
    }

    /// Binary-search the bracketed range `[lo, hi)` for the first position that is not equal; that is the run end.
    /// `lo` is known from the gallop to equal the value at `begin`.
    while (lo < hi)
    {
        const size_t mid = lo + (hi - lo) / 2;
        if (equals(mid))
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo;
}

}
