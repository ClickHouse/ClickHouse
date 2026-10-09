#pragma once

#include <algorithm>
#include <cstddef>

#include <base/defines.h>

namespace DB
{

namespace detail
{

template <typename Equals>
size_t findEqualRangeEndAssumeSortedImpl(size_t begin, size_t end, size_t linear_probe, Equals && equals)
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

/** Debug check for the result of a run-end search: every row in `[begin, run_end)` must be equal to
  * the row at `begin`, and every row in `[run_end, end)` must differ from it. Instead of checking all
  * rows, it checks five sampled positions on each side of `run_end` (the first, the last and the
  * quartiles). If one of them fails, equal values were not contiguous in `[begin, end)`, i.e. the
  * caller passed a range that is not sorted.
  */
template <typename Equals>
void checkEqualRangeEndAssumeSorted(
    [[maybe_unused]] size_t begin, [[maybe_unused]] size_t end, [[maybe_unused]] size_t run_end, [[maybe_unused]] Equals && equals)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    auto check_sample = [&](size_t from, size_t to, bool expected)
    {
        if (from >= to)
            return;
        const size_t len = to - from;
        for (size_t pos : {from, from + len / 4, from + len / 2, from + 3 * len / 4, to - 1})
            chassert(static_cast<bool>(equals(pos)) == expected, "Equal values are not contiguous within the range assumed to be sorted");
    };
    check_sample(begin, run_end, true);
    check_sample(run_end, end, false);
#endif
}

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
  * galloping and binary-search steps depend on. In debug and sanitizer builds the result is verified
  * against that precondition on a sample of positions via checkEqualRangeEndAssumeSorted.
  */
template <typename Equals>
size_t findEqualRangeEndAssumeSorted(size_t begin, size_t end, size_t linear_probe, Equals && equals)
{
    const size_t run_end = detail::findEqualRangeEndAssumeSortedImpl(begin, end, linear_probe, equals);
    checkEqualRangeEndAssumeSorted(begin, end, run_end, equals);
    return run_end;
}

}
