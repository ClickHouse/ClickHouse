#pragma once

#include <algorithm>
#include <cstddef>

#include <base/defines.h>
#include <base/types.h>
#include <Common/VectorWithMemoryTracking.h>

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

/** Shared tail of compareTrackAt (see IColumn::compareTrackAt): given `compare_result` for lhs[n] vs
  * rhs[m], find the end of the run of lesser rows on the lesser side by galloping. The predicate
  * is_less(row) must report lhs[row] < rhs[m], and is_greater(row) must report lhs[n] > rhs[row];
  * the sorted-tail precondition of compareTrackAt applies to the probed side.
  *
  * ALWAYS_INLINE: the merge joins call compareTrackAt once per loop iteration; leaving this as a
  * separate call behind the virtual one costs ~10% on interleaved short-run keys.
  */
template <typename IsLess, typename IsGreater>
ALWAYS_INLINE Int64 compareTrackAtImpl(
    int compare_result,
    size_t n,
    size_t m,
    size_t lhs_size,
    size_t rhs_size,
    size_t linear_probe,
    IsLess && is_less,
    IsGreater && is_greater)
{
    if (compare_result < 0)
    {
        /// Resolve a run of length one with a single comparison before paying the run-search setup cost.
        if (n + 1 >= lhs_size || !is_less(n + 1))
            return -1;
        return -static_cast<Int64>(findEqualRangeEndAssumeSorted(n + 1, lhs_size, linear_probe, is_less) - n);
    }
    if (compare_result > 0)
    {
        if (m + 1 >= rhs_size || !is_greater(m + 1))
            return 1;
        return static_cast<Int64>(findEqualRangeEndAssumeSorted(m + 1, rhs_size, linear_probe, is_greater) - m);
    }
    return 0;
}

/** Returns the end of the run of rows whose multi-column key equals the key at `begin`, within `[begin, end)` sorted by
  * that key. `search(i, from, bound)` must return the end of the run of column `i` at `from` within `[from, bound)`;
  * column `i` is sorted there because `bound` is the end of the run of columns `0..i-1`.
  */
template <typename Search>
size_t findKeyRangeEndAssumeSorted(size_t key_size, size_t begin, size_t end, Search && search)
{
    size_t run_end = end;
    for (size_t i = 0; i < key_size; ++i)
    {
        run_end = search(i, begin, run_end);
        if (run_end <= begin + 1)
            break;
    }
    return run_end;
}

/** Same result as findKeyRangeEndAssumeSorted, for callers that search the runs of one range one after another.
  * Remembers the last run found for each key prefix: a search from a row inside it ends at the same row, so a column
  * is searched again only when the search leaves its prefix's run. Call reset() when the columns or `end` change.
  */
class SortedKeyRuns
{
public:
    SortedKeyRuns() = default;
    explicit SortedKeyRuns(size_t key_size) : runs(key_size) {}

    void reset(size_t key_size) { runs.assign(key_size, Run{}); }
    size_t keySize() const { return runs.size(); }

    template <typename Search>
    ALWAYS_INLINE size_t findRunEnd(size_t begin, size_t end, Search && search)
    {
        const size_t key_size = runs.size();
        if (key_size < 2)
            return findKeyRangeEndAssumeSorted(key_size, begin, end, search);

        size_t level = 0;
        while (level < key_size && runs[level].begin <= begin && begin < runs[level].end)
            ++level;

        size_t run_end = level == 0 ? end : runs[level - 1].end;
        for (; level < key_size; ++level)
        {
            run_end = search(level, begin, run_end);
            if (run_end <= begin + 1)
                break;
            runs[level] = {begin, run_end};
        }

        chassert(
            run_end == findKeyRangeEndAssumeSorted(key_size, begin, end, search),
            "Equal values are not contiguous within the range assumed to be sorted");
        return run_end;
    }

private:
    struct Run
    {
        size_t begin = 0;
        size_t end = 0;
    };

    VectorWithMemoryTracking<Run> runs;
};

}
