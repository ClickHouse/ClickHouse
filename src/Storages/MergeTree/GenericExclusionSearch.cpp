#include <Storages/MergeTree/GenericExclusionSearch.h>

#include <base/defines.h>

#include <algorithm>
#include <queue>
#include <tuple>
#include <vector>

namespace DB
{

namespace
{

/// Appends a range to a left-to-right sequence of accepted ranges, merging it into the last range
/// when the gap between them is at most `max_gap` marks. The caller guarantees that `range` starts
/// at or after the end of the last appended range.
void appendWithMaxGap(MarkRanges & ranges, MarkRange range, size_t max_gap)
{
    /// The left-to-right order is what makes the unsigned gap subtraction below safe.
    chassert(ranges.empty() || range.begin >= ranges.back().end);

    if (ranges.empty() || range.begin - ranges.back().end > max_gap)
        ranges.push_back(range);
    else
        ranges.back().end = range.end;
}

/// The size of the subranges a range is split into; the leftmost subrange may be smaller.
size_t splitStep(const MarkRange & range, size_t coarse_index_granularity)
{
    /// A single-mark range must not be split, and a split factor of one would reproduce the range
    /// itself; either would make the search loop forever.
    chassert(range.end - range.begin > 1);
    chassert(coarse_index_granularity > 1);

    return (range.end - range.begin - 1) / coarse_index_granularity + 1;
}

/// Splits a range into at most `coarse_index_granularity` subranges and passes them to `consume`
/// from right to left.
template <typename Consume>
void splitRange(MarkRange range, size_t coarse_index_granularity, Consume && consume)
{
    size_t step = splitStep(range, coarse_index_granularity);
    size_t end = 0;

    for (end = range.end; end > range.begin + step; end -= step)
        consume(MarkRange(end - step, end));

    consume(MarkRange(range.begin, end));
}

/// The classic exhaustive algorithm. The stack holds the mark ranges that still have to be
/// analyzed, disjoint and ordered so that the leftmost one is on top (at the back). Each iteration
/// pops the leftmost range and asks the check callback about it. A range where no row can match the
/// condition is discarded. A range where every row matches, or a range of a single mark, is added
/// to the result. Any other range may contain both matching and non-matching rows, so it is split
/// into smaller subranges, which are pushed onto the stack for the same treatment.
void searchUnlimited(
    const MarkRanges & initial_ranges,
    const MarkRangeCheck & check_in_range,
    const GenericExclusionSearchSettings & search_settings,
    bool collect_exact_ranges,
    GenericExclusionSearchResult & result)
{
    for (const auto & initial_range : initial_ranges)
    {
        std::vector<MarkRange> ranges_stack = {initial_range};
        while (!ranges_stack.empty())
        {
            MarkRange range = ranges_stack.back();
            ranges_stack.pop_back();

            ++result.num_steps;

            BoolMask mask = check_in_range(range);

            if (!mask.can_be_true)
                continue;

            bool exact = !mask.can_be_false;
            if (exact || range.end == range.begin + 1)
            {
                appendWithMaxGap(result.ranges, range, search_settings.min_marks_for_seek);

                /// Unlike `ranges`, exact ranges must never absorb the gap between two accepted
                /// ranges: every mark of an exact range has to fully match the condition, while the
                /// skipped marks in between do not.
                if (collect_exact_ranges && exact)
                    appendWithMaxGap(result.exact_ranges, range, 0);
            }
            else
            {
                splitRange(range, search_settings.coarse_index_granularity, [&](MarkRange subrange) { ranges_stack.push_back(subrange); });
            }
        }
    }
}

/// Orders the queue of the step-limited search: the largest range comes first.
struct LessBySize
{
    bool operator()(const MarkRange & lhs, const MarkRange & rhs) const
    {
        size_t lhs_size = lhs.end - lhs.begin;
        size_t rhs_size = rhs.end - rhs.begin;
        return std::tie(lhs_size, rhs.begin) < std::tie(rhs_size, lhs.begin);
    }
};

/// The step-limited variant spends a budget of check invocations, largest ranges first. A range is
/// split further only when the budget still covers one check for every range already in the queue;
/// otherwise the range is accepted whole. The children of an allowed split are not reserved in
/// advance, so the total number of checks may exceed the budget by at most one split fan-out, but a
/// split is never refused while the budget lasts and every queued range still receives its one
/// check that can exclude it entirely.
void searchLimited(
    const MarkRanges & initial_ranges,
    const MarkRangeCheck & check_in_range,
    const GenericExclusionSearchSettings & search_settings,
    bool collect_exact_ranges,
    GenericExclusionSearchResult & result)
{
    std::vector<MarkRange> heap_storage(initial_ranges.begin(), initial_ranges.end());
    std::priority_queue<MarkRange, std::vector<MarkRange>, LessBySize> queue(LessBySize{}, std::move(heap_storage));

    /// Ranges are accepted out of spatial order, so they are collected raw and merged after sorting.
    std::vector<MarkRange> accepted;
    std::vector<MarkRange> accepted_exact;

    while (!queue.empty())
    {
        MarkRange range = queue.top();
        queue.pop();

        ++result.num_steps;

        BoolMask mask = check_in_range(range);

        if (!mask.can_be_true)
            continue;

        bool exact = !mask.can_be_false;
        if (exact || range.end == range.begin + 1)
        {
            accepted.push_back(range);
            if (collect_exact_ranges && exact)
                accepted_exact.push_back(range);
            continue;
        }

        if (result.num_steps + queue.size() > search_settings.max_steps)
        {
            result.reached_step_limit = true;
            accepted.push_back(range);
            continue;
        }

        splitRange(range, search_settings.coarse_index_granularity, [&](MarkRange subrange) { queue.push(subrange); });
    }

    /// The number of checks never exceeds the budget by more than the fan-out of the last allowed
    /// split, except that every initial range always receives its one check.
    chassert(
        result.num_steps <= search_settings.coarse_index_granularity
        || result.num_steps - search_settings.coarse_index_granularity <= std::max(search_settings.max_steps, initial_ranges.size()));

    std::sort(accepted.begin(), accepted.end());
    for (const auto & range : accepted)
        appendWithMaxGap(result.ranges, range, search_settings.min_marks_for_seek);

    std::sort(accepted_exact.begin(), accepted_exact.end());
    for (const auto & range : accepted_exact)
        appendWithMaxGap(result.exact_ranges, range, 0);
}

}

GenericExclusionSearchResult genericExclusionSearch(
    const MarkRanges & initial_ranges,
    const MarkRangeCheck & check_in_range,
    const GenericExclusionSearchSettings & search_settings,
    bool collect_exact_ranges)
{
    GenericExclusionSearchResult result;

#ifndef NDEBUG
    assertSortedAndNonIntersecting(initial_ranges);
#endif

    if (search_settings.max_steps == 0)
        searchUnlimited(initial_ranges, check_in_range, search_settings, collect_exact_ranges, result);
    else
        searchLimited(initial_ranges, check_in_range, search_settings, collect_exact_ranges, result);

#ifndef NDEBUG
    assertSortedAndNonIntersecting(result.ranges);
    assertSortedAndNonIntersecting(result.exact_ranges);

    const auto * containing = result.ranges.begin();
    for (const auto & exact_range : result.exact_ranges)
    {
        while (containing != result.ranges.end() && containing->end < exact_range.end)
            ++containing;
        chassert(containing != result.ranges.end() && containing->begin <= exact_range.begin && exact_range.end <= containing->end);
    }
#endif

    return result;
}

}
