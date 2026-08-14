#pragma once

#include <Storages/MergeTree/BoolMask.h>
#include <Storages/MergeTree/MarkRange.h>

#include <functional>

namespace DB
{

/// Generic exclusion search over the marks of one data part.
///
/// The search is used when a filter cannot be evaluated with a binary search over the primary key
/// index, for example a filter on a non-prefix column of the primary key. It recursively splits
/// mark ranges that may contain matching rows into smaller subranges and drops the subranges for
/// which the check callback proves that the condition cannot be true.

struct GenericExclusionSearchSettings
{
    /// The maximum number of subranges a range is split into when it has to be analyzed further.
    /// Must be greater than 1. The default matches the default of `merge_tree_coarse_index_granularity`.
    size_t coarse_index_granularity = 8;

    /// Budget of check invocations spent on the search; 0 means unlimited. When the budget no
    /// longer covers the checks already owed to the queued ranges, the remaining ranges are not
    /// split further: each still receives its one check and is then either dropped or accepted
    /// whole, so the result may contain more marks, but the analysis time stays bounded. The budget
    /// is spent on the largest remaining ranges first. It is applied loosely: the children of an
    /// allowed split are not reserved in advance, so the number of checks may exceed the budget by
    /// at most `coarse_index_granularity`, and at least one check per initial range is always made.
    size_t max_steps = 0;

    /// Accepted ranges separated by a gap of at most this many marks are merged, because reading the
    /// gap sequentially is cheaper than seeking past it. Exact ranges are never merged across a gap.
    size_t min_marks_for_seek = 0;
};

struct GenericExclusionSearchResult
{
    /// Sorted and non-intersecting. A superset of all marks within the initial ranges for which the
    /// condition can be true.
    MarkRanges ranges;

    /// Sorted and non-intersecting, each range contained in some element of `ranges`. The condition
    /// holds for every row of these marks. Filled only if `collect_exact_ranges` is set.
    MarkRanges exact_ranges;

    /// The number of check invocations made.
    size_t num_steps = 0;

    /// Set when the step budget forced at least one range to be accepted whole instead of being
    /// split. Merely spending the whole budget on checks does not set it.
    bool reached_step_limit = false;
};

/// Returns a BoolMask telling whether the condition can be true and whether it can be false for the
/// rows of the given mark range.
using MarkRangeCheck = std::function<BoolMask(const MarkRange &)>;

/// The initial ranges must be sorted and non-intersecting.
GenericExclusionSearchResult genericExclusionSearch(
    const MarkRanges & initial_ranges,
    const MarkRangeCheck & check_in_range,
    const GenericExclusionSearchSettings & search_settings,
    bool collect_exact_ranges);

}
