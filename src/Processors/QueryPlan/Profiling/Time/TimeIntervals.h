#pragma once

#include <vector>
#include <base/types.h>

namespace DB
{

struct TimeInterval
{
    UInt64 start;
    UInt64 end;
};

using TimeIntervals = std::vector<TimeInterval>;

/// Collapse overlaps in a start-sorted sequence into one sorted, non-overlapping sequence.
TimeIntervals collapseSortedIntervals(TimeIntervals sorted);

/// k-way merge of already start-sorted sequences into one start-sorted (possibly overlapping) sequence.
TimeIntervals mergeSortedIntervals(const std::vector<TimeIntervals> & sorted_sequences);

/// Merge sorted sequences and collapse overlaps into one sorted, non-overlapping sequence.
TimeIntervals uniteSortedIntervals(const std::vector<TimeIntervals> & sorted_sequences);

/// Total length of a non-overlapping sequence.
UInt64 totalIntervalsLength(const TimeIntervals & intervals);

}
