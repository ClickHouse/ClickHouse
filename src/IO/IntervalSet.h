#pragma once

#include <IO/ChainedBuffers.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A set of disjoint, sorted byte intervals. `ReaderExecutor` tracks window
/// coverage with one: every byte appended to the result is `add`-ed first, and
/// reads only fill what `subtract` reports as uncovered - so the assembled chain
/// is disjoint by construction regardless of overlapping cache tiers.
class IntervalSet
{
public:
    /// Add a range, merging overlaps and adjacencies.
    void add(ByteRange r);

    /// Returns `r` minus all intervals in the set, as disjoint sub-ranges in
    /// increasing-offset order.
    VectorWithMemoryTracking<ByteRange> subtract(ByteRange r) const;

    /// Remove `r`'s bytes from the set, trimming/splitting any overlapping intervals.
    void remove(ByteRange r);

    /// Total bytes held (sum of the disjoint intervals' sizes).
    size_t totalBytes() const;

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}
