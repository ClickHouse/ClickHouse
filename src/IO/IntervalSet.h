#pragma once

#include <IO/ChainedBuffers.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A set of disjoint (non-intersecting), sorted byte intervals. `ReaderExecutor` tracks window
/// coverage with one. It `add`-s every byte before it appends the byte to the result, and it fills
/// only what `subtract` reports as uncovered. So the assembled chain stays disjoint by construction,
/// even when cache tiers overlap.
class IntervalSet
{
public:
    /// Add a range, merging overlaps and adjacencies.
    void add(ByteRange range);

    /// Returns `range` minus all intervals in the set, as disjoint sub-ranges in
    /// increasing-offset order.
    VectorWithMemoryTracking<ByteRange> subtract(ByteRange range) const;

    /// Remove `range`'s bytes from the set, trimming or splitting any overlapping interval.
    void remove(ByteRange range);

    /// Total bytes held (sum of the disjoint intervals' sizes).
    size_t totalBytes() const;

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}
