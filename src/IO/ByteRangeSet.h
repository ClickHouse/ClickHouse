#pragma once

#include <IO/ByteRange.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

/// A set of disjoint (non-intersecting), sorted byte intervals. `ReadBuffer::setRequestMap` takes one as the
/// ranges a caller will read. `ReaderExecutor` also tracks window coverage with one: it `add`-s every byte before
/// it appends the byte to the result, and it fills only what `subtract` reports as uncovered. So the assembled
/// chain stays disjoint by construction, even when cache tiers overlap.
class ByteRangeSet
{
public:
    /// Add a range, merging overlaps and adjacencies.
    void add(ByteRange range);

    /// Returns `range` minus all intervals in the set, as disjoint sub-ranges in
    /// increasing-offset order.
    VectorWithMemoryTracking<ByteRange> subtract(ByteRange range) const;

    /// Remove `range`'s bytes from the set, trimming or splitting any overlapping interval.
    void remove(ByteRange range);

    /// The parts of the intervals that lie inside `range`.
    ByteRangeSet intersect(ByteRange range) const;

    /// Move every interval forward by `delta` bytes.
    void shift(size_t delta);

    /// Total bytes held (sum of the disjoint intervals' sizes).
    size_t totalBytes() const;

    bool empty() const { return intervals.empty(); }

    /// The disjoint intervals in increasing-offset order (read-only view).
    const VectorWithMemoryTracking<ByteRange> & ranges() const { return intervals; }

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}
