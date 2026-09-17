#pragma once

#include <IO/ChainedBuffers.h>
#include <Common/VectorWithMemoryTracking.h>

#include <optional>

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

    /// The set's coverage WITHIN `r` (`set ∩ r`), as disjoint sub-ranges in increasing-offset
    /// order - the complement of `subtract`, clamped to `r`.
    VectorWithMemoryTracking<ByteRange> intersect(ByteRange r) const;

    /// The interval containing `pos`, or nullopt (also when the set is empty).
    std::optional<ByteRange> coveringInterval(size_t pos) const;

    /// The first interval strictly after `pos` (its offset > pos), or nullopt.
    std::optional<ByteRange> nextIntervalAfter(size_t pos) const;

    /// How far contiguous demand reaches from `pos`: walks intervals forward, BRIDGING gaps
    /// strictly smaller than `bridge_gap`, stopping at the first wide gap. `pos` with no demand
    /// (a wide gap, or past the last interval) returns `pos` itself.
    size_t contiguousEnd(size_t pos, size_t bridge_gap) const;

    /// The symmetric start: how far back the contiguous demand run containing (or bridge-adjacent
    /// to) `pos` begins, bridging narrow gaps backwards. `pos` with no demand behind returns `pos`.
    size_t contiguousStart(size_t pos, size_t bridge_gap) const;

    bool empty() const { return intervals.empty(); }

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}
