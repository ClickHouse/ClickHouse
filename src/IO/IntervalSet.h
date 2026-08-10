#pragma once

#include <IO/ChainedBuffers.h>

#include <optional>
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

    /// Returns the set's coverage WITHIN `r` (`set ∩ r`), as disjoint sub-ranges in
    /// increasing-offset order - the complement of `subtract`: what IS covered, clamped to `r`.
    VectorWithMemoryTracking<ByteRange> intersect(ByteRange r) const;

    /// Remove `r`'s bytes from the set, trimming/splitting any overlapping intervals.
    void remove(ByteRange r);

    /// Total bytes held (sum of the disjoint intervals' sizes).
    size_t totalBytes() const;

    /// The interval containing `pos`, or nullopt (also when the set is empty).
    std::optional<ByteRange> coveringInterval(size_t pos) const;

    /// The first interval strictly after `pos` (its offset > pos), or nullopt.
    std::optional<ByteRange> nextIntervalAfter(size_t pos) const;

    /// How far contiguous demand reaches from `pos`: walks intervals forward,
    /// BRIDGING gaps strictly smaller than `bridge_gap` (reading through them
    /// is cheaper than a reopen), and stops at the first wide gap. `pos` in a
    /// narrow gap continues from the next interval; `pos` in a wide gap (or
    /// past the last interval) has no demand - returns `pos` itself.
    size_t contiguousEnd(size_t pos, size_t bridge_gap) const;

    /// The symmetric start: how far back the contiguous demand run containing
    /// (or bridge-adjacent to) `pos` begins, bridging narrow gaps backwards.
    /// `pos` with no demand behind it returns `pos` itself.
    size_t contiguousStart(size_t pos, size_t bridge_gap) const;

    bool empty() const { return intervals.empty(); }

private:
    VectorWithMemoryTracking<ByteRange> intervals;
};

}
