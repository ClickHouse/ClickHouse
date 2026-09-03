#pragma once

#include <Storages/MergeTree/Compaction/MergeSelectors/IMergeSelector.h>
#include <Storages/MergeTree/Compaction/PartProperties.h>

namespace DB
{

using RangesIterator = PartsRanges::const_iterator;
using PartsIterator = PartsRange::const_iterator;

/// Class that maintaints disjoint sets for each independent parts range returned from collector.
class DisjointPartsRangesSet
{
    struct PartsRangeBoundaries
    {
        PartsIterator range_begin;
        PartsIterator range_end;
    };

    struct BoundariesComparator
    {
        using is_transparent = void;

        bool operator()(const PartsRangeBoundaries & lhs, const PartsRangeBoundaries & rhs) const noexcept;
        bool operator()(const PartsRangeBoundaries & lhs, const PartsIterator & rhs) const noexcept;
        bool operator()(const PartsIterator & lhs, const PartsRangeBoundaries & rhs) const noexcept;
    };

    using SortedPartsRanges = std::set<PartsRangeBoundaries, BoundariesComparator>;

    static bool isDisjoint(const PartsRangeBoundaries & lhs, const PartsRangeBoundaries & rhs);
    static bool isDisjoint(const PartsRangeBoundaries & boundaries, const SortedPartsRanges & sorted_ranges);
    static bool isCovered(const PartsIterator & part_it, const SortedPartsRanges & sorted_ranges);

public:
    explicit DisjointPartsRangesSet(const PartsRanges & uncovered_ranges_);

    /// Check that specific point inside range is covered.
    bool isCovered(RangesIterator range_it, PartsIterator part_it) const;

    /// Updates disjoint set for range_it range with [range_being, range_end).
    bool addRangeIfPossible(RangesIterator range_it, PartsIterator range_begin, PartsIterator range_end);

private:
    /// Expected source of disjoint ranges.
    const PartsRanges & uncovered_ranges;

    std::unordered_map<const PartsRange *, SortedPartsRanges> disjoint;
};

}
