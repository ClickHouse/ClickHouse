#pragma once

#include <Storages/MergeTree/MergeTreeReadRangesRefiner.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <unordered_map>

namespace DB
{

/// The ranges of parts that the query condition cache of this server narrowed, keyed by the part.
/// A part missing from the map was not narrowed. The part pointer keeps the key alive.
struct QueryConditionCacheReadRanges
{
    struct Entry
    {
        DataPartPtr part;
        /// Sorted, non-intersecting; empty if the cache dropped the whole part.
        MarkRanges ranges;
    };

    std::unordered_map<const IMergeTreeDataPart *, Entry> parts;

    /// Records the parts whose ranges differ between `before` and `after` (`after` is `before`
    /// filtered by `MergeTreeDataSelectExecutor::filterPartsByQueryConditionCache`).
    static std::shared_ptr<const QueryConditionCacheReadRanges> create(const RangesInDataParts & before, const RangesInDataParts & after);
};

using QueryConditionCacheReadRangesPtr = std::shared_ptr<const QueryConditionCacheReadRanges>;

/// With parallel replicas the coordinator assigns ranges from the analysis of one replica, so only that
/// replica's query condition cache prunes them. Every other replica would read the granules the coordinator
/// assigns it even when its own cache knows they hold no matching rows, and since the assignment of segments
/// is stable, it would never benefit from its cache. This refiner drops those granules on the replica that
/// owns the cache entries. It then applies the inner refiner, if any, to what is left.
class QueryConditionCacheReadRangesRefiner : public IMergeTreeReadRangesRefiner
{
public:
    QueryConditionCacheReadRangesRefiner(QueryConditionCacheReadRangesPtr read_ranges_, MergeTreeReadRangesRefinerPtr inner_);

    MarkRanges refine(const MergeTreeReadTaskInfo & info, MarkRanges ranges) const override;

private:
    const QueryConditionCacheReadRangesPtr read_ranges;
    const MergeTreeReadRangesRefinerPtr inner;
};

}
