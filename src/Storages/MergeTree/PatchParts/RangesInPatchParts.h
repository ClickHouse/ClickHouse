#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/MarkRange.h>

namespace DB
{

struct RangesInPatchParts
{
public:
    explicit RangesInPatchParts(size_t max_granules_in_range_) : max_granules_in_range(max_granules_in_range_)
    {
    }

    void optimize();
    void addPart(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & original_ranges);
    std::vector<MarkRanges> getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const;

private:
    std::set<MarkRange> getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const;

    size_t max_granules_in_range;
    std::unordered_map<String, MarkRanges> ranges_by_name;
};

struct PatchRangeStats
{
    UInt64 min_value = 0;
    UInt64 max_value = 0;
};

using PatchRangesStats = std::vector<PatchRangeStats>;
using MaybePatchRangesStats = std::optional<PatchRangesStats>;

MaybePatchRangesStats getPatchRangesStats(const DataPartPtr & patch_part, const MarkRanges & ranges, const String & column_name);
MarkRanges filterPatchRanges(const MarkRanges & ranges, const PatchRangesStats & patch_stats, const PatchRangeStats & result_stats);

}
