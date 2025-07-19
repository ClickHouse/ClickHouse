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

struct PatchRangesStats
{
    UInt64 min_block_number = 0;
    UInt64 max_block_number = 0;
};

using MaybePatchRangesStats = std::optional<std::vector<PatchRangesStats>>;

MaybePatchRangesStats getPatchRangesStats(const DataPartPtr & patch_part, const MarkRanges & ranges);
MarkRanges filterPatchRanges(const MarkRanges & ranges, const std::vector<PatchRangesStats> & patch_stats, const PatchRangesStats & result_stats);

}
