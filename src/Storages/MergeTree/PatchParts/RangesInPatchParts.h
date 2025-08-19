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
    const std::unordered_map<String, MarkRanges> & getRanges() const { return ranges_by_name; }

private:
    std::set<MarkRange> getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const;

    size_t max_granules_in_range;
    std::unordered_map<String, MarkRanges> ranges_by_name;
};

struct MinMaxStat
{
    UInt64 min = 0;
    UInt64 max = 0;
};
struct PatchStats
{
    MinMaxStats block_number_stats;
    MinMaxStats block_offset_stats;
};

using PatchRangesStats = std::vector<MinMaxStats>;
using MaybePatchRangesStats = std::optional<PatchRangesStats>;

MaybePatchRangesStats getPatchRangesStats(const DataPartPtr & patch_part, const MarkRanges & ranges, const String & column_name);
MarkRanges filterPatchRanges(const MarkRanges & ranges, const std::map<MarkRange, PatchStats> & patch_stats, const PatchStats & result_stats);

}
