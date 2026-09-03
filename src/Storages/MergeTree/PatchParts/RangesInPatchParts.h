#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <absl/container/node_hash_map.h>

namespace DB
{

struct MergeTreeReaderSettings;

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
    MarkRanges getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const;

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
    MinMaxStat block_number_stat;
    MinMaxStat block_offset_stat;
};

using MinMaxStats = std::vector<MinMaxStat>;
using MaybeMinMaxStats = std::optional<MinMaxStats>;
using PatchStatsMap = absl::node_hash_map<MarkRange, PatchStats, MarkRangeHash>;

MaybeMinMaxStats getPatchMinMaxStats(const DataPartPtr & patch_part, const MarkRanges & ranges, const String & column_name, const MergeTreeReaderSettings & settings);
MarkRanges filterPatchRanges(const MarkRanges & ranges, const PatchStatsMap & patch_stats, const PatchStats & result_stats);

}
