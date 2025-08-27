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

    void addPart(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & original_ranges);
    void optimize();

    std::vector<MarkRanges> getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const;
    std::set<MarkRange> getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const;

private:
    size_t max_granules_in_range;
    std::unordered_map<String, MarkRanges> ranges_by_name;
};

}
