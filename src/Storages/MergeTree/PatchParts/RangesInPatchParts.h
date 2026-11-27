#pragma once
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/MarkRange.h>
#include <absl/container/node_hash_map.h>
#include <Common/VectorWithMemoryTracking.h>
#include <mutex>

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
    [[nodiscard]] std::vector<MarkRanges> getRanges(const DataPartPtr & original_part, const PatchPartsForReader & patch_parts, const MarkRanges & ranges) const;
    [[nodiscard]] const std::unordered_map<String, MarkRanges> & getRanges() const { return ranges_by_name; }

private:
    [[nodiscard]] MarkRanges getIntersectingRanges(const String & patch_name, const MarkRanges & ranges) const;

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

/// Whether the reads of different ranges of the original part take different granules of the patch part, so that the
/// patch is split between the replicas as the read is. A `Merge` patch is, by the offsets of the rows. A `Join` patch is
/// not, nor a `MergeOnKey` one whose sorting key cannot select its ranges, as every read takes all of it. For another
/// `MergeOnKey` patch, a run of equal keys can give the same granules to all the reads: it is not split when a granule of
/// it is taken by each of 4 pieces of `read_ranges`, the ranges the query reads from the part. A read of fewer marks is
/// taken to split it.
bool patchSplitsWithRead(const DataPartPtr & original_part, const PatchPartInfoForReader & patch, const MarkRanges & read_ranges);

/// `patchSplitsWithRead` of each patch of a part, computed once for all the tasks reading the part.
class PatchesSplitWithRead
{
public:
    explicit PatchesSplitWithRead(MarkRangesPtr read_ranges_) : read_ranges(std::move(read_ranges_)) {}

    /// For the patch at `patch_index` of `patches`, which must be the patches of `original_part` the query reads.
    bool splits(const DataPartPtr & original_part, const PatchPartsForReader & patches, size_t patch_index);

private:
    const MarkRangesPtr read_ranges;
    std::once_flag computed;
    VectorWithMemoryTracking<UInt8> splits_with_read;
};

MaybeMinMaxStats getPatchMinMaxStats(const DataPartPtr & patch_part, const MarkRanges & ranges, const String & column_name, const MergeTreeReaderSettings & settings);
MarkRanges filterPatchRanges(const MarkRanges & ranges, const PatchStatsMap & patch_stats, const PatchStats & result_stats);

}
