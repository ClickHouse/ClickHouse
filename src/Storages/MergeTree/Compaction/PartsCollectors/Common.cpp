#include <Storages/MergeTree/Compaction/PartsCollectors/Common.h>

#include <Disks/IStoragePolicy.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/FailPoint.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace FailPoints
{
    extern const char merge_parts_collection_before_volume_recheck[];
}

PartsRanges constructPartsRanges(
    std::vector<MergeTreeDataPartsVector> && ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time)
{
    PartsRanges properties_ranges;
    properties_ranges.reserve(ranges.size());
    const bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    for (const auto & range : ranges)
    {
        PartsRange properties_range;
        properties_range.reserve(range.size());

        for (const auto & part : range)
            properties_range.push_back(buildPartProperties(part, metadata_snapshot, storage_policy, current_time, has_volumes_with_disabled_merges));

        properties_ranges.push_back(std::move(properties_range));
    }

    fiu_do_on(FailPoints::merge_parts_collection_before_volume_recheck,
    {
        /// Pause an explicit `OPTIMIZE`, so unrelated background collections cannot consume the test hook.
        if (!CurrentThread::getQueryId().empty())
            FailPointInjection::notifyPauseAndWaitForResume(FailPoints::merge_parts_collection_before_volume_recheck);
    });

    /// A volume may have been stopped while constructing the snapshot. Preserve the fast path
    /// for an unchanged policy, but refresh merge eligibility before returning stale properties.
    if (!has_volumes_with_disabled_merges && storage_policy->hasAnyVolumeWithDisabledMerges())
    {
        for (size_t i = 0; i < ranges.size(); ++i)
            for (size_t j = 0; j < ranges[i].size(); ++j)
                properties_ranges[i][j].is_in_volume_where_merges_avoid = !ranges[i][j]->shallParticipateInMerges(storage_policy);
    }

    return properties_ranges;
}

MergeTreeDataPartsVector filterByPartitions(
    MergeTreeDataPartsVector && parts, const std::optional<PartitionIdsHint> & partitions_to_keep)
{
    if (!partitions_to_keep)
        return parts;

    std::erase_if(parts, [&partitions_to_keep](const auto & part) { return !partitions_to_keep->contains(part->info.getPartitionId()); });

    return parts;
}

}
