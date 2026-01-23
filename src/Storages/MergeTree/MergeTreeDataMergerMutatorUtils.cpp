#include <Storages/MergeTree/MergeTreeDataMergerMutatorUtils.h>

#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/quoteString.h>
#include <Common/WeightedRandomSampling.h>

#include <Interpreters/Context.h>
#include <base/insertAtEnd.h>

namespace ProfileEvents
{
    extern const Event MergerMutatorsGetPartsForMergeElapsedMicroseconds;
    extern const Event MergerMutatorSelectPartsForMergeElapsedMicroseconds;
    extern const Event MergerMutatorSelectRangePartsCount;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsInt64 merge_with_ttl_timeout;
    extern const MergeTreeSettingsInt64 merge_with_recompression_ttl_timeout;
    extern const MergeTreeSettingsBool min_age_to_force_merge_on_partition_only;
    extern const MergeTreeSettingsUInt64 min_age_to_force_merge_seconds;
    extern const MergeTreeSettingsBool enable_max_bytes_limit_for_min_age_to_force_merge;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_optimize_entire_partition;
    extern const MergeTreeSettingsBool apply_patches_on_merge;
    extern const MergeTreeSettingsMergeSelectorAlgorithm merge_selector_algorithm;

    /// Cloud only
    extern const MergeTreeSettingsUInt64 number_of_partitions_to_consider_for_merge;
}

PartsRanges grabAllPossibleRanges(
    const PartsCollectorPtr & parts_collector,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::optional<PartitionIdsHint> & partitions_hint,
    LogSeriesLimiter & series_log)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergerMutatorsGetPartsForMergeElapsedMicroseconds);
    return parts_collector->grabAllPossibleRanges(metadata_snapshot, storage_policy, current_time, partitions_hint, series_log);
}

MergeSelectorChoices chooseMergesFrom(
    const MergeSelectorApplier & selector,
    const IMergePredicate & predicate,
    const PartsRanges & ranges,
    const PartitionsStatistics & partitions_stats,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time,
    const LoggerPtr & log)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergerMutatorSelectPartsForMergeElapsedMicroseconds);

    auto choices = selector.chooseMergesFrom(
        ranges, partitions_stats, predicate, metadata_snapshot,
        data_settings, next_delete_times, next_recompress_times,
        can_use_ttl_merges, current_time);

    if (!choices.empty())
    {
        LOG_TRACE(log, "Selected {} merge ranges. Merge selecting phase took: {}ms", choices.size(), watch.elapsed() / 1000);

        for (size_t i = 0; i < choices.size(); ++i)
        {
            const auto & merge_type = choices[i].merge_type;
            const auto & range = choices[i].range;
            const auto & range_patches = choices[i].range_patches;
            ProfileEvents::increment(ProfileEvents::MergerMutatorSelectRangePartsCount, range.size());
            LOG_TRACE(log, "Merge #{} type {} with {} parts from {} to {} with {} patches", i, merge_type, range.size(), range.front().name, range.back().name, range_patches.size());
        }
    }

    return choices;
}

std::expected<PartsRange, PreformattedMessage> grabAllPartsInsidePartition(
    const PartsCollectorPtr & parts_collector,
    const StorageMetadataPtr & metadata_snapshot,
    const StoragePolicyPtr & storage_policy,
    const time_t & current_time,
    const std::string & partition_id)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergerMutatorsGetPartsForMergeElapsedMicroseconds);
    return parts_collector->grabAllPartsInsidePartition(metadata_snapshot, storage_policy, current_time, partition_id);
}


}
