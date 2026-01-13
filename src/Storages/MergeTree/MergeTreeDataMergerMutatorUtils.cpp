#include <Storages/MergeTree/MergeTreeDataMergerMutatorUtils.h>

#include <Storages/MergeTree/MergeTreeSettings.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/quoteString.h>
#include <Common/WeightedRandomSampling.h>

#include <Interpreters/Context.h>
#include <base/insertAtEnd.h>

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

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

std::expected<void, PreformattedMessage> canMergeAllParts(const PartsRange & range, const MergePredicatePtr & merge_predicate)
{
    for (size_t i = 1; i < range.size(); ++i)
    {
        const auto & prev_part = range[i - 1];
        const auto & current_part = range[i];

        if (auto can_merge_result = merge_predicate->canMergeParts(prev_part, current_part); !can_merge_result)
            return can_merge_result;
    }

    return {};
}

std::unordered_map<String, PartitionStatistics> calculateStatisticsForPartitions(const PartsRanges & ranges)
{
    std::unordered_map<String, PartitionStatistics> stats;

    for (const auto & range : ranges)
    {
        chassert(!range.empty());
        PartitionStatistics & partition_stats = stats[range.front().info.getPartitionId()];

        partition_stats.part_count += range.size();

        for (const auto & part : range)
        {
            partition_stats.min_age = std::min(partition_stats.min_age, part.age);
            partition_stats.total_size += part.size;
        }
    }

    return stats;
}

String getBestPartitionToOptimizeEntire(
    size_t max_total_size_to_merge,
    const ContextPtr & context,
    const MergeTreeSettingsPtr & settings,
    const std::unordered_map<String, PartitionStatistics> & stats,
    const LoggerPtr & log)
{
    if (!(*settings)[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
    {
        return {};
    }

    if (!(*settings)[MergeTreeSetting::min_age_to_force_merge_seconds])
    {
        return {};
    }

    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);
    size_t max_tasks_count = context->getMergeMutateExecutor()->getMaxTasksCount();
    if (occupied > 1 && max_tasks_count - occupied < (*settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_execute_optimize_entire_partition])
    {
        LOG_INFO(log,
            "Not enough idle threads to execute optimizing entire partition. See settings "
            "'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' and 'background_pool_size'");

        return {};
    }

    const auto is_partition_invalid = [&](const PartitionStatistics & partition)
    {
        if (partition.part_count == 1)
            return true;

        if (!max_total_size_to_merge || !(*settings)[MergeTreeSetting::enable_max_bytes_limit_for_min_age_to_force_merge])
            return false;

        return partition.total_size > max_total_size_to_merge;
    };

    auto best_partition_it = std::max_element(
        stats.begin(),
        stats.end(),
        [&](const auto & e1, const auto & e2)
        {
            // If one partition cannot be used for some reason (e.g. it has only single part, or it's size greater than limit), always select the other partition.
            if (is_partition_invalid(e1.second))
                return true;

            if (is_partition_invalid(e2.second))
                return false;

            // If both partitions have more than one part, select the older partition.
            return e1.second.min_age < e2.second.min_age;
        });

    chassert(best_partition_it != stats.end());

    const size_t best_partition_min_age = static_cast<size_t>(best_partition_it->second.min_age);
    if (best_partition_min_age < (*settings)[MergeTreeSetting::min_age_to_force_merge_seconds] || is_partition_invalid(best_partition_it->second))
    {
        return {};
    }

    return best_partition_it->first;
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
