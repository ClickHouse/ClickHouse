#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>

#include <Interpreters/Context.h>

#include <base/insertAtEnd.h>

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

namespace ProfileEvents
{
    extern const Event MergerMutatorsGetPartsForMergeElapsedMicroseconds;
    extern const Event MergerMutatorPrepareRangesForMergeElapsedMicroseconds;
    extern const Event MergerMutatorRangesForMergeCount;
    extern const Event MergerMutatorPartsInRangesForMergeCount;
    extern const Event MergerMutatorSelectPartsForMergeElapsedMicroseconds;
    extern const Event MergerMutatorSelectRangePartsCount;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsInt64 merge_with_ttl_timeout;
    extern const MergeTreeSettingsInt64 merge_with_recompression_ttl_timeout;
    extern const MergeTreeSettingsBool min_age_to_force_merge_on_partition_only;
    extern const MergeTreeSettingsUInt64 min_age_to_force_merge_seconds;
    extern const MergeTreeSettingsBool enable_max_bytes_limit_for_min_age_to_force_merge;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_optimize_entire_partition;
}

namespace
{

PartsRanges checkRanges(PartsRanges && ranges)
{
#ifndef NDEBUG
    /// If some range was generated -- it should not be empty.
    for (const auto & range : ranges)
        chassert(!range.empty());
#endif

    return ranges;
}

size_t calculatePartsCount(const PartsRanges & ranges)
{
    size_t count = 0;

    for (const auto & range : ranges)
        count += range.size();

    return count;
}

PartsRanges splitByMergePredicate(PartsRange && range, const MergePredicatePtr & merge_predicate, LogSeriesLimiter & series_log)
{
    const auto & build_next_range = [&](PartsRange::iterator & current_it)
    {
        PartsRange mergeable_range = {*current_it++};

        while (current_it != range.end())
        {
            PartProperties & prev_part = mergeable_range.back();
            PartProperties & current_part = *current_it;

            /// If we cannot merge with previous part we need to close this range.
            if (auto result = merge_predicate->canMergeParts(prev_part, current_part); !result.has_value())
            {
                LOG_TRACE(series_log, "Can't merge parts {} and {}. Reason: {}", prev_part.name, current_part.name, result.error().text);
                return mergeable_range;
            }

            /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
            if (current_part.info.contains(prev_part.info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} contains previous part {}", current_part.name, prev_part.name);

            if (!current_part.info.isDisjoint(prev_part.info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}", current_part.name, prev_part.name);

            mergeable_range.push_back(std::move(current_part));
            ++current_it;
        }

        return mergeable_range;
    };

    PartsRanges mergeable_ranges;
    for (auto current_it = range.begin(); current_it != range.end();)
        if (auto next_mergeable_range = build_next_range(current_it); !next_mergeable_range.empty())
            mergeable_ranges.push_back(std::move(next_mergeable_range));

    return mergeable_ranges;
}

PartsRanges splitByMergePredicate(PartsRanges && ranges, const MergePredicatePtr & merge_predicate, LogSeriesLimiter & series_log)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergerMutatorPrepareRangesForMergeElapsedMicroseconds);

    PartsRanges mergeable_ranges;
    for (auto && range : ranges)
    {
        auto split_range_by_predicate = splitByMergePredicate(std::move(range), merge_predicate, series_log);
        insertAtEnd(mergeable_ranges, std::move(split_range_by_predicate));
    }

    LOG_TRACE(series_log, "Split parts into {} mergeable ranges using merge predicate", mergeable_ranges.size());
    ProfileEvents::increment(ProfileEvents::MergerMutatorPartsInRangesForMergeCount, calculatePartsCount(mergeable_ranges));
    ProfileEvents::increment(ProfileEvents::MergerMutatorRangesForMergeCount, mergeable_ranges.size());

    return checkRanges(std::move(mergeable_ranges));
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

std::unordered_map<String, PartsRanges> combineByPartitions(PartsRanges && ranges)
{
    std::unordered_map<String, PartsRanges> ranges_by_partitions;

    for (auto && range : ranges)
    {
        chassert(!range.empty());
        ranges_by_partitions[range.front().info.partition_id].push_back(std::move(range));
    }

    return ranges_by_partitions;
}

struct PartitionStatistics
{
    time_t min_age = std::numeric_limits<time_t>::max();
    size_t part_count = 0;
    size_t total_size = 0;
};

std::unordered_map<String, PartitionStatistics> calculateStatisticsForPartitions(const PartsRanges & ranges)
{
    std::unordered_map<String, PartitionStatistics> stats;

    for (const auto & range : ranges)
    {
        chassert(!range.empty());
        PartitionStatistics & partition_stats = stats[range.front().info.partition_id];

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
        return {};

    if (!(*settings)[MergeTreeSetting::min_age_to_force_merge_seconds])
        return {};

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
        return {};

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

std::optional<MergeSelectorChoice> chooseMergeFrom(
    const MergeSelectorApplier & selector,
    const PartsRanges & ranges,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeSettingsPtr & data_settings,
    const PartitionIdToTTLs & next_delete_times,
    const PartitionIdToTTLs & next_recompress_times,
    bool can_use_ttl_merges,
    time_t current_time,
    const LoggerPtr & log)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergerMutatorSelectPartsForMergeElapsedMicroseconds);

    auto choice = selector.chooseMergeFrom(
        ranges, metadata_snapshot, data_settings, next_delete_times, next_recompress_times,
        can_use_ttl_merges, current_time);

    if (choice.has_value())
    {
        const auto & range = choice->range;
        ProfileEvents::increment(ProfileEvents::MergerMutatorSelectRangePartsCount, choice->range.size());
        LOG_INFO(log, "Selected {} parts from {} to {}. Merge selecting phase took: {}ms", range.size(), range.front().name, range.back().name, watch.elapsed() / 1000);
    }

    return choice;
}

}

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_)
    : data(data_)
    , log(getLogger(data.getLogName() + " (MergerMutator)"))
{
}

void MergeTreeDataMergerMutator::updateTTLMergeTimes(const MergeSelectorChoice & merge_choice, const MergeTreeSettingsPtr & settings, time_t current_time)
{
    chassert(!merge_choice.range.empty());
    const String & partition_id = merge_choice.range.front().info.partition_id;

    switch (merge_choice.merge_type)
    {
        case MergeType::Regular:
            /// Do not update anything for regular merge.
            return;
        case MergeType::TTLDelete:
            next_delete_ttl_merge_times_by_partition[partition_id] = current_time + (*settings)[MergeTreeSetting::merge_with_ttl_timeout];
            return;
        case MergeType::TTLRecompress:
            next_recompress_ttl_merge_times_by_partition[partition_id] = current_time + (*settings)[MergeTreeSetting::merge_with_recompression_ttl_timeout];
            return;
    }
}

PartitionIdsHint MergeTreeDataMergerMutator::getPartitionsThatMayBeMerged(
    const PartsCollectorPtr & parts_collector,
    const MergePredicatePtr & merge_predicate,
    const MergeSelectorApplier & selector) const
{
    const auto context = data.getContext();
    const auto settings = data.getSettings();
    const auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto storage_policy = data.getStoragePolicy();
    const time_t current_time = std::time(nullptr);
    const bool can_use_ttl_merges = !ttl_merges_blocker.isCancelled();
    LogSeriesLimiter series_log(log, 1, /*interval_s_=*/60 * 30);

    auto ranges = grabAllPossibleRanges(parts_collector, metadata_snapshot, storage_policy, current_time, std::nullopt, series_log);
    if (ranges.empty())
        return {};

    ranges = splitByMergePredicate(std::move(ranges), merge_predicate, series_log);
    if (ranges.empty())
        return {};

    const auto partitions_stats = calculateStatisticsForPartitions(ranges);
    const auto ranges_by_partitions = combineByPartitions(std::move(ranges));

    PartitionIdsHint partitions_hint;
    for (const auto & [partition, ranges_in_partition] : ranges_by_partitions)
    {
        chassert(!ranges_in_partition.empty());
        chassert(!ranges_in_partition.front().empty());

        auto merge_choice = chooseMergeFrom(
            selector,
            ranges_in_partition, metadata_snapshot, settings, next_delete_ttl_merge_times_by_partition, next_recompress_ttl_merge_times_by_partition,
            can_use_ttl_merges, current_time, log);

        const String & partition_id = ranges_in_partition.front().front().info.partition_id;

        if (merge_choice.has_value())
            partitions_hint.insert(partition_id);
        else
            LOG_TRACE(log, "Nothing to merge in partition {} with max_total_size_to_merge = {} (looked up {} ranges)",
                partition_id, ReadableSize(selector.max_total_size_to_merge), ranges_in_partition.size());
    }

    if (auto best = getBestPartitionToOptimizeEntire(selector.max_total_size_to_merge, context, settings, partitions_stats, log); !best.empty())
        partitions_hint.insert(std::move(best));

    LOG_TRACE(log,
            "Checked {} partitions, found {} partitions with parts that may be merged: [{}] "
            "(max_total_size_to_merge={}, merge_with_ttl_allowed={}, can_use_ttl_merges={})",
            ranges_by_partitions.size(), partitions_hint.size(), fmt::join(partitions_hint, ", "),
            selector.max_total_size_to_merge, selector.merge_with_ttl_allowed, can_use_ttl_merges);

    return partitions_hint;
}

std::expected<MergeSelectorChoice, SelectMergeFailure> MergeTreeDataMergerMutator::selectPartsToMerge(
    const PartsCollectorPtr & parts_collector,
    const MergePredicatePtr & merge_predicate,
    const MergeSelectorApplier & selector,
    const std::optional<PartitionIdsHint> & partitions_hint)
{
    const auto context = data.getContext();
    const auto settings = data.getSettings();
    const auto metadata_snapshot = data.getInMemoryMetadataPtr();
    const auto storage_policy = data.getStoragePolicy();
    const time_t current_time = std::time(nullptr);
    const bool can_use_ttl_merges = !ttl_merges_blocker.isCancelled();
    LogSeriesLimiter series_log(log, 1, /*interval_s_=*/60 * 30);

    auto ranges = grabAllPossibleRanges(parts_collector, metadata_snapshot, storage_policy, current_time, partitions_hint, series_log);
    if (ranges.empty())
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create("There are no parts that can be merged. (Collector returned empty ranges set)"),
        });
    }

    ranges = splitByMergePredicate(std::move(ranges), merge_predicate, series_log);
    if (ranges.empty())
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create("No parts satisfy preconditions for merge"),
        });
    }

    auto merge_choice = chooseMergeFrom(
        selector,
        ranges, metadata_snapshot, settings, next_delete_ttl_merge_times_by_partition, next_recompress_ttl_merge_times_by_partition,
        can_use_ttl_merges, current_time, log);

    if (merge_choice.has_value())
    {
        updateTTLMergeTimes(merge_choice.value(), settings, current_time);
        return std::move(merge_choice.value());
    }

    const auto partitions_stats = calculateStatisticsForPartitions(ranges);

    if (auto best = getBestPartitionToOptimizeEntire(selector.max_total_size_to_merge, context, settings, partitions_stats, log); !best.empty())
    {
        return selectAllPartsToMergeWithinPartition(
            metadata_snapshot,
            parts_collector,
            merge_predicate,
            /*partition_id=*/best,
            /*final=*/true,
            /*optimize_skip_merged_partitions=*/true);
    }

    return std::unexpected(SelectMergeFailure{
        .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
        .explanation = PreformattedMessage::create("There is no need to merge parts according to merge selector algorithm"),
    });
}

std::expected<MergeSelectorChoice, SelectMergeFailure> MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    const StorageMetadataPtr & metadata_snapshot,
    const PartsCollectorPtr & parts_collector,
    const MergePredicatePtr & merge_predicate,
    const String & partition_id,
    bool final,
    bool optimize_skip_merged_partitions)
{
    /// time is not important in this context, since the parts will not be passed through the merge selector.
    const time_t current_time = std::time(nullptr);
    const auto storage_policy = data.getStoragePolicy();

    auto collect_result = grabAllPartsInsidePartition(parts_collector, metadata_snapshot, storage_policy, current_time, partition_id);
    if (!collect_result)
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = std::move(collect_result.error()),
        });
    }

    auto parts = std::move(collect_result.value());

    if (parts.empty())
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create("There are no parts inside partition"),
        });
    }

    if (!final && parts.size() == 1)
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create("There is only one part inside partition"),
        });
    }

    /// If final, optimize_skip_merged_partitions is true and we have only one part in partition with level > 0
    /// than we don't select it to merge. But if there are some expired TTL then merge is needed
    if (final && optimize_skip_merged_partitions && parts.size() == 1)
    {
        const PartProperties & part = parts.front();

        /// FIXME? Probably we should check expired ttls here, not only calculated.
        if (part.info.level > 0 && (!metadata_snapshot->hasAnyTTL() || part.all_ttl_calculated_if_any))
        {
            return std::unexpected(SelectMergeFailure{
                .reason = SelectMergeFailure::Reason::NOTHING_TO_MERGE,
                .explanation = PreformattedMessage::create("Partition skipped due to optimize_skip_merged_partitions"),
            });
        }
    }

    if (auto result = canMergeAllParts(parts, merge_predicate); !result.has_value())
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = std::move(result.error()),
        });
    }

    /// Enough disk space to cover the new merge with a margin.
    const auto required_disk_space = CompactionStatistics::estimateAtLeastAvailableSpace(parts);
    const auto available_disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    if (available_disk_space <= required_disk_space)
    {
        return std::unexpected(SelectMergeFailure{
            .reason = SelectMergeFailure::Reason::CANNOT_SELECT,
            .explanation = PreformattedMessage::create(
                "Not enough free space to merge parts from {} to {}. Has {} free and unreserved, {} required now",
                parts.front().name, parts.back().name, ReadableSize(available_disk_space), ReadableSize(required_disk_space)),
        });
    }

    LOG_INFO(log, "Selected {} parts from {} to {}", parts.size(), parts.front().name, parts.back().name);
    return MergeSelectorChoice{std::move(parts), MergeType::Regular, final};
}

/// parts should be sorted.
MergeTaskPtr MergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    FutureMergedMutatedPartPtr future_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeList::Entry * merge_entry,
    std::unique_ptr<MergeListElement> projection_merge_list_element,
    TableLockHolder & holder,
    time_t time_of_merge,
    ContextPtr context,
    ReservationSharedPtr space_reservation,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    bool cleanup,
    const MergeTreeData::MergingParams & merging_params,
    const MergeTreeTransactionPtr & txn,
    bool need_prefix,
    IMergeTreeDataPart * parent_part,
    const String & suffix)
{
    return std::make_shared<MergeTask>(
        future_part,
        const_cast<StorageMetadataPtr &>(metadata_snapshot),
        merge_entry,
        std::move(projection_merge_list_element),
        time_of_merge,
        context,
        holder,
        space_reservation,
        deduplicate,
        deduplicate_by_columns,
        cleanup,
        merging_params,
        need_prefix,
        parent_part,
        suffix,
        txn,
        &data,
        this,
        &merges_blocker,
        &ttl_merges_blocker);
}

MutateTaskPtr MergeTreeDataMergerMutator::mutatePartToTemporaryPart(
    FutureMergedMutatedPartPtr future_part,
    StorageMetadataPtr metadata_snapshot,
    MutationCommandsConstPtr commands,
    MergeListEntry * merge_entry,
    time_t time_of_mutation,
    ContextPtr context,
    const MergeTreeTransactionPtr & txn,
    ReservationSharedPtr space_reservation,
    TableLockHolder & holder,
    bool need_prefix)
{
    return std::make_shared<MutateTask>(
        future_part,
        metadata_snapshot,
        commands,
        merge_entry,
        time_of_mutation,
        context,
        space_reservation,
        holder,
        txn,
        data,
        *this,
        merges_blocker,
        need_prefix);
}

MergeTreeData::DataPartPtr MergeTreeDataMergerMutator::renameMergedTemporaryPart(
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const MergeTreeData::DataPartsVector & parts,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData::Transaction & out_transaction)
{
    /// Some of source parts was possibly created in transaction, so non-transactional merge may break isolation.
    if (data.transactions_enabled.load(std::memory_order_relaxed) && !txn)
        throw Exception(ErrorCodes::ABORTED,
            "Cancelling merge, because it was done without starting transaction,"
            "but transactions were enabled for this table");

    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, out_transaction, /*rename_in_transaction=*/true);

    /// Explicitly rename part while still holding the lock for tmp folder to avoid cleanup
    out_transaction.renameParts();

    /// Let's check that all original parts have been deleted and only them.
    if (replaced_parts.size() != parts.size())
    {
        /** This is normal, although this happens rarely.
         *
         * The situation - was replaced 0 parts instead of N can be, for example, in the following case
         * - we had A part, but there was no B and C parts;
         * - A, B -> AB was in the queue, but it has not been done, because there is no B part;
         * - AB, C -> ABC was in the queue, but it has not been done, because there are no AB and C parts;
         * - we have completed the task of downloading a B part;
         * - we started to make A, B -> AB merge, since all parts appeared;
         * - we decided to download ABC part from another replica, since it was impossible to make merge AB, C -> ABC;
         * - ABC part appeared. When it was added, old A, B, C parts were deleted;
         * - AB merge finished. AB part was added. But this is an obsolete part. The log will contain the message `Obsolete part added`,
         *   then we get here.
         *
         * When M > N parts could be replaced?
         * - new block was added in ReplicatedMergeTreeSink;
         * - it was added to working dataset in memory and renamed on filesystem;
         * - but ZooKeeper transaction that adds it to reference dataset in ZK failed;
         * - and it is failed due to connection loss, so we don't rollback working dataset in memory,
         *   because we don't know if the part was added to ZK or not
         *   (see ReplicatedMergeTreeSink)
         * - then method selectPartsToMerge selects a range and sees, that EphemeralLock for the block in this part is unlocked,
         *   and so it is possible to merge a range skipping this part.
         *   (NOTE: Merging with part that is not in ZK is not possible, see checks in 'createLogEntryToMergeParts'.)
         * - and after merge, this part will be removed in addition to parts that was merged.
         */
        LOG_WARNING(log,
            "Unexpected number of parts removed when adding {}: {} instead of {}\n"
            "Replaced parts:\n{}\n"
            "Parts:\n{}\n",
            new_data_part->name,
            replaced_parts.size(),
            parts.size(),
            fmt::join(getPartsNames(replaced_parts), "\n"),
            fmt::join(getPartsNames(parts), "\n"));
    }
    else
    {
        for (size_t i = 0; i < parts.size(); ++i)
            if (parts[i]->name != replaced_parts[i]->name)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Unexpected part removed when adding {}: {} instead of {}",
                    new_data_part->name, replaced_parts[i]->name, parts[i]->name);
    }

    LOG_INFO(log, "Merged {} parts: [{}, {}] -> {}", parts.size(), parts.front()->name, parts.back()->name, new_data_part->name);
    return new_data_part;
}

}
