#include "MergeTreeDataMergerMutator.h"

#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/SimpleMergeSelector.h>
#include <Storages/MergeTree/AllMergeSelector.h>
#include <Storages/MergeTree/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>

#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Merges/MergingSortedTransform.h>
#include <Processors/Merges/CollapsingSortedTransform.h>
#include <Processors/Merges/SummingSortedTransform.h>
#include <Processors/Merges/ReplacingSortedTransform.h>
#include <Processors/Merges/GraphiteRollupSortedTransform.h>
#include <Processors/Merges/AggregatingSortedTransform.h>
#include <Processors/Merges/VersionedCollapsingTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Interpreters/MutationsInterpreter.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/Context.h>
#include <Common/interpolate.h>
#include <Common/typeid_cast.h>
#include <Common/escapeForFileName.h>
#include <Parsers/queryToString.h>

#include <cmath>
#include <ctime>
#include <numeric>

#include <boost/algorithm/string/replace.hpp>

namespace CurrentMetrics
{
    extern const Metric BackgroundMergesAndMutationsPoolTask;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ABORTED;
}

/// Do not start to merge parts, if free space is less than sum size of parts times specified coefficient.
/// This value is chosen to not allow big merges to eat all free space. Thus allowing small merges to proceed.
static const double DISK_USAGE_COEFFICIENT_TO_SELECT = 2;

/// To do merge, reserve amount of space equals to sum size of parts times specified coefficient.
/// Must be strictly less than DISK_USAGE_COEFFICIENT_TO_SELECT,
///  because between selecting parts to merge and doing merge, amount of free space could have decreased.
static const double DISK_USAGE_COEFFICIENT_TO_RESERVE = 1.1;

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_, size_t max_tasks_count_)
    : data(data_), max_tasks_count(max_tasks_count_), log(&Poco::Logger::get(data.getLogName() + " (MergerMutator)"))
{
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge() const
{
    size_t scheduled_tasks_count = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    return getMaxSourcePartsSizeForMerge(max_tasks_count, scheduled_tasks_count);
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge(size_t max_count, size_t scheduled_tasks_count) const
{
    if (scheduled_tasks_count > max_count)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: invalid argument passed to \
            getMaxSourcePartsSize: scheduled_tasks_count = {} > max_count = {}", scheduled_tasks_count, max_count);

    size_t free_entries = max_count - scheduled_tasks_count;
    const auto data_settings = data.getSettings();

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    if (scheduled_tasks_count <= 1 || free_entries >= data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge)
        max_size = data_settings->max_bytes_to_merge_at_max_space_in_pool;
    else
        max_size = interpolateExponential(
            data_settings->max_bytes_to_merge_at_min_space_in_pool,
            data_settings->max_bytes_to_merge_at_max_space_in_pool,
            static_cast<double>(free_entries) / data_settings->number_of_free_entries_in_pool_to_lower_max_size_of_merge);

    return std::min(max_size, static_cast<UInt64>(data.getStoragePolicy()->getMaxUnreservedFreeSpace() / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartSizeForMutation() const
{
    const auto data_settings = data.getSettings();
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    /// DataPart can be store only at one disk. Get maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();

    /// Allow mutations only if there are enough threads, leave free threads for merges else
    if (occupied <= 1
        || max_tasks_count - occupied >= data_settings->number_of_free_entries_in_pool_to_execute_mutation)
        return static_cast<UInt64>(disk_space / DISK_USAGE_COEFFICIENT_TO_RESERVE);

    return 0;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectPartsToMerge(
    FutureMergedMutatedPartPtr future_part,
    bool aggressive,
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback,
    bool merge_with_ttl_allowed,
    const MergeTreeTransactionPtr & txn,
    String * out_disable_reason)
{
    MergeTreeData::DataPartsVector data_parts;
    if (txn)
    {
        /// Merge predicate (for simple MergeTree) allows to merge two parts only if both parts are visible for merge transaction.
        /// So at the first glance we could just get all active parts.
        /// Active parts include uncommitted parts, but it's ok and merge predicate handles it.
        /// However, it's possible that some transaction is trying to remove a part in the middle, for example, all_2_2_0.
        /// If parts all_1_1_0 and all_3_3_0 are active and visible for merge transaction, then we would try to merge them.
        /// But it's wrong, because all_2_2_0 may become active again if transaction will roll back.
        /// That's why we must include some outdated parts into `data_part`, more precisely, such parts that removal is not committed.
        MergeTreeData::DataPartsVector active_parts;
        MergeTreeData::DataPartsVector outdated_parts;

        {
            auto lock = data.lockParts();
            active_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Active}, lock);
            outdated_parts = data.getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated}, lock);
        }

        ActiveDataPartSet active_parts_set{data.format_version};
        for (const auto & part : active_parts)
            active_parts_set.add(part->name);

        for (const auto & part : outdated_parts)
        {
            /// We don't need rolled back parts.
            /// NOTE When rolling back a transaction we set creation_csn to RolledBackCSN at first
            /// and then remove part from working set, so there's no race condition
            if (part->version.creation_csn == Tx::RolledBackCSN)
                continue;

            /// We don't need parts that are finally removed.
            /// NOTE There's a minor race condition: we may get UnknownCSN if a transaction has been just committed concurrently.
            /// But it's not a problem if we will add such part to `data_parts`.
            if (part->version.removal_csn != Tx::UnknownCSN)
                continue;

            active_parts_set.add(part->name);
        }

        /// Restore "active" parts set from selected active and outdated parts
        auto remove_pred = [&](const MergeTreeData::DataPartPtr & part) -> bool
        {
            return active_parts_set.getContainingPart(part->info) != part->name;
        };

        auto new_end_it = std::remove_if(active_parts.begin(), active_parts.end(), remove_pred);
        active_parts.erase(new_end_it, active_parts.end());

        new_end_it = std::remove_if(outdated_parts.begin(), outdated_parts.end(), remove_pred);
        outdated_parts.erase(new_end_it, outdated_parts.end());

        std::merge(active_parts.begin(), active_parts.end(),
                   outdated_parts.begin(), outdated_parts.end(),
                   std::back_inserter(data_parts), MergeTreeData::LessDataPart());
    }
    else
    {
        /// Simply get all active parts
        data_parts = data.getDataPartsVectorForInternalUsage();
    }
    const auto data_settings = data.getSettings();
    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    if (data_parts.empty())
    {
        if (out_disable_reason)
            *out_disable_reason = "There are no parts in the table";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    time_t current_time = std::time(nullptr);

    IMergeSelector::PartsRanges parts_ranges;

    StoragePolicyPtr storage_policy = data.getStoragePolicy();
    /// Volumes with stopped merges are extremely rare situation.
    /// Check it once and don't check each part (this is bad for performance).
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    const String * prev_partition_id = nullptr;
    /// Previous part only in boundaries of partition frame
    const MergeTreeData::DataPartPtr * prev_part = nullptr;

    size_t parts_selected_precondition = 0;
    for (const MergeTreeData::DataPartPtr & part : data_parts)
    {
        const String & partition_id = part->info.partition_id;

        if (!prev_partition_id || partition_id != *prev_partition_id)
        {
            if (parts_ranges.empty() || !parts_ranges.back().empty())
                parts_ranges.emplace_back();

            /// New partition frame.
            prev_partition_id = &partition_id;
            prev_part = nullptr;
        }

        /// Check predicate only for the first part in each range.
        if (!prev_part)
        {
            /* Parts can be merged with themselves for TTL needs for example.
            * So we have to check if this part is currently being inserted with quorum and so on and so forth.
            * Obviously we have to check it manually only for the first part
            * of each partition because it will be automatically checked for a pair of parts. */
            if (!can_merge_callback(nullptr, part, txn.get(), nullptr))
                continue;

            /// This part can be merged only with next parts (no prev part exists), so start
            /// new interval if previous was not empty.
            if (!parts_ranges.back().empty())
                parts_ranges.emplace_back();
        }
        else
        {
            /// If we cannot merge with previous part we had to start new parts
            /// interval (in the same partition)
            if (!can_merge_callback(*prev_part, part, txn.get(), nullptr))
            {
                /// Now we have no previous part
                prev_part = nullptr;

                /// Mustn't be empty
                assert(!parts_ranges.back().empty());

                /// Some parts cannot be merged with previous parts and also cannot be merged with themselves,
                /// for example, merge is already assigned for such parts, or they participate in quorum inserts
                /// and so on.
                /// Also we don't start new interval here (maybe all next parts cannot be merged and we don't want to have empty interval)
                if (!can_merge_callback(nullptr, part, txn.get(), nullptr))
                    continue;

                /// Starting new interval in the same partition
                parts_ranges.emplace_back();
            }
        }

        IMergeSelector::Part part_info;
        part_info.size = part->getBytesOnDisk();
        part_info.age = current_time - part->modification_time;
        part_info.level = part->info.level;
        part_info.data = &part;
        part_info.ttl_infos = &part->ttl_infos;
        part_info.compression_codec_desc = part->default_codec->getFullCodecDesc();
        part_info.shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true;

        ++parts_selected_precondition;

        parts_ranges.back().emplace_back(part_info);

        /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part && part->info.partition_id == (*prev_part)->info.partition_id
            && part->info.min_block <= (*prev_part)->info.max_block)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}", part->name, (*prev_part)->name);
        }

        prev_part = &part;
    }

    if (parts_selected_precondition == 0)
    {
        if (out_disable_reason)
            *out_disable_reason = "No parts satisfy preconditions for merge";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    IMergeSelector::PartsRange parts_to_merge;

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && !ttl_merges_blocker.isCancelled())
    {
        /// TTL delete is preferred to recompression
        TTLDeleteMergeSelector delete_ttl_selector(
                next_delete_ttl_merge_times_by_partition,
                current_time,
                data_settings->merge_with_ttl_timeout,
                data_settings->ttl_only_drop_parts);

        parts_to_merge = delete_ttl_selector.select(parts_ranges, max_total_size_to_merge);
        if (!parts_to_merge.empty())
        {
            future_part->merge_type = MergeType::TTL_DELETE;
        }
        else if (metadata_snapshot->hasAnyRecompressionTTL())
        {
            TTLRecompressMergeSelector recompress_ttl_selector(
                    next_recompress_ttl_merge_times_by_partition,
                    current_time,
                    data_settings->merge_with_recompression_ttl_timeout,
                    metadata_snapshot->getRecompressionTTLs());

            parts_to_merge = recompress_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part->merge_type = MergeType::TTL_RECOMPRESS;
        }
    }

    if (parts_to_merge.empty())
    {
        SimpleMergeSelector::Settings merge_settings;
        /// Override value from table settings
        merge_settings.max_parts_to_merge_at_once = data_settings->max_parts_to_merge_at_once;

        if (aggressive)
            merge_settings.base = 1;

        parts_to_merge = SimpleMergeSelector(merge_settings)
                            .select(parts_ranges, max_total_size_to_merge);

        /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
        if (parts_to_merge.size() == 1)
            throw Exception("Logical error: merge selector returned only one part to merge", ErrorCodes::LOGICAL_ERROR);

        if (parts_to_merge.empty())
        {
            if (out_disable_reason)
                *out_disable_reason = "There is no need to merge parts according to merge selector algorithm";
            return SelectPartsDecision::CANNOT_SELECT;
        }
    }

    MergeTreeData::DataPartsVector parts;
    parts.reserve(parts_to_merge.size());
    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = *static_cast<const MergeTreeData::DataPartPtr *>(part_info.data);
        parts.push_back(part);
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part->assign(std::move(parts));
    return SelectPartsDecision::SELECTED;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    FutureMergedMutatedPartPtr future_part,
    const AllowedMergingPredicate & can_merge,
    const String & partition_id,
    bool final,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeTransactionPtr & txn,
    String * out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition_id);

    if (parts.empty())
    {
        if (out_disable_reason)
            *out_disable_reason = "There are no parts inside partition";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    if (!final && parts.size() == 1)
    {
        if (out_disable_reason)
            *out_disable_reason = "There is only one part inside partition";
        return SelectPartsDecision::CANNOT_SELECT;
    }

    /// If final, optimize_skip_merged_partitions is true and we have only one part in partition with level > 0
    /// than we don't select it to merge. But if there are some expired TTL then merge is needed
    if (final && optimize_skip_merged_partitions && parts.size() == 1 && parts[0]->info.level > 0 &&
        (!metadata_snapshot->hasAnyTTL() || parts[0]->checkAllTTLCalculated(metadata_snapshot)))
    {
        if (out_disable_reason)
            *out_disable_reason = "Partition skipped due to optimize_skip_merged_partitions";
        return SelectPartsDecision::NOTHING_TO_MERGE;
    }

    auto it = parts.begin();
    auto prev_it = it;

    UInt64 sum_bytes = 0;
    while (it != parts.end())
    {
        /// For the case of one part, we check that it can be merged "with itself".
        if ((it != parts.begin() || parts.size() == 1) && !can_merge(*prev_it, *it, txn.get(), out_disable_reason))
        {
            return SelectPartsDecision::CANNOT_SELECT;
        }

        sum_bytes += (*it)->getBytesOnDisk();

        prev_it = it;
        ++it;
    }

    auto available_disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    /// Enough disk space to cover the new merge with a margin.
    auto required_disk_space = sum_bytes * DISK_USAGE_COEFFICIENT_TO_SELECT;
    if (available_disk_space <= required_disk_space)
    {
        time_t now = time(nullptr);
        if (now - disk_space_warning_time > 3600)
        {
            disk_space_warning_time = now;
            LOG_WARNING(log,
                "Won't merge parts from {} to {} because not enough free space: {} free and unreserved"
                ", {} required now (+{}% on overhead); suppressing similar warnings for the next hour",
                parts.front()->name,
                (*prev_it)->name,
                ReadableSize(available_disk_space),
                ReadableSize(sum_bytes),
                static_cast<int>((DISK_USAGE_COEFFICIENT_TO_SELECT - 1.0) * 100));
        }

        if (out_disable_reason)
            *out_disable_reason = fmt::format("Insufficient available disk space, required {}", ReadableSize(required_disk_space));

        return SelectPartsDecision::CANNOT_SELECT;
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part->assign(std::move(parts));

    return SelectPartsDecision::SELECTED;
}


MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::selectAllPartsFromPartition(const String & partition_id)
{
    MergeTreeData::DataPartsVector parts_from_partition;

    MergeTreeData::DataParts data_parts = data.getDataPartsForInternalUsage();

    for (const auto & current_part : data_parts)
    {
        if (current_part->info.partition_id != partition_id)
            continue;

        parts_from_partition.push_back(current_part);
    }

    return parts_from_partition;
}

/// parts should be sorted.
MergeTaskPtr MergeTreeDataMergerMutator::mergePartsToTemporaryPart(
    FutureMergedMutatedPartPtr future_part,
    const StorageMetadataPtr & metadata_snapshot,
    MergeList::Entry * merge_entry,
    std::unique_ptr<MergeListElement> projection_merge_list_element,
    TableLockHolder,
    time_t time_of_merge,
    ContextPtr context,
    ReservationSharedPtr space_reservation,
    bool deduplicate,
    const Names & deduplicate_by_columns,
    const MergeTreeData::MergingParams & merging_params,
    const MergeTreeTransactionPtr & txn,
    const IMergeTreeDataPart * parent_part,
    const String & suffix)
{
    return std::make_shared<MergeTask>(
        future_part,
        const_cast<StorageMetadataPtr &>(metadata_snapshot),
        merge_entry,
        std::move(projection_merge_list_element),
        time_of_merge,
        context,
        space_reservation,
        deduplicate,
        deduplicate_by_columns,
        merging_params,
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
    TableLockHolder & holder)
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
        merges_blocker
    );
}


MergeAlgorithm MergeTreeDataMergerMutator::chooseMergeAlgorithm(
    const MergeTreeData::DataPartsVector & parts,
    size_t sum_rows_upper_bound,
    const NamesAndTypesList & gathering_columns,
    bool deduplicate,
    bool need_remove_expired_values,
    const MergeTreeData::MergingParams & merging_params) const
{
    const auto data_settings = data.getSettings();

    if (deduplicate)
        return MergeAlgorithm::Horizontal;
    if (data_settings->enable_vertical_merge_algorithm == 0)
        return MergeAlgorithm::Horizontal;
    if (need_remove_expired_values)
        return MergeAlgorithm::Horizontal;

    for (const auto & part : parts)
        if (!part->supportsVerticalMerge())
            return MergeAlgorithm::Horizontal;

    bool is_supported_storage =
        merging_params.mode == MergeTreeData::MergingParams::Ordinary ||
        merging_params.mode == MergeTreeData::MergingParams::Collapsing ||
        merging_params.mode == MergeTreeData::MergingParams::Replacing ||
        merging_params.mode == MergeTreeData::MergingParams::VersionedCollapsing;

    bool enough_ordinary_cols = gathering_columns.size() >= data_settings->vertical_merge_algorithm_min_columns_to_activate;

    bool enough_total_rows = sum_rows_upper_bound >= data_settings->vertical_merge_algorithm_min_rows_to_activate;

    bool no_parts_overflow = parts.size() <= RowSourcePart::MAX_PARTS;

    auto merge_alg = (is_supported_storage && enough_total_rows && enough_ordinary_cols && no_parts_overflow) ?
                        MergeAlgorithm::Vertical : MergeAlgorithm::Horizontal;

    return merge_alg;
}


MergeTreeData::DataPartPtr MergeTreeDataMergerMutator::renameMergedTemporaryPart(
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const MergeTreeData::DataPartsVector & parts,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData::Transaction * out_transaction)
{
    /// Some of source parts was possibly created in transaction, so non-transactional merge may break isolation.
    if (data.transactions_enabled.load(std::memory_order_relaxed) && !txn)
        throw Exception(ErrorCodes::ABORTED, "Cancelling merge, because it was done without starting transaction,"
                                             "but transactions were enabled for this table");

    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, txn.get(), nullptr, out_transaction);

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
         * - new block was added in ReplicatedMergeTreeBlockOutputStream;
         * - it was added to working dataset in memory and renamed on filesystem;
         * - but ZooKeeper transaction that adds it to reference dataset in ZK failed;
         * - and it is failed due to connection loss, so we don't rollback working dataset in memory,
         *   because we don't know if the part was added to ZK or not
         *   (see ReplicatedMergeTreeBlockOutputStream)
         * - then method selectPartsToMerge selects a range and sees, that EphemeralLock for the block in this part is unlocked,
         *   and so it is possible to merge a range skipping this part.
         *   (NOTE: Merging with part that is not in ZK is not possible, see checks in 'createLogEntryToMergeParts'.)
         * - and after merge, this part will be removed in addition to parts that was merged.
         */
        LOG_WARNING(log, "Unexpected number of parts removed when adding {}: {} instead of {}", new_data_part->name, replaced_parts.size(), parts.size());
    }
    else
    {
        for (size_t i = 0; i < parts.size(); ++i)
            if (parts[i]->name != replaced_parts[i]->name)
                throw Exception("Unexpected part removed when adding " + new_data_part->name + ": " + replaced_parts[i]->name
                    + " instead of " + parts[i]->name, ErrorCodes::LOGICAL_ERROR);
    }

    LOG_TRACE(log, "Merged {} parts: from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    return new_data_part;
}


size_t MergeTreeDataMergerMutator::estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts)
{
    size_t res = 0;
    for (const MergeTreeData::DataPartPtr & part : source_parts)
        res += part->getBytesOnDisk();

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

void MergeTreeDataMergerMutator::splitMutationCommands(
    MergeTreeData::DataPartPtr part,
    const MutationCommands & commands,
    MutationCommands & for_interpreter,
    MutationCommands & for_file_renames)
{
    ColumnsDescription part_columns(part->getColumns());

    if (!isWidePart(part))
    {
        NameSet mutated_columns;
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_COLUMN
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);

                if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
                    mutated_columns.emplace(command.column_name);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN)
                {
                    mutated_columns.emplace(command.column_name);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    for_interpreter.push_back(
                    {
                        .type = MutationCommand::Type::READ_COLUMN,
                        .column_name = command.rename_to,
                    });
                    mutated_columns.emplace(command.column_name);
                    part_columns.rename(command.column_name, command.rename_to);
                }
            }
        }
        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part->getColumns())
        {
            if (!mutated_columns.contains(column.name))
                for_interpreter.emplace_back(
                    MutationCommand{.type = MutationCommand::Type::READ_COLUMN, .column_name = column.name, .data_type = column.type});
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_COLUMN
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE)
            {
                for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX || command.type == MutationCommand::Type::DROP_PROJECTION)
            {
                for_file_renames.push_back(command);
            }
            /// If we don't have this column in source part, than we don't need
            /// to materialize it
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::READ_COLUMN)
                {
                    for_interpreter.push_back(command);
                }
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    part_columns.rename(command.column_name, command.rename_to);
                    for_file_renames.push_back(command);
                }
                else
                {
                    for_file_renames.push_back(command);
                }
            }
        }
    }
}


std::pair<NamesAndTypesList, SerializationInfoByName>
MergeTreeDataMergerMutator::getColumnsForNewDataPart(
    MergeTreeData::DataPartPtr source_part,
    const Block & updated_header,
    NamesAndTypesList storage_columns,
    const SerializationInfoByName & serialization_infos,
    const MutationCommands & commands_for_removes)
{
    NameSet removed_columns;
    NameToNameMap renamed_columns_to_from;
    NameToNameMap renamed_columns_from_to;
    ColumnsDescription part_columns(source_part->getColumns());

    /// All commands are validated in AlterCommand so we don't care about order
    for (const auto & command : commands_for_removes)
    {
        /// If we don't have this column in source part, than we don't need to materialize it
        if (!part_columns.has(command.column_name))
            continue;

        if (command.type == MutationCommand::DROP_COLUMN)
            removed_columns.insert(command.column_name);

        if (command.type == MutationCommand::RENAME_COLUMN)
        {
            renamed_columns_to_from.emplace(command.rename_to, command.column_name);
            renamed_columns_from_to.emplace(command.column_name, command.rename_to);
        }
    }

    SerializationInfoByName new_serialization_infos;
    for (const auto & [name, info] : serialization_infos)
    {
        if (removed_columns.contains(name))
            continue;

        auto it = renamed_columns_from_to.find(name);
        if (it != renamed_columns_from_to.end())
            new_serialization_infos.emplace(it->second, info);
        else
            new_serialization_infos.emplace(name, info);
    }

    /// In compact parts we read all columns, because they all stored in a
    /// single file
    if (!isWidePart(source_part))
        return {updated_header.getNamesAndTypesList(), new_serialization_infos};

    Names source_column_names = source_part->getColumns().getNames();
    NameSet source_columns_name_set(source_column_names.begin(), source_column_names.end());
    for (auto it = storage_columns.begin(); it != storage_columns.end();)
    {
        if (updated_header.has(it->name))
        {
            auto updated_type = updated_header.getByName(it->name).type;
            if (updated_type != it->type)
                it->type = updated_type;
            ++it;
        }
        else
        {
            if (!source_columns_name_set.contains(it->name))
            {
                /// Source part doesn't have column but some other column
                /// was renamed to it's name.
                auto renamed_it = renamed_columns_to_from.find(it->name);
                if (renamed_it != renamed_columns_to_from.end()
                    && source_columns_name_set.contains(renamed_it->second))
                    ++it;
                else
                    it = storage_columns.erase(it);
            }
            else
            {
                /// Check that this column was renamed to some other name
                bool was_renamed = renamed_columns_from_to.contains(it->name);
                bool was_removed = removed_columns.contains(it->name);

                /// If we want to rename this column to some other name, than it
                /// should it's previous version should be dropped or removed
                if (renamed_columns_to_from.contains(it->name) && !was_renamed && !was_removed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Incorrect mutation commands, trying to rename column {} to {}, but part {} already has column {}", renamed_columns_to_from[it->name], it->name, source_part->name, it->name);

                /// Column was renamed and no other column renamed to it's name
                /// or column is dropped.
                if (!renamed_columns_to_from.contains(it->name) && (was_renamed || was_removed))
                    it = storage_columns.erase(it);
                else
                    ++it;
            }
        }
    }

    return {storage_columns, new_serialization_infos};
}


ExecuteTTLType MergeTreeDataMergerMutator::shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const ColumnDependencies & dependencies)
{
    if (!metadata_snapshot->hasAnyTTL())
        return ExecuteTTLType::NONE;

    bool has_ttl_expression = false;

    for (const auto & dependency : dependencies)
    {
        if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
            has_ttl_expression = true;

        if (dependency.kind == ColumnDependency::TTL_TARGET)
            return ExecuteTTLType::NORMAL;
    }
    return has_ttl_expression ? ExecuteTTLType::RECALCULATE : ExecuteTTLType::NONE;
}


}
