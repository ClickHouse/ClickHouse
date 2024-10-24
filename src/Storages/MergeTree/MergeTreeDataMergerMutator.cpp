#include "MergeTreeDataMergerMutator.h"

#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/MergeSelectors/AllMergeSelector.h>
#include <Storages/MergeTree/MergeSelectors/TTLMergeSelector.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>

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
#include <base/interpolate.h>
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

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_max_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_bytes_to_merge_at_min_space_in_pool;
    extern const MergeTreeSettingsUInt64 max_number_of_mutations_for_replica;
    extern const MergeTreeSettingsUInt64 max_parts_to_merge_at_once;
    extern const MergeTreeSettingsInt64 merge_with_recompression_ttl_timeout;
    extern const MergeTreeSettingsInt64 merge_with_ttl_timeout;
    extern const MergeTreeSettingsUInt64 merge_selector_blurry_base_scale_factor;
    extern const MergeTreeSettingsUInt64 merge_selector_window_size;
    extern const MergeTreeSettingsBool min_age_to_force_merge_on_partition_only;
    extern const MergeTreeSettingsUInt64 min_age_to_force_merge_seconds;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_optimize_entire_partition;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_execute_mutation;
    extern const MergeTreeSettingsUInt64 number_of_free_entries_in_pool_to_lower_max_size_of_merge;
    extern const MergeTreeSettingsBool ttl_only_drop_parts;
    extern const MergeTreeSettingsUInt64 parts_to_throw_insert;
    extern const MergeTreeSettingsMergeSelectorAlgorithm merge_selector_algorithm;
}

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

MergeTreeDataMergerMutator::MergeTreeDataMergerMutator(MergeTreeData & data_)
    : data(data_), log(getLogger(data.getLogName() + " (MergerMutator)"))
{
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge() const
{
    size_t scheduled_tasks_count = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    return getMaxSourcePartsSizeForMerge(max_tasks_count, scheduled_tasks_count);
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartsSizeForMerge(size_t max_count, size_t scheduled_tasks_count) const
{
    if (scheduled_tasks_count > max_count)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Invalid argument passed to getMaxSourcePartsSize: scheduled_tasks_count = {} > max_count = {}",
            scheduled_tasks_count, max_count);
    }

    size_t free_entries = max_count - scheduled_tasks_count;
    const auto data_settings = data.getSettings();

    /// Always allow maximum size if one or less pool entries is busy.
    /// One entry is probably the entry where this function is executed.
    /// This will protect from bad settings.
    UInt64 max_size = 0;
    if (scheduled_tasks_count <= 1 || free_entries >= (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_lower_max_size_of_merge])
        max_size = (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool];
    else
        max_size = static_cast<UInt64>(interpolateExponential(
            (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_min_space_in_pool],
            (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool],
            static_cast<double>(free_entries) / (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_lower_max_size_of_merge]));

    return std::min(max_size, static_cast<UInt64>(data.getStoragePolicy()->getMaxUnreservedFreeSpace() / DISK_USAGE_COEFFICIENT_TO_SELECT));
}


UInt64 MergeTreeDataMergerMutator::getMaxSourcePartSizeForMutation() const
{
    const auto data_settings = data.getSettings();
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);

    if ((*data_settings)[MergeTreeSetting::max_number_of_mutations_for_replica] > 0 &&
        occupied >= (*data_settings)[MergeTreeSetting::max_number_of_mutations_for_replica])
        return 0;

    /// A DataPart can be stored only at a single disk. Get the maximum reservable free space at all disks.
    UInt64 disk_space = data.getStoragePolicy()->getMaxUnreservedFreeSpace();
    auto max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();

    /// Allow mutations only if there are enough threads, otherwise, leave free threads for merges.
    if (occupied <= 1
        || max_tasks_count - occupied >= (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_execute_mutation])
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
    PreformattedMessage & out_disable_reason,
    const PartitionIdsHint * partitions_hint)
{
    MergeTreeData::DataPartsVector data_parts = getDataPartsToSelectMergeFrom(txn, partitions_hint);

    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    if (data_parts.empty())
    {
        out_disable_reason = PreformattedMessage::create("There are no parts in the table");
        return SelectPartsDecision::CANNOT_SELECT;
    }

    MergeSelectingInfo info = getPossibleMergeRanges(data_parts, can_merge_callback, txn, out_disable_reason);

    if (info.parts_selected_precondition == 0)
    {
        out_disable_reason = PreformattedMessage::create("No parts satisfy preconditions for merge");
        return SelectPartsDecision::CANNOT_SELECT;
    }

    auto res = selectPartsToMergeFromRanges(future_part, aggressive, max_total_size_to_merge, merge_with_ttl_allowed,
                                            metadata_snapshot, info.parts_ranges, info.current_time, out_disable_reason);

    if (res == SelectPartsDecision::SELECTED)
        return res;

    String best_partition_id_to_optimize = getBestPartitionToOptimizeEntire(info.partitions_info);
    if (!best_partition_id_to_optimize.empty())
    {
        return selectAllPartsToMergeWithinPartition(
            future_part,
            can_merge_callback,
            best_partition_id_to_optimize,
            /*final=*/true,
            metadata_snapshot,
            txn,
            out_disable_reason,
            /*optimize_skip_merged_partitions=*/true);
    }

    if (!out_disable_reason.text.empty())
        out_disable_reason.text += ". ";
    out_disable_reason.text += "There is no need to merge parts according to merge selector algorithm";
    return SelectPartsDecision::CANNOT_SELECT;
}

MergeTreeDataMergerMutator::PartitionIdsHint MergeTreeDataMergerMutator::getPartitionsThatMayBeMerged(
    size_t max_total_size_to_merge,
    const AllowedMergingPredicate & can_merge_callback,
    bool merge_with_ttl_allowed,
    const MergeTreeTransactionPtr & txn) const
{
    PartitionIdsHint res;
    MergeTreeData::DataPartsVector data_parts = getDataPartsToSelectMergeFrom(txn);
    if (data_parts.empty())
        return res;

    auto metadata_snapshot = data.getInMemoryMetadataPtr();

    PreformattedMessage out_reason;
    MergeSelectingInfo info = getPossibleMergeRanges(data_parts, can_merge_callback, txn, out_reason);

    if (info.parts_selected_precondition == 0)
        return res;

    Strings all_partition_ids;
    std::vector<IMergeSelector::PartsRanges> ranges_per_partition;

    String curr_partition;
    for (auto & range : info.parts_ranges)
    {
        if (range.empty())
            continue;
        const String & partition_id = range.front().getDataPartPtr()->info.partition_id;
        if (partition_id != curr_partition)
        {
            curr_partition = partition_id;
            all_partition_ids.push_back(curr_partition);
            ranges_per_partition.emplace_back();
        }
        ranges_per_partition.back().emplace_back(std::move(range));
    }

    for (size_t i = 0; i < all_partition_ids.size(); ++i)
    {
        auto future_part = std::make_shared<FutureMergedMutatedPart>();
        PreformattedMessage out_disable_reason;
        /// This method should have been const, but something went wrong... it's const with dry_run = true
        auto status = const_cast<MergeTreeDataMergerMutator *>(this)->selectPartsToMergeFromRanges(
                future_part, /*aggressive*/ false, max_total_size_to_merge, merge_with_ttl_allowed,
                metadata_snapshot, ranges_per_partition[i], info.current_time, out_disable_reason,
                /* dry_run */ true);
        if (status == SelectPartsDecision::SELECTED)
            res.insert(all_partition_ids[i]);
        else
            LOG_TEST(log, "Nothing to merge in partition {}: {}", all_partition_ids[i], out_disable_reason.text);
    }

    String best_partition_id_to_optimize = getBestPartitionToOptimizeEntire(info.partitions_info);
    if (!best_partition_id_to_optimize.empty())
        res.emplace(std::move(best_partition_id_to_optimize));

    LOG_TRACE(log, "Checked {} partitions, found {} partitions with parts that may be merged: [{}] "
              "(max_total_size_to_merge={}, merge_with_ttl_allowed={})",
              all_partition_ids.size(), res.size(), fmt::join(res, ", "), max_total_size_to_merge, merge_with_ttl_allowed);
    return res;
}

MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::getDataPartsToSelectMergeFrom(
    const MergeTreeTransactionPtr & txn, const PartitionIdsHint * partitions_hint) const
{
    auto res = getDataPartsToSelectMergeFrom(txn);
    if (!partitions_hint)
        return res;

    std::erase_if(res, [partitions_hint](const auto & part)
    {
        return !partitions_hint->contains(part->info.partition_id);
    });
    return res;
}

MergeTreeData::DataPartsVector MergeTreeDataMergerMutator::getDataPartsToSelectMergeFrom(const MergeTreeTransactionPtr & txn) const
{
    MergeTreeData::DataPartsVector res;
    if (!txn)
    {
        /// Simply get all active parts
        return data.getDataPartsVectorForInternalUsage();
    }

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

    std::erase_if(active_parts, remove_pred);

    std::erase_if(outdated_parts, remove_pred);

    MergeTreeData::DataPartsVector data_parts;
    std::merge(
        active_parts.begin(),
        active_parts.end(),
        outdated_parts.begin(),
        outdated_parts.end(),
        std::back_inserter(data_parts),
        MergeTreeData::LessDataPart());

    return data_parts;
}

MergeTreeDataMergerMutator::MergeSelectingInfo MergeTreeDataMergerMutator::getPossibleMergeRanges(
    const MergeTreeData::DataPartsVector & data_parts,
    const AllowedMergingPredicate & can_merge_callback,
    const MergeTreeTransactionPtr & txn,
    PreformattedMessage & out_disable_reason) const
{
    MergeSelectingInfo res;

    res.current_time = std::time(nullptr);

    IMergeSelector::PartsRanges & parts_ranges = res.parts_ranges;

    StoragePolicyPtr storage_policy = data.getStoragePolicy();
    /// Volumes with stopped merges are extremely rare situation.
    /// Check it once and don't check each part (this is bad for performance).
    bool has_volumes_with_disabled_merges = storage_policy->hasAnyVolumeWithDisabledMerges();

    const String * prev_partition_id = nullptr;
    /// Previous part only in boundaries of partition frame
    const MergeTreeData::DataPartPtr * prev_part = nullptr;

    /// collect min_age for each partition while iterating parts
    PartitionsInfo & partitions_info = res.partitions_info;

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
            if (!can_merge_callback(nullptr, part, txn.get(), out_disable_reason))
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
            if (!can_merge_callback(*prev_part, part, txn.get(), out_disable_reason))
            {
                /// Now we have no previous part
                prev_part = nullptr;

                /// Mustn't be empty
                assert(!parts_ranges.back().empty());

                /// Some parts cannot be merged with previous parts and also cannot be merged with themselves,
                /// for example, merge is already assigned for such parts, or they participate in quorum inserts
                /// and so on.
                /// Also we don't start new interval here (maybe all next parts cannot be merged and we don't want to have empty interval)
                if (!can_merge_callback(nullptr, part, txn.get(), out_disable_reason))
                    continue;

                /// Starting new interval in the same partition
                parts_ranges.emplace_back();
            }
        }

        IMergeSelector::Part part_info;
        part_info.size = part->getExistingBytesOnDisk();
        part_info.age = res.current_time - part->modification_time;
        part_info.level = part->info.level;
        part_info.data = &part;
        part_info.ttl_infos = &part->ttl_infos;
        part_info.compression_codec_desc = part->default_codec->getFullCodecDesc();
        part_info.shall_participate_in_merges = has_volumes_with_disabled_merges ? part->shallParticipateInMerges(storage_policy) : true;

        auto & partition_info = partitions_info[partition_id];
        partition_info.min_age = std::min(partition_info.min_age, part_info.age);

        ++res.parts_selected_precondition;

        parts_ranges.back().emplace_back(part_info);

        /// Check for consistency of data parts. If assertion is failed, it requires immediate investigation.
        if (prev_part)
        {
            if (part->info.contains((*prev_part)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} contains previous part {}", part->name, (*prev_part)->name);

            if (!part->info.isDisjoint((*prev_part)->info))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} intersects previous part {}", part->name, (*prev_part)->name);
        }

        prev_part = &part;
    }

    return res;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectPartsToMergeFromRanges(
    FutureMergedMutatedPartPtr future_part,
    bool aggressive,
    size_t max_total_size_to_merge,
    bool merge_with_ttl_allowed,
    const StorageMetadataPtr & metadata_snapshot,
    const IMergeSelector::PartsRanges & parts_ranges,
    const time_t & current_time,
    PreformattedMessage & out_disable_reason,
    bool dry_run)
{
    const auto data_settings = data.getSettings();
    IMergeSelector::PartsRange parts_to_merge;

    if (metadata_snapshot->hasAnyTTL() && merge_with_ttl_allowed && !ttl_merges_blocker.isCancelled())
    {
        TTLDeleteMergeSelector::Params params_drop
        {
            .merge_due_times = next_delete_ttl_merge_times_by_partition,
            .current_time = current_time,
            .merge_cooldown_time = (*data_settings)[MergeTreeSetting::merge_with_ttl_timeout],
            .only_drop_parts = true,
            .dry_run = dry_run
        };

        /// TTL delete is preferred to recompression
        TTLDeleteMergeSelector drop_ttl_selector(params_drop);

        /// The size of the completely expired part of TTL drop is not affected by the merge pressure and the size of the storage space
        parts_to_merge = drop_ttl_selector.select(parts_ranges, (*data_settings)[MergeTreeSetting::max_bytes_to_merge_at_max_space_in_pool]);
        if (!parts_to_merge.empty())
        {
            future_part->merge_type = MergeType::TTLDelete;
        }
        else if (!(*data_settings)[MergeTreeSetting::ttl_only_drop_parts])
        {
            TTLDeleteMergeSelector::Params params_delete
            {
                .merge_due_times = next_delete_ttl_merge_times_by_partition,
                .current_time = current_time,
                .merge_cooldown_time = (*data_settings)[MergeTreeSetting::merge_with_ttl_timeout],
                .only_drop_parts = false,
                .dry_run = dry_run
            };
            TTLDeleteMergeSelector delete_ttl_selector(params_delete);

            parts_to_merge = delete_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part->merge_type = MergeType::TTLDelete;
        }

        if (parts_to_merge.empty() && metadata_snapshot->hasAnyRecompressionTTL())
        {
            TTLRecompressMergeSelector::Params params
            {
                .merge_due_times = next_recompress_ttl_merge_times_by_partition,
                .current_time = current_time,
                .merge_cooldown_time = (*data_settings)[MergeTreeSetting::merge_with_recompression_ttl_timeout],
                .recompression_ttls = metadata_snapshot->getRecompressionTTLs(),
                .dry_run = dry_run,
            };

            TTLRecompressMergeSelector recompress_ttl_selector(params);

            parts_to_merge = recompress_ttl_selector.select(parts_ranges, max_total_size_to_merge);
            if (!parts_to_merge.empty())
                future_part->merge_type = MergeType::TTLRecompress;
        }
    }

    if (parts_to_merge.empty())
    {
        auto merge_selector_algorithm = (*data_settings)[MergeTreeSetting::merge_selector_algorithm];

        std::any merge_settings;
        if (merge_selector_algorithm == MergeSelectorAlgorithm::SIMPLE
            || merge_selector_algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
        {
            SimpleMergeSelector::Settings simple_merge_settings;
            /// Override value from table settings
            simple_merge_settings.window_size = (*data_settings)[MergeTreeSetting::merge_selector_window_size];
            simple_merge_settings.max_parts_to_merge_at_once = (*data_settings)[MergeTreeSetting::max_parts_to_merge_at_once];
            if (!(*data_settings)[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
                simple_merge_settings.min_age_to_force_merge = (*data_settings)[MergeTreeSetting::min_age_to_force_merge_seconds];

            if (aggressive)
                simple_merge_settings.base = 1;

            if (merge_selector_algorithm == MergeSelectorAlgorithm::STOCHASTIC_SIMPLE)
            {
                simple_merge_settings.parts_to_throw_insert = (*data_settings)[MergeTreeSetting::parts_to_throw_insert];
                simple_merge_settings.blurry_base_scale_factor = (*data_settings)[MergeTreeSetting::merge_selector_blurry_base_scale_factor];
                simple_merge_settings.use_blurry_base = simple_merge_settings.blurry_base_scale_factor != 0;
                simple_merge_settings.enable_stochastic_sliding = true;
            }

            merge_settings = simple_merge_settings;
        }

        parts_to_merge = MergeSelectorFactory::instance().get(merge_selector_algorithm, merge_settings)->select(parts_ranges, max_total_size_to_merge);

        /// Do not allow to "merge" part with itself for regular merges, unless it is a TTL-merge where it is ok to remove some values with expired ttl
        if (parts_to_merge.size() == 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Merge selector returned only one part to merge");

        if (parts_to_merge.empty())
        {
            out_disable_reason = PreformattedMessage::create("Did not find any parts to merge (with usual merge selectors)");
            return SelectPartsDecision::CANNOT_SELECT;
        }
    }

    MergeTreeData::DataPartsVector parts;
    parts.reserve(parts_to_merge.size());
    for (IMergeSelector::Part & part_info : parts_to_merge)
    {
        const MergeTreeData::DataPartPtr & part = part_info.getDataPartPtr();
        parts.push_back(part);
    }

    LOG_DEBUG(log, "Selected {} parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
    future_part->assign(std::move(parts));
    return SelectPartsDecision::SELECTED;
}

String MergeTreeDataMergerMutator::getBestPartitionToOptimizeEntire(
    const PartitionsInfo & partitions_info) const
{
    const auto & data_settings = data.getSettings();
    if (!(*data_settings)[MergeTreeSetting::min_age_to_force_merge_on_partition_only])
        return {};
    if (!(*data_settings)[MergeTreeSetting::min_age_to_force_merge_seconds])
        return {};
    size_t occupied = CurrentMetrics::values[CurrentMetrics::BackgroundMergesAndMutationsPoolTask].load(std::memory_order_relaxed);
    size_t max_tasks_count = data.getContext()->getMergeMutateExecutor()->getMaxTasksCount();
    if (occupied > 1 && max_tasks_count - occupied < (*data_settings)[MergeTreeSetting::number_of_free_entries_in_pool_to_execute_optimize_entire_partition])
    {
        LOG_INFO(
            log,
            "Not enough idle threads to execute optimizing entire partition. See settings "
            "'number_of_free_entries_in_pool_to_execute_optimize_entire_partition' "
            "and 'background_pool_size'");
        return {};
    }

    auto best_partition_it = std::max_element(
        partitions_info.begin(),
        partitions_info.end(),
        [](const auto & e1, const auto & e2) { return e1.second.min_age < e2.second.min_age; });

    assert(best_partition_it != partitions_info.end());

    if (static_cast<size_t>(best_partition_it->second.min_age) < (*data_settings)[MergeTreeSetting::min_age_to_force_merge_seconds])
        return {};

    return best_partition_it->first;
}

SelectPartsDecision MergeTreeDataMergerMutator::selectAllPartsToMergeWithinPartition(
    FutureMergedMutatedPartPtr future_part,
    const AllowedMergingPredicate & can_merge,
    const String & partition_id,
    bool final,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeTransactionPtr & txn,
    PreformattedMessage & out_disable_reason,
    bool optimize_skip_merged_partitions)
{
    MergeTreeData::DataPartsVector parts = selectAllPartsFromPartition(partition_id);

    if (parts.empty())
    {
        out_disable_reason = PreformattedMessage::create("There are no parts inside partition");
        return SelectPartsDecision::CANNOT_SELECT;
    }

    if (!final && parts.size() == 1)
    {
        out_disable_reason = PreformattedMessage::create("There is only one part inside partition");
        return SelectPartsDecision::CANNOT_SELECT;
    }

    /// If final, optimize_skip_merged_partitions is true and we have only one part in partition with level > 0
    /// than we don't select it to merge. But if there are some expired TTL then merge is needed
    if (final && optimize_skip_merged_partitions && parts.size() == 1 && parts[0]->info.level > 0 &&
        (!metadata_snapshot->hasAnyTTL() || parts[0]->checkAllTTLCalculated(metadata_snapshot)))
    {
        out_disable_reason = PreformattedMessage::create("Partition skipped due to optimize_skip_merged_partitions");
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

        sum_bytes += (*it)->getExistingBytesOnDisk();

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

        out_disable_reason = PreformattedMessage::create("Insufficient available disk space, required {}", ReadableSize(required_disk_space));
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
        need_prefix
    );
}


MergeTreeData::DataPartPtr MergeTreeDataMergerMutator::renameMergedTemporaryPart(
    MergeTreeData::MutableDataPartPtr & new_data_part,
    const MergeTreeData::DataPartsVector & parts,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData::Transaction & out_transaction)
{
    /// Some of source parts was possibly created in transaction, so non-transactional merge may break isolation.
    if (data.transactions_enabled.load(std::memory_order_relaxed) && !txn)
        throw Exception(ErrorCodes::ABORTED, "Cancelling merge, because it was done without starting transaction,"
                                             "but transactions were enabled for this table");

    /// Rename new part, add to the set and remove original parts.
    auto replaced_parts = data.renameTempPartAndReplace(new_data_part, out_transaction, /*rename_in_transaction=*/ true);

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
        LOG_WARNING(log, "Unexpected number of parts removed when adding {}: {} instead of {}\n"
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
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected part removed when adding {}: {} instead of {}",
                    new_data_part->name, replaced_parts[i]->name, parts[i]->name);
    }

    LOG_TRACE(log, "Merged {} parts: [{}, {}] -> {}", parts.size(), parts.front()->name, parts.back()->name, new_data_part->name);
    return new_data_part;
}


size_t MergeTreeDataMergerMutator::estimateNeededDiskSpace(const MergeTreeData::DataPartsVector & source_parts, const bool & account_for_deleted)
{
    size_t res = 0;
    time_t current_time = std::time(nullptr);
    for (const MergeTreeData::DataPartPtr & part : source_parts)
    {
        /// Exclude expired parts
        time_t part_max_ttl = part->ttl_infos.part_max_ttl;
        if (part_max_ttl && part_max_ttl <= current_time)
            continue;

        if (account_for_deleted)
            res += part->getExistingBytesOnDisk();
        else
            res += part->getBytesOnDisk();
    }

    return static_cast<size_t>(res * DISK_USAGE_COEFFICIENT_TO_RESERVE);
}

}
