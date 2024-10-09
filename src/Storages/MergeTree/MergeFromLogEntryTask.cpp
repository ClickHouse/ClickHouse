#include <Storages/MergeTree/MergeFromLogEntryTask.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/ProfileEventsScope.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <pcg_random.hpp>
#include <Common/randomSeed.h>
#include <cmath>

namespace ProfileEvents
{
    extern const Event DataAfterMergeDiffersFromReplica;
    extern const Event ReplicatedPartMerges;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool always_fetch_merged_part;
    extern const MergeTreeSettingsBool detach_not_byte_identical_parts;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 prefer_fetch_merged_part_size_threshold;
    extern const MergeTreeSettingsSeconds prefer_fetch_merged_part_time_threshold;
    extern const MergeTreeSettingsSeconds try_fetch_recompressed_part_timeout;
    extern const MergeTreeSettingsUInt64 zero_copy_merge_mutation_min_parts_size_sleep_before_lock;
}

namespace ErrorCodes
{
    extern const int BAD_DATA_PART_NAME;
    extern const int LOGICAL_ERROR;
}

MergeFromLogEntryTask::MergeFromLogEntryTask(
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
    StorageReplicatedMergeTree & storage_,
    IExecutableTask::TaskResultCallback & task_result_callback_)
    : ReplicatedMergeMutateTaskBase(
        getLogger(
            storage_.getStorageID().getShortName() + "::" + selected_entry_->log_entry->new_part_name + " (MergeFromLogEntryTask)"),
        storage_,
        selected_entry_,
        task_result_callback_)
    , rng(randomSeed())
{
}


ReplicatedMergeMutateTaskBase::PrepareResult MergeFromLogEntryTask::prepare()
{
    LOG_TRACE(log, "Executing log entry to merge parts {} to {}",
        fmt::join(entry.source_parts, ", "), entry.new_part_name);

    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();
    int32_t metadata_version = metadata_snapshot->getMetadataVersion();
    const auto storage_settings_ptr = storage.getSettings();

    stopwatch_ptr = std::make_unique<Stopwatch>();
    auto part_log_writer = [this, stopwatch = *stopwatch_ptr](const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MERGE_PARTS, execution_status, stopwatch.elapsed(),
            entry.new_part_name, part, parts, merge_mutate_entry.get(), std::move(profile_counters_snapshot));
    };

    if ((*storage_settings_ptr)[MergeTreeSetting::always_fetch_merged_part])
    {
        LOG_INFO(log, "Will fetch part {} because setting 'always_fetch_merged_part' is true", entry.new_part_name);
        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = part_log_writer,
        };
    }

    if (entry.merge_type == MergeType::TTLRecompress &&
        (time(nullptr) - entry.create_time) <= (*storage_settings_ptr)[MergeTreeSetting::try_fetch_recompressed_part_timeout].totalSeconds() &&
        entry.source_replica != storage.replica_name)
    {
        LOG_INFO(log, "Will try to fetch part {} until '{}' because this part assigned to recompression merge. "
            "Source replica {} will try to merge this part first", entry.new_part_name,
            DateLUT::serverTimezoneInstance().timeToString(entry.create_time + (*storage_settings_ptr)[MergeTreeSetting::try_fetch_recompressed_part_timeout].totalSeconds()), entry.source_replica);
            /// Waiting other replica to recompress part. No need to check it.
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = false,
                .part_log_writer = part_log_writer,
            };
    }

    /// In some use cases merging can be more expensive than fetching
    /// and it may be better to spread merges tasks across the replicas
    /// instead of doing exactly the same merge cluster-wise

    if (storage.merge_strategy_picker.shouldMergeOnSingleReplica(entry))
    {
        std::optional<String> replica_to_execute_merge = storage.merge_strategy_picker.pickReplicaToExecuteMerge(entry);
        if (replica_to_execute_merge)
        {
            LOG_DEBUG(log,
                "Prefer fetching part {} from replica {} due to execute_merges_on_single_replica_time_threshold",
                entry.new_part_name, replica_to_execute_merge.value());

            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = part_log_writer,
            };
        }
    }


    for (const String & source_part_name : entry.source_parts)
    {
        MergeTreeData::DataPartPtr source_part_or_covering = storage.getActiveContainingPart(source_part_name);

        if (!source_part_or_covering)
        {
            /// We do not have one of source parts locally, try to take some already merged part from someone.
            LOG_DEBUG(log, "Don't have all parts (at least {} is missing) for merge {}; will try to fetch it instead. "
                "Either pool for fetches is starving, see background_fetches_pool_size, or none of active replicas has it",
               source_part_name, entry.new_part_name);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = part_log_writer,
            };
        }

        if (source_part_or_covering->name != source_part_name)
        {
            /// We do not have source part locally, but we have some covering part. Possible options:
            /// 1. We already have merged part (source_part_or_covering->name == new_part_name)
            /// 2. We have some larger merged part which covers new_part_name (and therefore it covers source_part_name too)
            /// 3. We have two intersecting parts, both cover source_part_name. It's logical error.
            /// TODO Why 1 and 2 can happen? Do we need more assertions here or somewhere else?
            constexpr auto fmt_string = "Part {} is covered by {} but should be merged into {}. This shouldn't happen often.";
            String message;
            LOG_WARNING(LogToStr(message, log), fmt_string, source_part_name, source_part_or_covering->name, entry.new_part_name);
            if (!source_part_or_covering->info.contains(MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version)))
                throw Exception::createDeprecated(message, ErrorCodes::LOGICAL_ERROR);

            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = part_log_writer,
            };
        }

        int32_t part_metadata_version = source_part_or_covering->getMetadataVersion();
        if (part_metadata_version > metadata_version)
        {
            LOG_DEBUG(log, "Source part metadata version {} is newer then the table metadata version {}. ALTER_METADATA is still in progress.",
                part_metadata_version, metadata_version);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = false,
                .part_log_writer = part_log_writer,
            };
        }

        parts.push_back(source_part_or_covering);
    }

    /// All source parts are found locally, we can execute merge

    if (entry.create_time + (*storage_settings_ptr)[MergeTreeSetting::prefer_fetch_merged_part_time_threshold].totalSeconds() <= time(nullptr))
    {
        /// If entry is old enough, and have enough size, and part are exists in any replica,
        ///  then prefer fetching of merged part from replica.

        size_t sum_parts_bytes_on_disk = 0;
        for (const auto & item : parts)
            sum_parts_bytes_on_disk += item->getBytesOnDisk();

        if (sum_parts_bytes_on_disk >= (*storage_settings_ptr)[MergeTreeSetting::prefer_fetch_merged_part_size_threshold])
        {
            String replica = storage.findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
            if (!replica.empty())
            {
                LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
                /// We found covering part, no checks for missing part.
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = false,
                    .part_log_writer = part_log_writer,
                };
            }
        }
    }

    /// Start to make the main work
    size_t estimated_space_for_merge = MergeTreeDataMergerMutator::estimateNeededDiskSpace(parts, true);

    /// Can throw an exception while reserving space.
    IMergeTreeDataPart::TTLInfos ttl_infos;
    size_t max_volume_index = 0;
    for (auto & part_ptr : parts)
    {
        ttl_infos.update(part_ptr->ttl_infos);
        auto disk_name = part_ptr->getDataPartStorage().getDiskName();
        size_t volume_index = storage.getStoragePolicy()->getVolumeIndexByDiskName(disk_name);
        max_volume_index = std::max(max_volume_index, volume_index);
    }

    /// It will live until the whole task is being destroyed
    table_lock_holder = storage.lockForShare(RWLockImpl::NO_QUERY, (*storage_settings_ptr)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);

    auto future_merged_part = std::make_shared<FutureMergedMutatedPart>(parts, entry.new_part_format);
    if (future_merged_part->name != entry.new_part_name)
    {
        throw Exception(ErrorCodes::BAD_DATA_PART_NAME, "Future merged part name {} differs from part name in log entry: {}",
            backQuote(future_merged_part->name), backQuote(entry.new_part_name));
    }

    std::optional<CurrentlySubmergingEmergingTagger> tagger;
    ReservationSharedPtr reserved_space = storage.balancedReservation(
        metadata_snapshot,
        estimated_space_for_merge,
        max_volume_index,
        future_merged_part->name,
        future_merged_part->part_info,
        future_merged_part->parts,
        &tagger,
        &ttl_infos);

    if (!reserved_space)
        reserved_space = storage.reserveSpacePreferringTTLRules(
            metadata_snapshot, estimated_space_for_merge, ttl_infos, time(nullptr), max_volume_index);

    future_merged_part->uuid = entry.new_part_uuid;
    future_merged_part->updatePath(storage, reserved_space.get());
    future_merged_part->merge_type = entry.merge_type;

    if ((*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        if (auto disk = reserved_space->getDisk(); disk->supportZeroCopyReplication())
        {
            if (storage.findReplicaHavingCoveringPart(entry.new_part_name, true))
            {
                LOG_DEBUG(log, "Merge of part {} finished by some other replica, will fetch merged part", entry.new_part_name);
                /// We found covering part, no checks for missing part.
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = false,
                    .part_log_writer = part_log_writer,
                };
            }

            if ((*storage_settings_ptr)[MergeTreeSetting::zero_copy_merge_mutation_min_parts_size_sleep_before_lock] != 0 &&
                estimated_space_for_merge >= (*storage_settings_ptr)[MergeTreeSetting::zero_copy_merge_mutation_min_parts_size_sleep_before_lock])
            {
                /// In zero copy replication only one replica execute merge/mutation, others just download merged parts metadata.
                /// Here we are trying to mitigate the skew of merges execution because of faster/slower replicas.
                /// Replicas can be slow because of different reasons like bigger latency for ZooKeeper or just slight step behind because of bigger queue.
                /// In this case faster replica can pick up all merges execution, especially large merges while other replicas can just idle. And even in this case
                /// the fast replica is not overloaded because amount of executing merges doesn't affect the ability to acquire locks for new merges.
                ///
                /// So here we trying to solve it with the simplest solution -- sleep random time up to 500ms for 1GB part and up to 7 seconds for 300GB part.
                /// It can sound too much, but we are trying to acquire these locks in background tasks which can be scheduled each 5 seconds or so.
                double start_to_sleep_seconds = std::logf((*storage_settings_ptr)[MergeTreeSetting::zero_copy_merge_mutation_min_parts_size_sleep_before_lock].value);
                uint64_t right_border_to_sleep_ms = static_cast<uint64_t>((std::log(estimated_space_for_merge) - start_to_sleep_seconds + 0.5) * 1000);
                uint64_t time_to_sleep_milliseconds = std::min<uint64_t>(10000UL, std::uniform_int_distribution<uint64_t>(1, 1 + right_border_to_sleep_ms)(rng));

                LOG_INFO(log, "Merge size is {} bytes (it's more than sleep threshold {}) so will intentionally sleep for {} ms to allow other replicas to took this big merge",
                    estimated_space_for_merge, (*storage_settings_ptr)[MergeTreeSetting::zero_copy_merge_mutation_min_parts_size_sleep_before_lock], time_to_sleep_milliseconds);

                std::this_thread::sleep_for(std::chrono::milliseconds(time_to_sleep_milliseconds));
            }

            zero_copy_lock = storage.tryCreateZeroCopyExclusiveLock(entry.new_part_name, disk);

            if (!zero_copy_lock || !zero_copy_lock->isLocked())
            {
                LOG_DEBUG(
                    log,
                    "Merge of part {} started by some other replica, will wait for it and fetch merged part. Number of tries {}",
                    entry.new_part_name,
                    entry.num_tries);
                storage.watchZeroCopyLock(entry.new_part_name, disk);
                /// Don't check for missing part -- it's missing because other replica still not
                /// finished merge.
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = false,
                    .part_log_writer = part_log_writer,
                };
            }
            if (storage.findReplicaHavingCoveringPart(entry.new_part_name, /* active */ false))
            {
                /// Why this if still needed? We can check for part in zookeeper, don't find it and sleep for any amount of time. During this sleep part will be actually committed from other replica
                /// and exclusive zero copy lock will be released. We will take the lock and execute merge one more time, while it was possible just to download the part from other replica.
                ///
                /// It's also possible just because reads in [Zoo]Keeper are not lineariazable.
                ///
                /// NOTE: In case of mutation and hardlinks it can even lead to extremely rare dataloss (we will produce new part with the same hardlinks, don't fetch the same from other replica), so this check is important.
                zero_copy_lock->lock->unlock();

                LOG_DEBUG(
                    log,
                    "We took zero copy lock, but merge of part {} finished by some other replica, will release lock and download merged "
                    "part to avoid data duplication",
                    entry.new_part_name);
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = true,
                    .part_log_writer = part_log_writer,
                };
            }

            LOG_DEBUG(log, "Zero copy lock taken, will merge part {}", entry.new_part_name);
        }
    }

    /// Account TTL merge
    if (isTTLMergeType(future_merged_part->merge_type))
        storage.getContext()->getMergeList().bookMergeWithTTL();

    auto table_id = storage.getStorageID();

    task_context = Context::createCopy(storage.getContext());
    task_context->makeQueryContextForMerge(*storage.getSettings());
    task_context->setCurrentQueryId(getQueryId());
    task_context->setBackgroundOperationTypeForContext(ClientInfo::BackgroundOperationType::MERGE);

    /// Add merge to list
    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_merged_part,
        task_context);

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage, NO_TRANSACTION_RAW);

    merge_task = storage.merger_mutator.mergePartsToTemporaryPart(
            future_merged_part,
            metadata_snapshot,
            merge_mutate_entry.get(),
            {} /* projection_merge_list_element */,
            table_lock_holder,
            entry.create_time,
            storage.getContext(),
            reserved_space,
            entry.deduplicate,
            entry.deduplicate_by_columns,
            entry.cleanup,
            storage.merging_params,
            NO_TRANSACTION_PTR);


    /// Adjust priority
    for (auto & item : future_merged_part->parts)
        priority.value += item->getBytesOnDisk();

    return PrepareResult{
        .prepared_successfully = true,
        .need_to_check_missing_part_in_fetch = true,
        .part_log_writer = part_log_writer,
    };
}


bool MergeFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    part = merge_task->getFuture().get();

    storage.merger_mutator.renameMergedTemporaryPart(part, parts, NO_TRANSACTION_PTR, *transaction_ptr);
    /// Why we reset task here? Because it holds shared pointer to part and tryRemovePartImmediately will
    /// not able to remove the part and will throw an exception (because someone holds the pointer).
    ///
    /// Why we cannot reset task right after obtaining part from getFuture()? Because it holds RAII wrapper for
    /// temp directories which guards temporary dir from background removal. So it's right place to reset the task
    /// and it's really needed.
    merge_task.reset();

    try
    {
        storage.checkPartChecksumsAndCommit(*transaction_ptr, part);
    }
    catch (const Exception & e)
    {
        if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
        {
            transaction_ptr->rollback();

            ProfileEvents::increment(ProfileEvents::DataAfterMergeDiffersFromReplica);

            Strings files_with_size;
            for (const auto & file : part->getFilesChecksums())
            {
                files_with_size.push_back(fmt::format("{}: {} ({})",
                    file.first, file.second.file_size, getHexUIntLowercase(file.second.file_hash)));
            }

            LOG_ERROR(log,
                "{}. Data after merge is not byte-identical to data on another replicas. There could be several reasons:"
                " 1. Using newer version of compression library after server update."
                " 2. Using another compression method."
                " 3. Non-deterministic compression algorithm (highly unlikely)."
                " 4. Non-deterministic merge algorithm due to logical error in code."
                " 5. Data corruption in memory due to bug in code."
                " 6. Data corruption in memory due to hardware issue."
                " 7. Manual modification of source data after server startup."
                " 8. Manual modification of checksums stored in ZooKeeper."
                " 9. Part format related settings like 'enable_mixed_granularity_parts' are different on different replicas."
                " We will download merged part from replica to force byte-identical result."
                " List of files in local parts:\n{}",
                getCurrentExceptionMessage(false),
                fmt::join(files_with_size, "\n"));

            write_part_log(ExecutionStatus::fromCurrentException("", true));

            if ((*storage.getSettings())[MergeTreeSetting::detach_not_byte_identical_parts])
                storage.forcefullyMovePartToDetachedAndRemoveFromMemory(std::move(part), "merge-not-byte-identical");
            else
                storage.tryRemovePartImmediately(std::move(part));

            /// No need to delete the part from ZK because we can be sure that the commit transaction
            /// didn't go through.

            return false;
        }

        throw;
    }

    if (zero_copy_lock)
        zero_copy_lock->lock->unlock();

    /** Removing old parts from ZK and from the disk is delayed - see ReplicatedMergeTreeCleanupThread, clearOldParts.
     */

    /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
     * This is not a problem, because in this case the merge will remain in the queue, and we will try again.
     */
    finish_callback = [storage_ptr = &storage]() { storage_ptr->merge_selecting_task->schedule(); };
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);

    write_part_log({});
    StorageReplicatedMergeTree::incrementMergedPartsProfileEvent(part->getType());

    return true;
}


}
