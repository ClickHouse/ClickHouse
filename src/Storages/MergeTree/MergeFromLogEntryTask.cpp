#include <Storages/MergeTree/MergeFromLogEntryTask.h>

#include <base/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace ProfileEvents
{
    extern const Event DataAfterMergeDiffersFromReplica;
    extern const Event ReplicatedPartMerges;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_DATA_PART_NAME;
    extern const int LOGICAL_ERROR;
}


std::pair<bool, ReplicatedMergeMutateTaskBase::PartLogWriter> MergeFromLogEntryTask::prepare()
{
    LOG_TRACE(log, "Executing log entry to merge parts {} to {}",
        fmt::join(entry.source_parts, ", "), entry.new_part_name);

    const auto storage_settings_ptr = storage.getSettings();

    if (storage_settings_ptr->always_fetch_merged_part)
    {
        LOG_INFO(log, "Will fetch part {} because setting 'always_fetch_merged_part' is true", entry.new_part_name);
        return {false, {}};
    }

    if (entry.merge_type == MergeType::TTL_RECOMPRESS &&
        (time(nullptr) - entry.create_time) <= storage_settings_ptr->try_fetch_recompressed_part_timeout.totalSeconds() &&
        entry.source_replica != storage.replica_name)
    {
        LOG_INFO(log, "Will try to fetch part {} until '{}' because this part assigned to recompression merge. "
            "Source replica {} will try to merge this part first", entry.new_part_name,
            DateLUT::instance().timeToString(entry.create_time + storage_settings_ptr->try_fetch_recompressed_part_timeout.totalSeconds()), entry.source_replica);
        return {false, {}};
    }

    /// In some use cases merging can be more expensive than fetching
    /// and it may be better to spread merges tasks across the replicas
    /// instead of doing exactly the same merge cluster-wise
    std::optional<String> replica_to_execute_merge;
    bool replica_to_execute_merge_picked = false;

    if (storage.merge_strategy_picker.shouldMergeOnSingleReplica(entry))
    {
        replica_to_execute_merge = storage.merge_strategy_picker.pickReplicaToExecuteMerge(entry);
        replica_to_execute_merge_picked = true;

        if (replica_to_execute_merge)
        {
            LOG_DEBUG(log,
                "Prefer fetching part {} from replica {} due to execute_merges_on_single_replica_time_threshold",
                entry.new_part_name, replica_to_execute_merge.value());

            return {false, {}};
        }
    }


    for (const String & source_part_name : entry.source_parts)
    {
        MergeTreeData::DataPartPtr source_part_or_covering = storage.getActiveContainingPart(source_part_name);

        if (!source_part_or_covering)
        {
            /// We do not have one of source parts locally, try to take some already merged part from someone.
            LOG_DEBUG(log, "Don't have all parts for merge {}; will try to fetch it instead", entry.new_part_name);
            return {false, {}};
        }

        if (source_part_or_covering->name != source_part_name)
        {
            /// We do not have source part locally, but we have some covering part. Possible options:
            /// 1. We already have merged part (source_part_or_covering->name == new_part_name)
            /// 2. We have some larger merged part which covers new_part_name (and therefore it covers source_part_name too)
            /// 3. We have two intersecting parts, both cover source_part_name. It's logical error.
            /// TODO Why 1 and 2 can happen? Do we need more assertions here or somewhere else?
            constexpr const char * message = "Part {} is covered by {} but should be merged into {}. This shouldn't happen often.";
            LOG_WARNING(log, message, source_part_name, source_part_or_covering->name, entry.new_part_name);
            if (!source_part_or_covering->info.contains(MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version)))
                throw Exception(ErrorCodes::LOGICAL_ERROR, message, source_part_name, source_part_or_covering->name, entry.new_part_name);
            return {false, {}};
        }

        parts.push_back(source_part_or_covering);
    }

    /// All source parts are found locally, we can execute merge

    if (entry.create_time + storage_settings_ptr->prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr))
    {
        /// If entry is old enough, and have enough size, and part are exists in any replica,
        ///  then prefer fetching of merged part from replica.

        size_t sum_parts_bytes_on_disk = 0;
        for (const auto & item : parts)
            sum_parts_bytes_on_disk += item->getBytesOnDisk();

        if (sum_parts_bytes_on_disk >= storage_settings_ptr->prefer_fetch_merged_part_size_threshold)
        {
            String replica = storage.findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
            if (!replica.empty())
            {
                LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
                return {false, {}};
            }
        }
    }

    /// Start to make the main work
    size_t estimated_space_for_merge = MergeTreeDataMergerMutator::estimateNeededDiskSpace(parts);

    /// Can throw an exception while reserving space.
    IMergeTreeDataPart::TTLInfos ttl_infos;
    size_t max_volume_index = 0;
    for (auto & part_ptr : parts)
    {
        ttl_infos.update(part_ptr->ttl_infos);
        max_volume_index = std::max(max_volume_index, storage.getStoragePolicy()->getVolumeIndexByDisk(part_ptr->volume->getDisk()));
    }

    /// It will live until the whole task is being destroyed
    table_lock_holder = storage.lockForShare(RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);

    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();

    auto future_merged_part = std::make_shared<FutureMergedMutatedPart>(parts, entry.new_part_type);
    if (future_merged_part->name != entry.new_part_name)
    {
        throw Exception("Future merged part name " + backQuote(future_merged_part->name) + " differs from part name in log entry: "
            + backQuote(entry.new_part_name), ErrorCodes::BAD_DATA_PART_NAME);
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

    if (storage_settings_ptr->allow_remote_fs_zero_copy_replication)
    {
        if (auto disk = reserved_space->getDisk(); disk->getType() == DB::DiskType::S3)
        {
            if (storage.merge_strategy_picker.shouldMergeOnSingleReplicaShared(entry))
            {
                if (!replica_to_execute_merge_picked)
                    replica_to_execute_merge = storage.merge_strategy_picker.pickReplicaToExecuteMerge(entry);

                if (replica_to_execute_merge)
                {
                    LOG_DEBUG(log,
                        "Prefer fetching part {} from replica {} due s3_execute_merges_on_single_replica_time_threshold",
                        entry.new_part_name, replica_to_execute_merge.value());
                    return {false, {}};
                }
            }
        }
    }

    /// Account TTL merge
    if (isTTLMergeType(future_merged_part->merge_type))
        storage.getContext()->getMergeList().bookMergeWithTTL();

    auto table_id = storage.getStorageID();

    /// Add merge to list
    const Settings & settings = storage.getContext()->getSettingsRef();
    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_merged_part,
        settings.memory_profiler_step,
        settings.memory_profiler_sample_probability,
        settings.max_untracked_memory);

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage);
    stopwatch_ptr = std::make_unique<Stopwatch>();

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
            storage.merging_params);


    /// Adjust priority
    for (auto & item : future_merged_part->parts)
        priority += item->getBytesOnDisk();

    return {true, [this, stopwatch = *stopwatch_ptr] (const ExecutionStatus & execution_status)
    {
        storage.writePartLog(
            PartLogElement::MERGE_PARTS, execution_status, stopwatch.elapsed(),
            entry.new_part_name, part, parts, merge_mutate_entry.get());
    }};
}


bool MergeFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    part = merge_task->getFuture().get();

    /// Task is not needed
    merge_task.reset();

    storage.merger_mutator.renameMergedTemporaryPart(part, parts, transaction_ptr.get());

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

            LOG_ERROR(log,
                "{}. Data after merge is not byte-identical to data on another replicas. There could be several"
                " reasons: 1. Using newer version of compression library after server update. 2. Using another"
                " compression method. 3. Non-deterministic compression algorithm (highly unlikely). 4."
                " Non-deterministic merge algorithm due to logical error in code. 5. Data corruption in memory due"
                " to bug in code. 6. Data corruption in memory due to hardware issue. 7. Manual modification of"
                " source data after server startup. 8. Manual modification of checksums stored in ZooKeeper. 9."
                " Part format related settings like 'enable_mixed_granularity_parts' are different on different"
                " replicas. We will download merged part from replica to force byte-identical result.",
                getCurrentExceptionMessage(false));

            write_part_log(ExecutionStatus::fromCurrentException());

            if (storage.getSettings()->detach_not_byte_identical_parts)
                storage.forgetPartAndMoveToDetached(std::move(part), "merge-not-byte-identical");
            else
                storage.tryRemovePartImmediately(std::move(part));

            /// No need to delete the part from ZK because we can be sure that the commit transaction
            /// didn't go through.

            return false;
        }

        throw;
    }

    /** Removing old parts from ZK and from the disk is delayed - see ReplicatedMergeTreeCleanupThread, clearOldParts.
     */

    /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
     * This is not a problem, because in this case the merge will remain in the queue, and we will try again.
     */
    storage.merge_selecting_task->schedule();
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);

    write_part_log({});

    return true;
}


}
