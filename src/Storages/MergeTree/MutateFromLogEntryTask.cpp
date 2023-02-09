#include <Storages/MergeTree/MutateFromLogEntryTask.h>

#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace ProfileEvents
{
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event ReplicatedPartMutations;
}

namespace DB
{

ReplicatedMergeMutateTaskBase::PrepareResult MutateFromLogEntryTask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);
    const auto storage_settings_ptr = storage.getSettings();
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    MergeTreeData::DataPartPtr source_part = storage.getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for {} is not ready; will try to fetch it instead", source_part_name, entry.new_part_name);
        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = {}
        };
    }

    if (source_part->name != source_part_name)
    {
        LOG_WARNING(log,
            "Part {} is covered by {} but should be mutated to {}. "
            "Possibly the mutation of this part is not needed and will be skipped. "
            "This shouldn't happen often.",
            source_part_name, source_part->name, entry.new_part_name);

        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = {}
        };
    }

    /// TODO - some better heuristic?
    size_t estimated_space_for_result = MergeTreeDataMergerMutator::estimateNeededDiskSpace({source_part});

    if (entry.create_time + storage_settings_ptr->prefer_fetch_merged_part_time_threshold.totalSeconds() <= time(nullptr)
        && estimated_space_for_result >= storage_settings_ptr->prefer_fetch_merged_part_size_threshold)
    {
        /// If entry is old enough, and have enough size, and some replica has the desired part,
        /// then prefer fetching from replica.
        String replica = storage.findReplicaHavingPart(entry.new_part_name, true);    /// NOTE excessive ZK requests for same data later, may remove.
        if (!replica.empty())
        {
            LOG_DEBUG(log, "Prefer to fetch {} from replica {}", entry.new_part_name, replica);
            return PrepareResult{
                .prepared_successfully = false,
                .need_to_check_missing_part_in_fetch = true,
                .part_log_writer = {}
            };
        }
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
                .part_log_writer = {}
            };

        }
    }

    new_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version);
    commands = std::make_shared<MutationCommands>(storage.queue.getMutationCommands(source_part, new_part_info.mutation));

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    reserved_space = storage.reserveSpace(estimated_space_for_result, source_part->data_part_storage);

    table_lock_holder = storage.lockForShare(
            RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage, NO_TRANSACTION_RAW);

    future_mutated_part = std::make_shared<FutureMergedMutatedPart>();
    future_mutated_part->name = entry.new_part_name;
    future_mutated_part->uuid = entry.new_part_uuid;
    future_mutated_part->parts.push_back(source_part);
    future_mutated_part->part_info = new_part_info;
    future_mutated_part->updatePath(storage, reserved_space.get());
    future_mutated_part->type = source_part->getType();

    if (storage_settings_ptr->allow_remote_fs_zero_copy_replication)
    {
        if (auto disk = reserved_space->getDisk(); disk->supportZeroCopyReplication())
        {
            String dummy;
            if (!storage.findReplicaHavingCoveringPart(entry.new_part_name, true, dummy).empty())
            {
                LOG_DEBUG(log, "Mutation of part {} finished by some other replica, will download mutated part", entry.new_part_name);
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = true,
                    .part_log_writer = {}
                };
            }

            zero_copy_lock = storage.tryCreateZeroCopyExclusiveLock(entry.new_part_name, disk);

            if (!zero_copy_lock)
            {
                LOG_DEBUG(log, "Mutation of part {} started by some other replica, will wait it and mutated merged part", entry.new_part_name);
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = false,
                    .part_log_writer = {}
                };
            }
        }
    }


    const Settings & settings = storage.getContext()->getSettingsRef();
    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_mutated_part,
        settings);

    stopwatch_ptr = std::make_unique<Stopwatch>();

    fake_query_context = Context::createCopy(storage.getContext());
    fake_query_context->makeQueryContext();
    fake_query_context->setCurrentQueryId("");

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, merge_mutate_entry.get(),
            entry.create_time, fake_query_context, NO_TRANSACTION_PTR, reserved_space, table_lock_holder);

    /// Adjust priority
    for (auto & item : future_mutated_part->parts)
        priority += item->getBytesOnDisk();

    return {true, true, [this] (const ExecutionStatus & execution_status)
    {
        storage.writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch_ptr->elapsed(),
            entry.new_part_name, new_part, future_mutated_part->parts, merge_mutate_entry.get());
    }};
}


bool MutateFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    new_part = mutate_task->getFuture().get();
    auto builder = mutate_task->getBuilder();

    if (!builder)
        builder = new_part->data_part_storage->getBuilder();

    storage.renameTempPartAndReplace(new_part, *transaction_ptr, builder);

    try
    {
        storage.checkPartChecksumsAndCommit(*transaction_ptr, new_part, mutate_task->getHardlinkedFiles());
    }
    catch (const Exception & e)
    {
        if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
        {
            transaction_ptr->rollback();

            ProfileEvents::increment(ProfileEvents::DataAfterMutationDiffersFromReplica);

            LOG_ERROR(log, "{}. Data after mutation is not byte-identical to data on another replicas. "
                           "We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

            write_part_log(ExecutionStatus::fromCurrentException());

            if (storage.getSettings()->detach_not_byte_identical_parts)
                storage.forgetPartAndMoveToDetached(std::move(new_part), "mutate-not-byte-identical");
            else
                storage.tryRemovePartImmediately(std::move(new_part));

            /// No need to delete the part from ZK because we can be sure that the commit transaction
            /// didn't go through.

            return false;
        }

        throw;
    }

    if (zero_copy_lock)
    {
        LOG_DEBUG(log, "Removing zero-copy lock");
        zero_copy_lock->lock->unlock();
    }

    /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
         * This is not a problem, because in this case the entry will remain in the queue, and we will try again.
         */
    storage.merge_selecting_task->schedule();
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMutations);
    write_part_log({});

    return true;
}


}
