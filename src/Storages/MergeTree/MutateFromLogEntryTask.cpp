#include <Storages/MergeTree/MutateFromLogEntryTask.h>

#include <base/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace ProfileEvents
{
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event ReplicatedPartMutations;
}

namespace DB
{

std::pair<bool, ReplicatedMergeMutateTaskBase::PartLogWriter> MutateFromLogEntryTask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);
    const auto storage_settings_ptr = storage.getSettings();
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    MergeTreeData::DataPartPtr source_part = storage.getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for {} is not ready; will try to fetch it instead", source_part_name, entry.new_part_name);
        return {false, {}};
    }

    if (source_part->name != source_part_name)
    {
        LOG_WARNING(log, "Part " + source_part_name + " is covered by " + source_part->name
                    + " but should be mutated to " + entry.new_part_name + ". "
                    + "Possibly the mutation of this part is not needed and will be skipped. This shouldn't happen often.");
        return {false, {}};
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
            return {false, {}};
        }
    }

    new_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version);
    commands = MutationCommands::create(storage.queue.getMutationCommands(source_part, new_part_info.mutation));

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    reserved_space = storage.reserveSpace(estimated_space_for_result, source_part->volume);

    table_lock_holder = storage.lockForShare(
            RWLockImpl::NO_QUERY, storage_settings_ptr->lock_acquire_timeout_for_background_operations);
    StorageMetadataPtr metadata_snapshot = storage.getInMemoryMetadataPtr();

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage);

    future_mutated_part = std::make_shared<FutureMergedMutatedPart>();
    future_mutated_part->name = entry.new_part_name;
    future_mutated_part->uuid = entry.new_part_uuid;
    future_mutated_part->parts.push_back(source_part);
    future_mutated_part->part_info = new_part_info;
    future_mutated_part->updatePath(storage, reserved_space.get());
    future_mutated_part->type = source_part->getType();

    const Settings & settings = storage.getContext()->getSettingsRef();
    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_mutated_part,
        settings.memory_profiler_step,
        settings.memory_profiler_sample_probability,
        settings.max_untracked_memory);

    stopwatch_ptr = std::make_unique<Stopwatch>();

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, merge_mutate_entry.get(),
            entry.create_time, storage.getContext(), reserved_space, table_lock_holder);

    /// Adjust priority
    for (auto & item : future_mutated_part->parts)
        priority += item->getBytesOnDisk();

    return {true, [this] (const ExecutionStatus & execution_status)
    {
        storage.writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch_ptr->elapsed(),
            entry.new_part_name, new_part, future_mutated_part->parts, merge_mutate_entry.get());
    }};
}


bool MutateFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    new_part = mutate_task->getFuture().get();

    storage.renameTempPartAndReplace(new_part, nullptr, transaction_ptr.get());

    try
    {
        storage.checkPartChecksumsAndCommit(*transaction_ptr, new_part);
    }
    catch (const Exception & e)
    {
        if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
        {
            transaction_ptr->rollback();

            ProfileEvents::increment(ProfileEvents::DataAfterMutationDiffersFromReplica);

            LOG_ERROR(log, "{}. Data after mutation is not byte-identical to data on another replicas. We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

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

    /** With `ZSESSIONEXPIRED` or `ZOPERATIONTIMEOUT`, we can inadvertently roll back local changes to the parts.
         * This is not a problem, because in this case the entry will remain in the queue, and we will try again.
         */
    storage.merge_selecting_task->schedule();
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMutations);
    write_part_log({});

    return true;
}


}
