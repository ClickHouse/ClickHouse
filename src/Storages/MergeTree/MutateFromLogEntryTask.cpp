#include <Storages/MergeTree/MutateFromLogEntryTask.h>

#include <Core/BackgroundSchedulePool.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEvents.h>
#include <Common/FailPoint.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/Compaction/CompactionStatistics.h>
#include <Core/Settings.h>

namespace ProfileEvents
{
    extern const Event DataAfterMutationDiffersFromReplica;
    extern const Event MutationCommitMilliseconds;
    extern const Event MutationTotalMilliseconds;
    extern const Event ReplicatedPartMutations;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_ENOUGH_SPACE;
}

namespace Setting
{
    extern const SettingsSeconds receive_timeout;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool always_fetch_mutated_part;
    extern const MergeTreeSettingsBool detach_not_byte_identical_parts;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 prefer_fetch_merged_part_size_threshold;
    extern const MergeTreeSettingsSeconds prefer_fetch_merged_part_time_threshold;
}

namespace FailPoints
{
    extern const char rmt_mutate_task_pause_in_prepare[];
    extern const char rmt_mutate_task_pause_after_zero_copy_lock[];
}

MutateFromLogEntryTask::~MutateFromLogEntryTask()
{
    /// zero_copy_lock's destructor can perform a real ZooKeeper request (releasing the exclusive
    /// lock's ephemeral node) if the task is destroyed while still holding the lock, e.g. on
    /// cancellation before the explicit unlock in prepare()/finalize() is reached. That request
    /// has no component scope by default when this destructor runs from generic background-task
    /// cleanup (MergeTreeBackgroundExecutor::routine), so set one explicitly here.
    auto component_guard = Coordination::setCurrentComponent("MutateFromLogEntryTask::~MutateFromLogEntryTask");
    zero_copy_lock.reset();
}

ReplicatedMergeMutateTaskBase::PrepareResult MutateFromLogEntryTask::prepare()
{
    const String & source_part_name = entry.source_parts.at(0);
    const auto storage_settings_ptr = storage.getSettings();
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    FailPointInjection::pauseFailPoint(FailPoints::rmt_mutate_task_pause_in_prepare);

    new_part_info = MergeTreePartInfo::fromPartName(entry.new_part_name, storage.format_version);

    future_mutated_part = std::make_shared<FutureMergedMutatedPart>();
    future_mutated_part->name = entry.new_part_name;
    future_mutated_part->uuid = entry.new_part_uuid;
    future_mutated_part->part_info = new_part_info;

    stopwatch_ptr = std::make_unique<Stopwatch>();

    auto part_log_writer = [this](const ExecutionStatus & execution_status)
    {
        auto profile_counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(profile_counters.getPartiallyAtomicSnapshot());
        storage.writePartLog(
            PartLogElement::MUTATE_PART, execution_status, stopwatch_ptr->elapsed(),
            entry.new_part_name, new_part, future_mutated_part->parts, merge_mutate_entry.get(), std::move(profile_counters_snapshot),
            mutation_ids_for_log, {});
    };

    /// This mutation was admitted at selection time as one that only hardlinks the files it does not
    /// touch, and the little space it needs was reserved there rather than here (see
    /// ReplicatedMergeTreeQueue::selectEntryToProcess): reserving at selection time lets an entry that
    /// cannot get its space stay queued and be retried, instead of failing the mutation.
    /// Take ownership before any early return below: selected_entry outlives this task, and a
    /// not-prepared result is executed as a fetch in this same thread - a fetch that reserves the
    /// sender's whole part on the disk this reservation would otherwise still be charged to.
    ReservationSharedPtr hardlink_only_reservation = std::move(selected_entry->hardlink_only_reservation);
    const bool hardlink_only = hardlink_only_reservation != nullptr;
    future_mutated_part->hardlink_only = hardlink_only;

    if ((*storage_settings_ptr)[MergeTreeSetting::always_fetch_mutated_part])
    {
        LOG_INFO(log, "Will fetch part {} because setting 'always_fetch_mutated_part' is true", entry.new_part_name);
        /// No replica may have produced the mutated part yet, so a missing part is not an error
        /// here: `executeFetch` must quietly return and let the entry be retried later instead of
        /// throwing `NO_REPLICA_HAS_PART`. Otherwise the exception would be recorded as
        /// `latest_fail_reason` in `system.mutations`, and a synchronous wait
        /// (`mutations_sync` = 1/2) issued on this replica could fail for a long-running mutation
        /// that another replica is still executing.
        /// Because no exception is thrown, the queue's exponential backoff is not armed, so request
        /// a postponed retry explicitly - otherwise the same entry would be re-scheduled immediately
        /// in a loop while another replica is still mutating the part.
        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = false,
            .part_log_writer = part_log_writer,
            .postpone_next_attempt = true,
        };
    }

    MergeTreeData::DataPartPtr source_part = storage.getActiveContainingPart(source_part_name);
    if (!source_part)
    {
        LOG_DEBUG(log, "Source part {} for {} is missing; will try to fetch it instead. "
            "Either pool for fetches is starving, see background_fetches_pool_size, or none of active replicas has it",
            source_part_name, entry.new_part_name);

        return PrepareResult{
            .prepared_successfully = false,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = part_log_writer,
        };
    }

    future_mutated_part->parts.push_back(source_part);
    future_mutated_part->part_format = source_part->getFormat();

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
            .part_log_writer = part_log_writer,
        };
    }

    /// TODO - some better heuristic?
    size_t estimated_space_for_result = CompactionStatistics::estimateNeededDiskSpace({source_part}, false);

    /// Selection reserved on the disk of the part it saw, while the source part was resolved again just
    /// above. A move that passed its own checks before this entry existed commits at an arbitrary later
    /// time (MergeTreePartsMover::swapClonedPart only re-checks that an active part of that name still
    /// exists), so the two disks can differ - and this reservation is what decides the result part's
    /// path, which a hardlink cannot cross.
    /// One read is enough: the source part was resolved once above and is retained, and a later
    /// swapActivePart replaces the indexed part with a new object rather than moving this one
    /// (MergeTreeData::swapActivePart), so both the hardlink source and the result path stay on the
    /// disk this reservation names.
    if (hardlink_only)
    {
        const String reserved_disk_name = hardlink_only_reservation->getDisk()->getName();
        const String source_disk_name = source_part->getDataPartStorage().getDiskName();

        if (reserved_disk_name != source_disk_name)
        {
            /// Throwing rather than returning a not-prepared result: that result is what makes the entry
            /// fetch (ReplicatedMergeMutateTaskBase::executeImpl calls executeFetch unconditionally on
            /// it), and the fetch reserves the sender's whole part on the disk that just refused this
            /// much. Throwing leaves the entry to be retried locally, with the reason in
            /// system.mutations.latest_fail_reason.
            hardlink_only_reservation = MergeTreeData::tryReserveSpace(0, source_part->getDataPartStorage());
            if (!hardlink_only_reservation)
                throw Exception(ErrorCodes::NOT_ENOUGH_SPACE,
                    "Source part {} is on disk {} while this mutation reserved space on disk {}, "
                    "and there is no space to reserve on disk {}",
                    source_part->name, source_disk_name, reserved_disk_name, source_disk_name);

            LOG_DEBUG(log, "Source part {} moved from disk {} to disk {} since this mutation was admitted; "
                "reserved space on the part's current disk instead",
                source_part->name, reserved_disk_name, source_disk_name);
        }
    }

    /// Never divert such a mutation into a fetch either: it is cheap locally, while the fetch reserves
    /// the sender's whole part, which on a full disk can never succeed - the reported bug with extra
    /// steps. This must be its own condition rather than a smaller estimate, because the comparison
    /// below is `>=` and prefer_fetch_merged_part_size_threshold = 0 is a supported way to force fetches.
    if (!hardlink_only
        && entry.create_time + (*storage_settings_ptr)[MergeTreeSetting::prefer_fetch_merged_part_time_threshold].totalSeconds() <= time(nullptr)
        && estimated_space_for_result >= (*storage_settings_ptr)[MergeTreeSetting::prefer_fetch_merged_part_size_threshold])
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
                .part_log_writer = part_log_writer,
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
                .part_log_writer = part_log_writer,
            };

        }
    }

    Strings mutation_ids;
    commands = std::make_shared<MutationCommands>(storage.queue.getMutationCommands(source_part, new_part_info.mutation, mutation_ids));
    LOG_TRACE(log, "Mutating part {} with mutation commands from {} mutations ({}): {}",
              entry.new_part_name, commands->size(), fmt::join(mutation_ids, ", "), commands->toString(true));

    /// mutation_ids can be empty here.
    mutation_ids_for_log = mutation_ids;

    /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
    /// Can throw an exception.
    if (hardlink_only)
        reserved_space = hardlink_only_reservation;
    else
        reserved_space = StorageReplicatedMergeTree::reserveSpace(estimated_space_for_result, source_part->getDataPartStorage());
    future_mutated_part->updatePath(storage, reserved_space.get());

    table_lock_holder = storage.lockForShare(
            RWLockImpl::NO_QUERY, (*storage_settings_ptr)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    const auto metadata_snapshot = storage.getInMemoryMetadataPtr(storage.getContext(), false);

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage, NO_TRANSACTION_RAW);

    if ((*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        if (auto disk = reserved_space->getDisk(); disk->supportZeroCopyReplication())
        {
            if (storage.findReplicaHavingCoveringPart(entry.new_part_name, true))
            {
                LOG_DEBUG(log, "Mutation of part {} finished by some other replica, will download mutated part", entry.new_part_name);
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = true,
                    .part_log_writer = part_log_writer,
                };
            }

            maybeSleepBeforeZeroCopyLock(estimated_space_for_result);
            zero_copy_lock = storage.tryCreateZeroCopyExclusiveLock(entry.new_part_name, disk);

            if (!zero_copy_lock || !zero_copy_lock->isLocked())
            {
                LOG_DEBUG(
                    log,
                    "Mutation of part {} started by some other replica, will wait for it and mutated merged part. Number of tries {}",
                    entry.new_part_name,
                    entry.num_tries);

                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = false,
                    .part_log_writer = part_log_writer,
                };
            }
            if (storage.findReplicaHavingCoveringPart(entry.new_part_name, /* active */ false))
            {
                /// Why this if still needed? We can check for part in zookeeper, don't find it and sleep for any amount of time. During this sleep part will be actually committed from other replica
                /// and exclusive zero copy lock will be released. We will take the lock and execute mutation one more time, while it was possible just to download the part from other replica.
                ///
                /// It's also possible just because reads in [Zoo]Keeper are not lineariazable.
                ///
                /// NOTE: In case of mutation and hardlinks it can even lead to extremely rare dataloss (we will produce new part with the same hardlinks, don't fetch the same from other replica), so this check is important.
                ///
                /// In case of DROP_RANGE on fast replica and stale replica we can have some failed select queries in case of zero copy replication.
                zero_copy_lock->lock->unlock();

                LOG_DEBUG(
                    log,
                    "We took zero copy lock, but mutation of part {} finished by some other replica, will release lock and download "
                    "mutated part to avoid data duplication",
                    entry.new_part_name);
                return PrepareResult{
                    .prepared_successfully = false,
                    .need_to_check_missing_part_in_fetch = true,
                    .part_log_writer = part_log_writer,
                };
            }

            LOG_DEBUG(log, "Zero copy lock taken, will mutate part {}", entry.new_part_name);

            /// Pause here with the zero-copy exclusive lock held, so a test can tear the task
            /// down (e.g. via DROP TABLE) while ~ZooKeeperLock still has to release the lock.
            FailPointInjection::pauseFailPoint(FailPoints::rmt_mutate_task_pause_after_zero_copy_lock);
        }
    }

    task_context = Context::createCopy(storage.getContext()->getBackgroundContext());
    task_context->makeQueryContextForMutate(*storage.getSettings());
    task_context->setCurrentQueryId(getQueryId());

    merge_mutate_entry = storage.getContext()->getMergeList().insert(
        storage.getStorageID(),
        future_mutated_part,
        task_context);

    storage.writePartLog(
        PartLogElement::MUTATE_PART_START, {}, 0,
        entry.new_part_name, new_part, future_mutated_part->parts, merge_mutate_entry.get(), {}, mutation_ids_for_log, {});

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, merge_mutate_entry.get(),
            entry.create_time, task_context, NO_TRANSACTION_PTR, reserved_space, table_lock_holder);

    /// Adjust priority
    for (auto & item : future_mutated_part->parts)
        priority.value += item->getBytesOnDisk();

    return PrepareResult{
        .prepared_successfully = true,
        .need_to_check_missing_part_in_fetch = true,
        .part_log_writer = part_log_writer,
    };
}


bool MutateFromLogEntryTask::finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log)
{
    new_part = mutate_task->getFuture().get();

    auto & data_part_storage = new_part->getDataPartStorage();

#if CLICKHOUSE_CLOUD
    new_part->is_prewarmed = true;
    data_part_storage.setPreferredFileOrder(new_part->getPreferredFileOrder());
#endif

    if (data_part_storage.hasActiveTransaction())
        data_part_storage.precommitTransaction();

    storage.renameTempPartAndReplace(new_part, *transaction_ptr, /*rename_in_transaction=*/ true);
    new_part->getDataPartStorage().commitTransaction();
    /// We must reset the task here, similarly to MergeFromLogEntryTask::finalize.
    /// The task holds RAII guards for temporary part directories (TemporaryParts).
    /// If checkPartChecksumsAndCommit fails with a checksum mismatch, the execution
    /// falls back to fetching the part from another replica. The fetch may use
    /// cloneAndLoadDataPart with the same "tmp_clone_" prefix, which would try to
    /// register the same temporary part name in TemporaryParts — causing a
    /// LOGICAL_ERROR if the old guard is still alive. Resetting the task here
    /// releases these guards before the fallback fetch can run.
    auto hardlinked_files = mutate_task->getHardlinkedFiles();
    mutate_task->updateProfileEvents();
    mutate_task.reset();

    Stopwatch commit_watch;

    try
    {
        transaction_ptr->renameParts();
        storage.checkPartChecksumsAndCommit(*transaction_ptr, new_part, hardlinked_files);
    }
    catch (const Exception & e)
    {
        if (MergeTreeDataPartChecksums::isBadChecksumsErrorCode(e.code()))
        {
            transaction_ptr->rollback();

            UInt64 commit_elapsed_ms = commit_watch.elapsedMilliseconds();
            ProfileEvents::increment(ProfileEvents::MutationCommitMilliseconds, commit_elapsed_ms);
            ProfileEvents::increment(ProfileEvents::MutationTotalMilliseconds, commit_elapsed_ms);
            ProfileEvents::increment(ProfileEvents::DataAfterMutationDiffersFromReplica);

            LOG_ERROR(log, "{}. Data after mutation is not byte-identical to data on another replicas. "
                           "We will download merged part from replica to force byte-identical result.", getCurrentExceptionMessage(false));

            write_part_log(ExecutionStatus::fromCurrentException("", true));

            if ((*storage.getSettings())[MergeTreeSetting::detach_not_byte_identical_parts])
                storage.forcefullyMovePartToDetachedAndRemoveFromMemory(std::move(new_part), "mutate-not-byte-identical");
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
    UInt64 commit_elapsed_ms = commit_watch.elapsedMilliseconds();
    ProfileEvents::increment(ProfileEvents::MutationCommitMilliseconds, commit_elapsed_ms);
    ProfileEvents::increment(ProfileEvents::MutationTotalMilliseconds, commit_elapsed_ms);

    finish_callback = [storage_ptr = &storage]() { storage_ptr->merge_selecting_task->schedule(); };
    ProfileEvents::increment(ProfileEvents::ReplicatedPartMutations);
    write_part_log({});

    return true;
}


}
