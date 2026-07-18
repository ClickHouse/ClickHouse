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
    extern const Event MutationsSurvivedKeeperReconnect;
    extern const Event MutationsReusedPrecomputedParts;
}

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds receive_timeout;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
    extern const MergeTreeSettingsBool detach_not_byte_identical_parts;
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 prefer_fetch_merged_part_size_threshold;
    extern const MergeTreeSettingsSeconds prefer_fetch_merged_part_time_threshold;
    extern const MergeTreeSettingsBool reuse_precomputed_mutations_after_keeper_reconnect;
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
    survival_enabled = (*storage_settings_ptr)[MergeTreeSetting::reuse_precomputed_mutations_after_keeper_reconnect];
    LOG_TRACE(log, "Executing log entry to mutate part {} to {}", source_part_name, entry.new_part_name);

    FailPointInjection::pauseFailPoint(FailPoints::rmt_mutate_task_pause_in_prepare);

    /// If a previous attempt of this mutation survived a transient Keeper reconnection and deposited
    /// its finished result, take it now — before any early return in prepare() — so that whatever
    /// happens next, the deposited temporary part is not stranded on disk. The take is not gated on
    /// the current setting value: a deposit can only exist if the setting was enabled when it was
    /// made, and it must be drained even if the setting has been disabled since. If this attempt
    /// does not end up reusing it (it fetches the part, skips it, hands off to another replica, or
    /// the assignment changed), this local drops its temporary directory lock on scope exit and the
    /// leftover directory is cleaned up as an old temporary directory. The reuse decision below is
    /// fail-closed: the result is reused only if the setting is still enabled and the source part,
    /// the table metadata version, and the exact mutation set are all unchanged, and it still goes
    /// through the normal commit path (which re-validates against ZooKeeper).
    std::optional<StorageReplicatedMergeTree::PreservedMutationPart> preserved
        = storage.takePrecomputedMutation(entry.new_part_name);

    /// The deposited result is taken above, before the reuse decision and the setup below. The
    /// reuse decision is made *before* `reserveSpace` and skips it entirely when the result is
    /// reused, so a `NOT_ENOUGH_SPACE` on a nearly-full disk no longer discards the deposit — that
    /// is exactly the large-part-on-a-full-disk case the feature targets. This guard covers the
    /// remaining window: if a step between here and the reuse commit throws a transient error (the
    /// zero-copy exclusive lock, `MergeList::insert`, ...), put the deposited result back so the
    /// next attempt can still reuse it instead of re-mutating the whole part from scratch.
    /// Deliberate early returns (the part is obsolete, or is being fetched from / computed by
    /// another replica) intentionally let it go: those paths obtain the part another way, and the
    /// deposit is cleaned up when the queue entry is removed. The reuse and discard branches below
    /// consume `preserved` on the non-throwing path, so this guard only fires on an in-flight
    /// exception.
    const int uncaught_exceptions_before = std::uncaught_exceptions();
    scope_guard redeposit_preserved_on_failure = [this, &preserved, uncaught_exceptions_before]()
    {
        if (preserved && std::uncaught_exceptions() > uncaught_exceptions_before)
            storage.depositPrecomputedMutation(entry.new_part_name, std::move(*preserved));
    };

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

    if (entry.create_time + (*storage_settings_ptr)[MergeTreeSetting::prefer_fetch_merged_part_time_threshold].totalSeconds() <= time(nullptr)
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

    table_lock_holder = storage.lockForShare(
            RWLockImpl::NO_QUERY, (*storage_settings_ptr)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
    const auto metadata_snapshot = storage.getInMemoryMetadataPtr(storage.getContext(), false);
    mutation_metadata_version = metadata_snapshot->getMetadataVersion();

    transaction_ptr = std::make_unique<MergeTreeData::Transaction>(storage, NO_TRANSACTION_RAW);

    /// Decide whether the result a survivor deposited across a transient Keeper reconnection can be
    /// reused instead of re-computing the whole part — and decide it *before* reserving disk space
    /// below. The deposited part is already fully written to disk, so committing it needs no fresh
    /// reservation; this matters for exactly the "large part on a nearly full disk" case the feature
    /// targets, where a second `reserveSpace()` would throw `NOT_ENOUGH_SPACE` on every retry and the
    /// deposited result — otherwise ready to commit — would never make it past the reservation.
    ///
    /// The decision is fail-closed: reuse only if the setting is still enabled and the source part,
    /// the table metadata version, the exact mutation set, and the zero-copy commit disposition are
    /// all still unchanged. The last check closes a data-safety gap: if the survivor produced the
    /// part on a zero-copy disk, committing it must recreate its Keeper zero-copy lock nodes, which
    /// `getLockSharedDataOps` only does while `allow_remote_fs_zero_copy_replication` is enabled and
    /// the disk supports zero-copy. If that disposition changed after the deposit (the setting was
    /// toggled either way), reuse would publish the part with the wrong lock metadata, so re-compute.
    const bool zero_copy_commit_now = preserved
        && (*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication]
        && preserved->part->getDataPartStorage().supportZeroCopyReplication();

    const bool reuse_precomputed_result = preserved
        && survival_enabled
        && preserved->source_part_name == source_part_name
        && preserved->metadata_version == mutation_metadata_version
        && preserved->mutation_ids == mutation_ids
        && preserved->requires_zero_copy_commit == zero_copy_commit_now;

    if (preserved && !reuse_precomputed_result)
    {
        LOG_INFO(log, "Discarding the pre-computed result for mutation of part {}: the setting was disabled, or the "
            "source part, the table metadata, the set of mutations, or the zero-copy replication mode changed after "
            "the reconnection, will re-compute it.", entry.new_part_name);
        /// Release the preserved temporary part now, before re-computing below into a possibly
        /// identically-named temporary directory: its lock is dropped and the leftover directory is
        /// cleaned up as an old temporary directory.
        preserved.reset();
    }

    if (reuse_precomputed_result)
    {
        /// The deposited part is already on disk; reuse needs no fresh reservation. Point the
        /// merge-list display at the disk the part already lives on, mirroring updatePath below.
        future_mutated_part->updatePath(
            storage, storage.getStoragePolicy()->getDiskByName(preserved->part->getDataPartStorage().getDiskName()));
    }
    else
    {
        /// Once we mutate part, we must reserve space on the same disk, because mutations can possibly create hardlinks.
        /// Can throw an exception.
        reserved_space = StorageReplicatedMergeTree::reserveSpace(estimated_space_for_result, source_part->getDataPartStorage());
        future_mutated_part->updatePath(storage, reserved_space.get());
    }

    /// Take the zero-copy exclusive lock. On the reuse path the deposited part is already on a
    /// specific disk; on the recompute path it is the freshly reserved disk. Both paths then commit
    /// through `checkPartChecksumsAndCommit`, which recreates the shared-data lock nodes.
    if ((*storage_settings_ptr)[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        DiskPtr disk = reuse_precomputed_result
            ? storage.getStoragePolicy()->getDiskByName(preserved->part->getDataPartStorage().getDiskName())
            : reserved_space->getDisk();

        if (disk->supportZeroCopyReplication())
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

    /// Reuse the result deposited by a survivor of a transient Keeper reconnection instead of
    /// re-computing the whole part. Eligibility (including the fail-closed checks) was decided
    /// above, before the disk reservation, so this branch only adopts the already-validated result.
    if (reuse_precomputed_result)
    {
        LOG_INFO(log, "Reusing the pre-computed result for mutation of part {} that survived a ZooKeeper reconnection.",
            entry.new_part_name);
        new_part = preserved->part;
        reused_hardlinked_files = std::move(preserved->hardlinked_files);
        reused_temporary_directory_lock = std::move(preserved->temporary_directory_lock);
        reused_precomputed_part = true;
        /// Consumed: the temporary-directory lock now lives in `reused_temporary_directory_lock`,
        /// so the re-deposit guard must not put this (moved-from) result back.
        preserved.reset();

        for (auto & item : future_mutated_part->parts)
            priority.value += item->getBytesOnDisk();

        return PrepareResult{
            .prepared_successfully = true,
            .need_to_check_missing_part_in_fetch = true,
            .part_log_writer = part_log_writer,
        };
    }

    mutate_task = storage.merger_mutator.mutatePartToTemporaryPart(
            future_mutated_part, metadata_snapshot, commands, merge_mutate_entry.get(),
            entry.create_time, task_context, NO_TRANSACTION_PTR, reserved_space, table_lock_holder);

    if (survival_enabled)
    {
        mutate_task->enableSurvivalAcrossTransientReconnect(
            &is_surviving_reconnect, &storage.getTransientReconnectFlag(), &storage.getShutdownCalledFlag(),
            &survivor_invalidated);

        /// The mutation is now actually computing into a temporary part, so from here on a transient
        /// reconnect may detach it and keep it running. Publish this only after `mutate_task` and the
        /// survival wiring above are fully set up: `tryDetachForTransientReconnect` reads this flag
        /// (with acquire semantics) to decide eligibility without racing this worker thread.
        ready_to_detach.store(true);
    }

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
    /// The compute phase is over. Close the detach window (under `detach_mutex`) and take a single
    /// consistent reading of whether this task detached itself as a survivor: after this point
    /// `tryDetachForTransientReconnect` must not detach the task, so it does not race the commit
    /// path below (which resets `mutate_task` and releases the temporary-directory guards).
    bool surviving_reconnect = false;
    {
        std::lock_guard lock(detach_mutex);
        ready_to_detach.store(false);
        surviving_reconnect = is_surviving_reconnect.load();
    }

    /// A task that survived a transient reconnection does not commit its result here; it deposits
    /// it so that a follow-up attempt can re-validate and commit it.
    if (surviving_reconnect)
        return depositPrecomputedResultForReuse();

    HardlinkedFiles hardlinked_files;

    if (reused_precomputed_part)
    {
        /// `new_part` was adopted from a previously-computed (deposited) result in prepare(); it is
        /// already finalized on disk, so there is nothing to compute here.
        hardlinked_files = std::move(reused_hardlinked_files);
        ProfileEvents::increment(ProfileEvents::MutationsReusedPrecomputedParts);

        storage.renameTempPartAndReplace(new_part, *transaction_ptr, /*rename_in_transaction=*/ true);
    }
    else
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

        /// We must reset the task here, similarly to MergeFromLogEntryTask::finalize.
        /// The task holds RAII guards for temporary part directories (TemporaryParts).
        /// If checkPartChecksumsAndCommit fails with a checksum mismatch, the execution
        /// falls back to fetching the part from another replica. The fetch may use
        /// cloneAndLoadDataPart with the same "tmp_clone_" prefix, which would try to
        /// register the same temporary part name in TemporaryParts — causing a
        /// LOGICAL_ERROR if the old guard is still alive. Resetting the task here
        /// releases these guards before the fallback fetch can run.
        hardlinked_files = mutate_task->getHardlinkedFiles();
        mutate_task->updateProfileEvents();
        mutate_task.reset();
    }

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


bool MutateFromLogEntryTask::tryDetachForTransientReconnect()
{
    /// This is called by the background executor from the shutdown thread, possibly while a worker
    /// thread is executing this very task. It must be race-free and must never block (it is called
    /// with the executor's mutex held), so it only touches atomics up front and only ever tries to
    /// acquire `detach_mutex`.

    /// This may be called more than once for the same task during a single partial shutdown
    /// (each background assignee removes the storage's tasks from the shared executor). If we
    /// already detached as a survivor, keep saying "yes" so the task is left running. Checked
    /// first, so a survivor that is already depositing its result is never re-entered below.
    if (is_surviving_reconnect.load())
        return true;

    /// Only a task that is actually computing a mutation into a temporary part is eligible. This
    /// gate is published by the worker at the end of prepare() and cleared when finalize() begins,
    /// so a not-yet-prepared task, one reusing a previously-computed result, or one that is already
    /// finishing is not eligible. Reading it here (acquire) does not race the worker.
    if (!ready_to_detach.load())
        return false;

    /// Serialize against finalize()'s commit-or-deposit decision. Never block: if the worker holds
    /// the lock (it is finishing, or publishing state), just decline to survive this time — the
    /// work is re-computed later, which is correct and only a lost optimization.
    std::unique_lock lock(detach_mutex, std::try_to_lock);
    if (!lock.owns_lock())
        return false;

    /// Re-check under the lock: finalize() clears `ready_to_detach` under `detach_mutex`, so if it
    /// is still set here the task is guaranteed to be in the compute phase and `mutate_task` /
    /// `selected_entry` are stable and owned by us for the duration of this call.
    if (!ready_to_detach.load() || is_surviving_reconnect.load())
        return false;

    /// Reserve the target part name so the queue does not schedule a duplicate task for it while we
    /// keep computing. If it is already reserved or deposited, don't try to survive again. The
    /// invalidation flag lets the storage abort our compute if the queue entry is later removed by a
    /// range operation (see `discardPrecomputedMutation`).
    if (!storage.reservePrecomputedMutation(entry.new_part_name, &survivor_invalidated))
        return false;

    survivor_reservation_guard = [this, name = entry.new_part_name]()
    {
        storage.releasePrecomputedMutationReservation(name);
    };

    is_surviving_reconnect.store(true);
    /// The mutation survived the reconnect: it keeps running instead of being cancelled and later
    /// recomputed from scratch. Counted here, at the point of survival, rather than when the result
    /// is finally deposited — the computation may still be aborted before then (e.g. if the target
    /// part is dropped by a concurrent `DROP PARTITION` while this survivor is still computing).
    ProfileEvents::increment(ProfileEvents::MutationsSurvivedKeeperReconnect);

    /// Detach from the replication queue: releasing the "currently executing" holder empties
    /// `future_parts`, so the queue can be reinitialized cleanly during the reconnect. The queue
    /// entry (and its ZooKeeper node) remains; it is re-selected only after we deposit our result
    /// (guarded by the reservation above via isPartBeingComputedBySurvivor()).
    if (selected_entry)
        selected_entry->currently_executing_holder.reset();

    LOG_INFO(log, "Mutation of part {} will keep running to survive a transient ZooKeeper reconnection.",
        entry.new_part_name);

    return true;
}


bool MutateFromLogEntryTask::depositPrecomputedResultForReuse()
{
    new_part = mutate_task->getFuture().get();

    auto & data_part_storage = new_part->getDataPartStorage();

#if CLICKHOUSE_CLOUD
    new_part->is_prewarmed = true;
    data_part_storage.setPreferredFileOrder(new_part->getPreferredFileOrder());
#endif

    if (data_part_storage.hasActiveTransaction())
        data_part_storage.precommitTransaction();

    StorageReplicatedMergeTree::PreservedMutationPart preserved;
    preserved.hardlinked_files = mutate_task->getHardlinkedFiles();
    /// Keep the temporary directory alive so the deposited part is not cleaned up before it is reused.
    preserved.temporary_directory_lock = mutate_task->releaseTemporaryDirectoryLock();
    preserved.part = new_part;
    preserved.source_part_name = entry.source_parts.at(0);
    preserved.metadata_version = mutation_metadata_version;
    preserved.mutation_ids = mutation_ids_for_log;
    /// The survivor held a zero-copy exclusive lock iff it computed the part on a zero-copy disk
    /// under zero-copy replication. Record it (before the lock is released below) so the follow-up
    /// attempt reuses the part only when the same zero-copy commit semantics still apply — see
    /// `PreservedMutationPart::requires_zero_copy_commit`.
    preserved.requires_zero_copy_commit = zero_copy_lock.has_value();

    mutate_task->updateProfileEvents();
    mutate_task.reset();

    /// Release our zero-copy exclusive lock before the follow-up attempt is scheduled. On zero-copy
    /// disks, `MergeTreeBackgroundExecutor::complete_task` reschedules the queue entry (via
    /// `onCompleted`) before this task object is destroyed, so if we kept the lock until
    /// `~MutateFromLogEntryTask` the follow-up attempt would run `prepare()`, take the deposited
    /// part, then fail `tryCreateZeroCopyExclusiveLock` against our own still-held lock and drop the
    /// preserved result on the early return. The deposited part is already finalized on disk (its
    /// temporary directory is kept alive by `temporary_directory_lock`); the follow-up attempt
    /// re-acquires a fresh lock and re-validates against ZooKeeper before committing.
    if (zero_copy_lock)
    {
        LOG_DEBUG(log, "Removing zero-copy lock (the deposited result will be committed by a follow-up attempt)");
        zero_copy_lock->lock->unlock();
        zero_copy_lock.reset();
    }

    storage.depositPrecomputedMutation(entry.new_part_name, std::move(preserved));

    /// The reservation is now represented by the deposited entry; don't release it in the destructor.
    survivor_reservation_guard.release();

    /// Prevent cancel() from deleting the part we just handed off for reuse.
    new_part = nullptr;

    LOG_INFO(log, "Kept the pre-computed result for mutation of part {} after a transient ZooKeeper reconnection; "
        "it will be committed by a follow-up attempt if the assignment is still valid.", entry.new_part_name);

    return true;
}


}
