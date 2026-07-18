#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <atomic>
#include <mutex>

namespace DB
{

class MutateFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{
public:
    template <typename Callback>
    MutateFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
        StorageReplicatedMergeTree & storage_,
        Callback && task_result_callback_)
        : ReplicatedMergeMutateTaskBase(
            getLogger(storage_.getStorageID().getShortName() + "::" + selected_entry_->log_entry->new_part_name + " (MutateFromLogEntryTask)"),
            storage_,
            selected_entry_,
            task_result_callback_)
        {}


    ~MutateFromLogEntryTask() override;

    Priority getPriority() const override { return priority; }

    void cancel() noexcept override
    {
        if (mutate_task)
            mutate_task->cancel();

        if (new_part)
            new_part->removeIfNeeded();
    }

    /// Called by the background executor on a transient ZooKeeper reconnection. If this mutation is
    /// allowed to survive it and has work in progress, detach from the replication queue and keep
    /// running so the already-performed computation is not thrown away.
    bool tryDetachForTransientReconnect() override;

private:

    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;

    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;

    bool executeInnerTask() override
    {
        /// When reusing a previously-computed result there is no computation to run.
        if (reused_precomputed_part)
            return false;
        return mutate_task->execute();
    }

    bool isDetachedSurvivor() const override { return is_surviving_reconnect.load(); }

    /// Deposit the finished-but-not-committed result so it can be reused after the reconnect,
    /// instead of committing it here. Returns true (the base class then leaves the queue entry
    /// in place for the follow-up attempt to pick up).
    bool depositPrecomputedResultForReuse();

    Priority priority;

    TableLockHolder table_lock_holder{nullptr};
    ReservationSharedPtr reserved_space{nullptr};

    MergeTreePartInfo new_part_info;
    MutationCommandsConstPtr commands;
    Strings mutation_ids_for_log;

    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;
    StopwatchUniquePtr stopwatch_ptr{nullptr};

    MergeTreeData::MutableDataPartPtr new_part{nullptr};
    FutureMergedMutatedPartPtr future_mutated_part{nullptr};

    MutateTaskPtr mutate_task;

    /// Support for surviving a transient Keeper reconnection (see
    /// `reuse_precomputed_mutations_after_keeper_reconnect`).
    /// Whether the setting is enabled for this table (captured in prepare()).
    bool survival_enabled{false};

    /// Concurrency for `tryDetachForTransientReconnect`, which is called by the background executor
    /// from the shutdown thread while a worker thread may be executing this task. The worker owns
    /// the "detachable" state (`mutate_task`, `survivor_reservation_guard`,
    /// `selected_entry->currently_executing_holder`, ...) during the compute phase, so the detach
    /// must not touch it concurrently:
    ///   * `ready_to_detach` is set (by the worker) only at the end of `prepare()` once the mutation
    ///     is actually computing, and cleared (by the worker, under `detach_mutex`) as soon as
    ///     `finalize()` begins. It is a fast, race-free gate the shutdown thread can read.
    ///   * `detach_mutex` serializes the detach against `finalize()`'s commit-or-deposit decision.
    ///     `tryDetachForTransientReconnect` only ever `try_lock`s it (it is called with the
    ///     executor's mutex held and must never block); failing to acquire it simply means the task
    ///     is not detached this time and its work is re-computed later — a benign lost optimization.
    std::mutex detach_mutex;
    std::atomic<bool> ready_to_detach{false};

    /// Set when this task detached itself and is computing the result to be reused later.
    std::atomic<bool> is_surviving_reconnect{false};
    /// Metadata version at prepare() time, recorded to re-validate a reused result.
    Int64 mutation_metadata_version{-1};
    /// Releases the reservation of the target part name if the survivor fails before depositing.
    scope_guard survivor_reservation_guard;

    /// Set when prepare() adopted a previously-computed result instead of re-computing it.
    bool reused_precomputed_part{false};
    HardlinkedFiles reused_hardlinked_files;
    scope_guard reused_temporary_directory_lock;
};


}
