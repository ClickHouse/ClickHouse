#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/PartitionActionBlocker.h>
#include <Storages/MutationCommands.h>
#include <atomic>


namespace DB
{


class MutateTask;
using MutateTaskPtr = std::shared_ptr<MutateTask>;


class MergeTreeDataMergerMutator;

struct MutationContext;

class MutateTask
{
public:
    static constexpr auto TEMP_DIRECTORY_PREFIX = "tmp_mut_";

    MutateTask(
        FutureMergedMutatedPartPtr future_part_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommandsConstPtr commands_,
        MergeListEntry * mutate_entry_,
        time_t time_of_mutation_,
        ContextPtr context_,
        ReservationSharedPtr space_reservation_,
        TableLockHolder & table_lock_holder_,
        const MergeTreeTransactionPtr & txn,
        MergeTreeData & data_,
        MergeTreeDataMergerMutator & mutator_,
        PartitionActionBlocker & merges_blocker_,
        bool need_prefix_);

    bool execute();
    void cancel() noexcept;

    void updateProfileEvents() const;

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return promise.get_future();
    }

    const HardlinkedFiles & getHardlinkedFiles() const;

    /// Allow this mutation to keep running (instead of being aborted) while a transient ZooKeeper
    /// session re-establishment is in progress. `is_surviving`/`transient_reconnect`/`shutdown_called`
    /// are storage-owned flags; the mutation compute keeps going while a reconnect is in progress or
    /// after it has detached itself as a survivor, and only aborts on a real shutdown or a KILL.
    /// `survivor_invalidated` is set by the storage when the survivor's target part queue entry is
    /// removed (e.g. by `DROP PARTITION`); once set, the mutation aborts promptly because its result
    /// can no longer be committed.
    void enableSurvivalAcrossTransientReconnect(
        const std::atomic<bool> * is_surviving,
        const std::atomic<bool> * transient_reconnect,
        const std::atomic<bool> * shutdown_called,
        const std::atomic<bool> * survivor_invalidated);

    /// Move out the guard that keeps the temporary part directory alive, so a caller can keep the
    /// finished-but-not-committed part around (e.g. to reuse it after a reconnect) after the task
    /// is destroyed. Must be called only after the result is ready.
    scope_guard releaseTemporaryDirectoryLock();

private:

    bool prepare();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE
    };

    State state{State::NEED_PREPARE};

    std::promise<MergeTreeData::MutableDataPartPtr> promise;

    std::shared_ptr<MutationContext> ctx;
    ExecutableTaskPtr task;

};

[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MutateTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
