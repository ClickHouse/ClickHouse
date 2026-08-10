#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/PartitionActionBlocker.h>
#include <Storages/MutationCommands.h>


namespace DB
{


class MutateTask;
using MutateTaskPtr = std::shared_ptr<MutateTask>;


class MergeTreeDataMergerMutator;

struct MutationContext;

namespace MutationHelpers
{

/// True iff the shape of `part` alone forces mutating it with `commands` to rewrite the whole part
/// instead of hardlinking the files it does not touch. Shared by splitAndModifyMutationCommands, by
/// rewritesAllPartColumns (which adds the interpreter-dependent condition) and by
/// isHardlinkOnlyMutation below, so the shape conditions cannot drift apart between them.
bool mutationRequiresFullPartRewrite(const MergeTreeData::DataPartPtr & part, const MutationCommands & commands);

/// True iff mutating `part` with `commands` is guaranteed to take the partial route (hardlink every
/// untouched file, unlink the dropped ones) AND to write none of the data-copying outputs that route
/// can otherwise produce. Such a mutation needs almost no free space, so the selection guards may
/// admit it for a part larger than the free-space budget. Conservative: any doubt means false.
/// Answers about the state as of the call: the copy-mode setting and the storage's patch parts can both
/// change afterwards, so a caller that admits a mutation on this answer must re-validate on the write
/// side, which MutateTask does.
/// Locking: reads `data`'s active patch parts, so it acquires `data`'s data-parts read lock itself. Safe
/// to call while ReplicatedMergeTreeQueue's state_mutex is held, which is the order that critical
/// section already uses (ReplicatedMergeTreeQueue.cpp:1774 takes the same lock through getPartIfExists).
/// Must NOT be called with the data-parts lock already held.
/// `has_pending_rename` says whether a rename outside `commands` still applies to `part`; the caller
/// answers it, because only the caller can query its own mutation bookkeeping.
bool isHardlinkOnlyMutation(
    const MergeTreeData & data,
    const MergeTreeData::DataPartPtr & part,
    const MutationCommands & commands,
    bool has_pending_rename);

}

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
        PartitionActionBlocker & merges_blocker_);

    bool execute();
    void cancel() noexcept;

    void updateProfileEvents() const;

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return promise.get_future();
    }

    const HardlinkedFiles & getHardlinkedFiles() const;

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
