#pragma once

#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/ANNIndexManager.h>
#include <Storages/MergeTree/ANNIndex/IANNIndexBuilder.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTreeIndexANN.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/TableLockHolder.h>

#include <Common/Logger.h>
#include <Common/Stopwatch.h>

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>


namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
class StorageMergeTree;


/// Background framework task that turns a snapshot of unindexed parts into a new ANN index
/// group and publishes it through `ANNIndexManager`.
///
/// Deliberately thin: the task orchestrates a four-state machine (modelled on
/// `MergePlainMergeTreeTask`) and delegates all algorithm-specific work to an
/// `IANNIndexBuilder` instance obtained from `createANNIndexBuilder`.
///
///   `NEED_PREPARE` — shape re-check, idempotent build-slot reservation, construct the
///                    algorithm-specific builder. Non-yielding.
///   `NEED_EXECUTE` — drive `builder->execute()`; the builder is free to yield between
///                    its internal stages, and each yield releases the executor slot.
///   `NEED_FINISH`  — atomic publish: rename `tmp_ann_<uuid>/` to `ann_<uuid>/` via the
///                    manager, rebind the group's storage handle, register the group in
///                    the manager's active snapshot, and release the build slot.
///   `SUCCESS`      — terminal; further `executeStep` calls throw.
///
/// Cancellation / exception paths drop the `BuildReservation` (RAII), which releases the
/// build slot and clears the in-flight registration. Any leftover `tmp_ann_<uuid>/` on disk
/// is then swept by the retired-group orphan-cleanup pass on the next tick (legitimately,
/// since the manager no longer claims it).
class BuildANNIndexTask final : public IExecutableTask
{
public:
    /// `reservation` carries the build slot AND the `tmp_ann_<uuid>` registration. If
    /// `nullopt` is passed (currently only `executeHere` does this without a prior
    /// reservation), `prepare()` will try to acquire one itself and throw `ABORTED` if the
    /// slot is already taken.
    BuildANNIndexTask(
        StorageMergeTree & storage_,
        ANNBuildSelectedEntryPtr entry_,
        ANNIndexManager * manager_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback task_result_callback_,
        std::optional<ANNIndexManager::BuildReservation> reservation_ = std::nullopt);

    ~BuildANNIndexTask() override;

    BuildANNIndexTask(const BuildANNIndexTask &) = delete;
    BuildANNIndexTask & operator=(const BuildANNIndexTask &) = delete;

    bool executeStep() override;
    void onCompleted() override;
    void cancel() noexcept override;

    StorageID getStorageID() const override;
    String getQueryId() const override;
    Priority getPriority() const override { return priority; }

    /// Drive the state machine inline to completion (or throw). Used by `SYSTEM BUILD ANN INDEX`
    /// and unit tests to avoid going through the background pool.
    static void executeHere(std::shared_ptr<BuildANNIndexTask> task);

private:
    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,
        SUCCESS,
    };

    void prepare();
    void finish();

    /// Machine state.
    State state{State::NEED_PREPARE};

    /// Inputs.
    StorageMergeTree & storage;
    ANNBuildSelectedEntryPtr entry;
    ANNIndexManager * manager;
    TableLockHolder table_lock_holder;
    IExecutableTask::TaskResultCallback task_result_callback;

    /// Owns the build slot + the `tmp_ann_<uuid>` in-flight registration for the entire
    /// duration of the task. `commit()` is called only on the happy path inside `finish()`;
    /// destruction (cancel / exception / shutdown) auto-rolls-back.
    std::optional<ANNIndexManager::BuildReservation> reservation;

    /// Algorithm-specific worker. Constructed in `prepare`, consumed in `finish`.
    std::unique_ptr<IANNIndexBuilder> builder;

    Priority priority;
    /// Set to `true` only by `finish` on the happy path; consumed by `onCompleted` to decide
    /// whether this run should be reported as a success to the scheduler callback.
    bool build_successful = false;
    LoggerPtr log;
    std::unique_ptr<Stopwatch> stopwatch;
};

using BuildANNIndexTaskPtr = std::shared_ptr<BuildANNIndexTask>;

}

#endif
