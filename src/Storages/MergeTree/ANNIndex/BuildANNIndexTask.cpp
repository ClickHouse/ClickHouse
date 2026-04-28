#include "config.h"
#if USE_DISKANN

#include <Storages/MergeTree/ANNIndex/BuildANNIndexTask.h>

#include <Storages/MergeTree/ANNIndex/ANNIndexGroup.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/StorageMergeTree.h>

#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    /// The `active_group_dir` for a successful build is derived from the tmp directory name
    /// by swapping the `tmp_ann_` prefix for `ann_`. This keeps the UUID identical across the
    /// rename and makes failure-mode debugging straightforward.
    std::string deriveActiveDirFromTmp(const std::string & tmp_dir)
    {
        if (!std::string_view(tmp_dir).starts_with(ANN_GROUP_TMP_PREFIX))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "BuildANNIndexTask: temp group dir `{}` does not start with `{}`",
                tmp_dir, ANN_GROUP_TMP_PREFIX);
        return std::string(ANN_GROUP_ACTIVE_PREFIX)
             + tmp_dir.substr(ANN_GROUP_TMP_PREFIX.size());
    }
}


namespace
{
    UInt64 totalRowsOf(const std::vector<DataPartPtr> & parts)
    {
        UInt64 total = 0;
        for (const auto & p : parts)
        {
            if (p)
                total += p->rows_count;
        }
        return total;
    }
}


BuildANNIndexTask::BuildANNIndexTask(
    StorageMergeTree & storage_,
    ANNBuildSelectedEntryPtr entry_,
    ANNIndexManager * manager_,
    TableLockHolder table_lock_holder_,
    IExecutableTask::TaskResultCallback task_result_callback_,
    std::optional<ANNIndexManager::BuildReservation> reservation_)
    : storage(storage_)
    , entry(std::move(entry_))
    , manager(manager_)
    , table_lock_holder(std::move(table_lock_holder_))
    , task_result_callback(std::move(task_result_callback_))
    , reservation(std::move(reservation_))
    , log(getLogger("BuildANNIndexTask"))
{
    if (!manager)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildANNIndexTask: manager must not be null");
    if (!entry)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildANNIndexTask: entry must not be null");
    if (entry->selected_parts.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildANNIndexTask: entry has no selected parts");
    if (!entry->storage_snapshot)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildANNIndexTask: entry.storage_snapshot must not be null");

    /// Rough priority estimate: a larger build fetches more data, so should run earlier if
    /// the scheduler ever needs to pick between two pending ANN builds.
    UInt64 bytes = 0;
    for (const auto & p : entry->selected_parts)
    {
        if (p)
            bytes += p->getBytesOnDisk();
    }
    priority.value = static_cast<Int64>(bytes);
}

BuildANNIndexTask::~BuildANNIndexTask() = default;

StorageID BuildANNIndexTask::getStorageID() const
{
    return storage.getStorageID();
}

String BuildANNIndexTask::getQueryId() const
{
    return storage.getStorageID().getShortName() + "::ann_build";
}


bool BuildANNIndexTask::executeStep()
{
    try
    {
        switch (state)
        {
            case State::NEED_PREPARE:
                prepare();
                state = State::NEED_EXECUTE;
                return true;
            case State::NEED_EXECUTE:
                if (builder->execute())
                    return true;
                state = State::NEED_FINISH;
                return true;
            case State::NEED_FINISH:
                finish();
                state = State::SUCCESS;
                return false;
            case State::SUCCESS:
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "BuildANNIndexTask: executeStep called after SUCCESS");
        }
    }
    catch (...)
    {
        /// Drop the builder (and with it any open file handles) on failure. The reservation
        /// stays alive on `this` and will roll back (release slot + clear in-flight
        /// registration) when the task is destroyed; the on-disk `tmp_ann_<uuid>/` then
        /// becomes a true orphan eligible for the next cleanup pass.
        builder.reset();
        state = State::SUCCESS;
        throw;
    }
    return false;
}


void BuildANNIndexTask::prepare()
{
    /// Shape / column re-check. Catches `ALTER MODIFY INDEX` races.
    const auto & definition = entry->definition;
    const auto & manager_shape = manager->getShape();
    if (!(definition.shape == manager_shape))
        throw Exception(ErrorCodes::ABORTED,
            "ANN build aborted: index shape changed between scheduling and execution "
            "(definition dim={} / params_hash={}, manager dim={} / params_hash={})",
            definition.shape.dim, definition.shape.params_hash,
            manager_shape.dim, manager_shape.params_hash);

    const auto & metadata = entry->storage_snapshot->metadata;
    if (!metadata || !metadata->getColumns().has(definition.vector_column_name))
        throw Exception(ErrorCodes::ABORTED,
            "ANN build aborted: vector column `{}` no longer exists in metadata",
            definition.vector_column_name);

    stopwatch = std::make_unique<Stopwatch>();
    stopwatch->restart();

    /// Safety net for the `executeHere` path: tests / `SYSTEM BUILD ANN INDEX` may construct
    /// the task without a prior reservation. Acquire one now so the rest of the body has a
    /// consistent invariant (`reservation` is always set).
    if (!reservation)
    {
        reservation = manager->tryReserveBuildSlot();
        if (!reservation)
            throw Exception(ErrorCodes::ABORTED,
                "ANN build aborted: another build is already in flight for this table");
    }

    builder = createANNIndexBuilder(entry, *manager, reservation->tmpDir());

    LOG_DEBUG(log,
        "ANN build prepare: table={}, parts={}, rows={}, tmp_dir={}",
        storage.getStorageID().getNameForLogs(),
        entry->selected_parts.size(), totalRowsOf(entry->selected_parts),
        reservation->tmpDir());
}


void BuildANNIndexTask::finish()
{
    /// The algorithm-specific work has already finished: `builder->getResult()` returns a
    /// fully-loaded in-memory group whose storage handle still points at `tmp_ann_<uuid>/`.
    /// Now perform the atomic publish: disk rename (throwing), then in-memory publish
    /// (swallowed on failure because the on-disk state is already correct and a subsequent
    /// server restart's `loadFromDisk` will converge).
    auto group = builder->getResult();

    const std::string tmp_dir = group->getGroupDir();
    const std::string active_dir = deriveActiveDirFromTmp(tmp_dir);

    /// On-disk atomic publish through the manager's single FS-touching helper.
    manager->renameGroupDir(tmp_dir, active_dir);

    /// In-memory publish. Failures here are logged and swallowed: the disk side has already
    /// been committed, and a restart's scan would pick up the new `ann_<uuid>/` directory.
    try
    {
        group->rebindStorage(manager->openGroupStorage(active_dir));
        manager->registerGroup(group);
        const UInt64 elapsed_ms = stopwatch ? stopwatch->elapsedMilliseconds() : 0;
        LOG_INFO(log,
            "ANN group `{}` published for table `{}` in {} ms",
            active_dir,
            storage.getStorageID().getNameForLogs(),
            elapsed_ms);
    }
    catch (...)
    {
        tryLogCurrentException(log,
            "BuildANNIndexTask::finish: in-memory publish failed after disk rename; "
            "state will reconcile on next `ANNIndexManager::loadFromDisk`");
    }

    /// Atomic publish complete. Release the slot + clear the in-flight registration. From
    /// here on the directory is in `active_dir` form and `isPathKnown` reports true via the
    /// active snapshot — there is no window where cleanup could mistake it for an orphan.
    if (reservation)
        reservation->commit();

    /// The builder has served its purpose; drop it now so any held file handles are released
    /// before the task destructor runs.
    builder.reset();

    build_successful = true;
}


void BuildANNIndexTask::cancel() noexcept
{
    /// Drop the builder early so any open files are released before further teardown. The
    /// reservation (if still held) will roll back automatically when the task is destroyed,
    /// at which point any leftover `tmp_ann_<uuid>/` becomes a legitimate orphan.
    try
    {
        builder.reset();
    }
    catch (...)
    {
        tryLogCurrentException(log, "BuildANNIndexTask::cancel: error while discarding builder");
    }

    state = State::SUCCESS;
}


void BuildANNIndexTask::onCompleted()
{
    const bool has_error = !build_successful;
    if (task_result_callback)
        task_result_callback(has_error);
}


void BuildANNIndexTask::executeHere(std::shared_ptr<BuildANNIndexTask> task)
{
    if (!task)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "BuildANNIndexTask::executeHere: null task");
    while (task->executeStep())
    {
    }
}


}

#endif
