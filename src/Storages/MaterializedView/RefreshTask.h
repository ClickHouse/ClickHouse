#pragma once

#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>
#include <Storages/MaterializedView/RefreshSchedule.h>

#include <Processors/Executors/ManualPipelineExecutor.h>

#include <Core/BackgroundSchedulePool.h>

#include <random>


namespace DB
{

class StorageMaterializedView;
class ASTRefreshStrategy;

class RefreshTask : public std::enable_shared_from_this<RefreshTask>
{
public:
    /// Never call it manual, public for shared_ptr construction only
    explicit RefreshTask(const ASTRefreshStrategy & strategy);

    /// The only proper way to construct task
    static RefreshTaskHolder create(
        const StorageMaterializedView & view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy);

    void initializeAndStart(std::shared_ptr<StorageMaterializedView> view);

    /// Call when renaming the materialized view.
    void rename(StorageID new_id);

    /// Call when changing refresh params (ALTER MODIFY REFRESH).
    void alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy);

    RefreshInfo getInfo() const;

    /// Enable task scheduling
    void start();

    /// Disable task scheduling
    void stop();

    /// Schedule task immediately
    void run();

    /// Cancel task execution
    void cancel();

    /// Pause task execution (must be either resumed or canceled later)
    void pause();

    /// Resume task execution
    void resume();

    /// Permanently disable task scheduling and remove this table from RefreshSet.
    void shutdown();

    /// Notify dependent task
    void notify(const StorageID & parent_id, std::chrono::sys_seconds prescribed_time, const RefreshSchedule & parent_schedule);

    void setFakeTime(std::optional<Int64> t);

private:
    Poco::Logger * log = nullptr;
    std::weak_ptr<IStorage> view_to_refresh;

    /// Protects all fields below (they're accessed by both refreshTask() and public methods).
    /// Never locked for blocking operations (e.g. creating or dropping the internal table).
    mutable std::mutex mutex;

    /// Task execution. Non-empty iff a refresh is in progress (possibly paused).
    /// Whoever unsets these should also assign info.last_refresh_result.
    std::optional<ManualPipelineExecutor> refresh_executor;
    std::optional<BlockIO> refresh_block;
    std::shared_ptr<ASTInsertQuery> refresh_query;

    RefreshSchedule refresh_schedule;
    RefreshSet::Handle set_handle;

    /// StorageIDs of our dependencies that we're waiting for.
    DatabaseAndTableNameSet remaining_dependencies;
    bool time_arrived = false;

    /// Refreshes are stopped (e.g. by SYSTEM STOP VIEW).
    bool stop_requested = false;
    /// Refreshes are paused (e.g. by SYSTEM PAUSE VIEW).
    bool pause_requested = false;
    /// Cancel current refresh, then reset this flag.
    bool cancel_requested = false;

    /// If true, we should start a refresh right away. All refreshes go through this flag.
    bool refresh_immediately = false;

    /// If true, the refresh task should interrupt its query execution and reconsider what to do,
    /// re-checking `stop_requested`, `cancel_requested`, etc.
    std::atomic_bool interrupt_execution {false};

    /// When to refresh next. Updated when a refresh is finished or canceled.
    /// We maintain the distinction between:
    ///  * The "prescribed" time of the refresh, dictated by the refresh schedule.
    ///    E.g. for REFERSH EVERY 1 DAY, the prescribed time is always at the exact start of a day.
    ///  * Actual wall clock timestamps, e.g. when the refresh is scheduled to happen
    ///    (including random spread) or when a refresh completed.
    /// The prescribed time is required for:
    ///  * Doing REFRESH EVERY correctly if the random spread came up negative, and a refresh completed
    ///    before the prescribed time. E.g. suppose a refresh was prescribed at 05:00, which was randomly
    ///    adjusted to 4:50, and the refresh completed at 4:55; we shouldn't schedule another refresh
    ///    at 5:00, so we should remember that the 4:50-4:55 refresh actually had prescribed time 5:00.
    ///  * Similarly, for dependencies between REFRESH EVERY tables, using actual time would be unreliable.
    ///    E.g. for REFRESH EVERY 1 DAY, yesterday's refresh of the dependency shouldn't trigger today's
    ///    refresh of the dependent even if it happened today (e.g. it was slow or had random spread > 1 day).
    std::chrono::sys_seconds next_refresh_prescribed;
    std::chrono::system_clock::time_point next_refresh_with_spread;

    /// Calls refreshTask() from background thread.
    BackgroundSchedulePool::TaskHolder refresh_task;

    /// Used in tests. If not INT64_MIN, we pretend that this is the current time, instead of calling system_clock::now().
    std::atomic<Int64> fake_clock {INT64_MIN};

    /// Just for observability.
    RefreshInfo info;
    Progress progress;

    /// The main loop of the refresh task. It examines the state, sees what needs to be
    /// done and does it. If there's nothing to do at the moment, returns; it's then scheduled again,
    /// when needed, by public methods or by timer.
    ///
    /// Public methods just provide inputs for the refreshTask()'s decisions
    /// (e.g. stop_requested, cancel_requested), they don't do anything significant themselves.
    /// This adds some inefficiency: even trivial or no-op requests have to schedule a background
    /// task instead of directly performing the operation; but the simplicity seems worth it, I had
    /// a really hard time trying to organize this code in any other way.
    void refreshTask();

    /// Methods that do the actual work: creating/dropping internal table, executing the query.
    /// Mutex must be unlocked. Called only from refresh_task.
    void initializeRefreshUnlocked(std::shared_ptr<const StorageMaterializedView> view);
    bool executeRefreshUnlocked();
    void completeRefreshUnlocked(std::shared_ptr<StorageMaterializedView> view, LastRefreshResult result, std::chrono::sys_seconds prescribed_time);
    void cancelRefreshUnlocked(LastRefreshResult result);
    void cleanStateUnlocked();

    /// Assigns next_refresh_*
    void advanceNextRefreshTime(std::chrono::system_clock::time_point now);

    /// Returns true if all dependencies are fulfilled now. Refills remaining_dependencies in this case.
    bool arriveDependency(const StorageID & parent);
    bool arriveTime();
    void populateDependencies();

    std::shared_ptr<StorageMaterializedView> lockView();

    std::chrono::system_clock::time_point currentTime() const;
};

}
