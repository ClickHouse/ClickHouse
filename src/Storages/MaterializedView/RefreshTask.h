#pragma once

#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>
#include <Storages/MaterializedView/RefreshSchedule.h>
#include <Storages/MaterializedView/RefreshSettings.h>

#include <Core/BackgroundSchedulePool.h>

#include <random>


namespace DB
{

class PipelineExecutor;

class StorageMaterializedView;
class ASTRefreshStrategy;

class RefreshTask : public std::enable_shared_from_this<RefreshTask>
{
public:
    /// Never call it manually, public for shared_ptr construction only
    RefreshTask(StorageMaterializedView * view_, const ASTRefreshStrategy & strategy);

    /// The only proper way to construct task
    static RefreshTaskHolder create(
        StorageMaterializedView * view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy);

    void initializeAndStart();

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

    /// Waits for the currently running refresh attempt to complete.
    /// If the refresh fails, throws an exception.
    /// If no refresh is running, completes immediately, throwing an exception if previous refresh failed.
    void wait();

    /// Permanently disable task scheduling and remove this table from RefreshSet.
    void shutdown();

    /// Notify dependent task
    void notify(const StorageID & parent_id, std::chrono::sys_seconds parent_next_prescribed_time);

    /// For tests
    void setFakeTime(std::optional<Int64> t);

private:
    LoggerPtr log = nullptr;
    StorageMaterializedView * view;

    /// Protects interrupt_execution and running_executor.
    /// Can be locked while holding `mutex`.
    std::mutex executor_mutex;
    /// If there's a refresh in progress, it can be aborted by setting this flag and cancel()ling
    /// this executor. Refresh task will then reconsider what to do, re-checking `stop_requested`,
    /// `cancel_requested`, etc.
    std::atomic_bool interrupt_execution {false};
    PipelineExecutor * running_executor = nullptr;

    /// Protects all fields below.
    /// Never locked for blocking operations (e.g. creating or dropping the internal table).
    /// Can't be locked while holding `executor_mutex`.
    mutable std::mutex mutex;

    RefreshSchedule refresh_schedule;
    RefreshSettings refresh_settings;
    bool refresh_append;

    RefreshSet::Handle set_handle;

    /// StorageIDs of our dependencies that we're waiting for.
    DatabaseAndTableNameSet remaining_dependencies;
    bool time_arrived = false;

    /// Refreshes are stopped (e.g. by SYSTEM STOP VIEW).
    bool stop_requested = false;

    /// If true, we should start a refresh right away. All refreshes go through this flag.
    bool refresh_immediately = false;

    /// When to refresh next. Updated when a refresh is finished or cancelled.
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
    std::chrono::system_clock::time_point next_refresh_actual;
    Int64 num_retries = 0;

    /// Calls refreshTask() from background thread.
    BackgroundSchedulePool::TaskHolder refresh_task;

    /// Used in tests. If not INT64_MIN, we pretend that this is the current time, instead of calling system_clock::now().
    std::atomic<Int64> fake_clock {INT64_MIN};

    /// Just for observability.
    RefreshInfo info;
    Progress progress;
    std::condition_variable refresh_cv; // notified when info.state changes

    /// The main loop of the refresh task. It examines the state, sees what needs to be
    /// done and does it. If there's nothing to do at the moment, returns; it's then scheduled again,
    /// when needed, by public methods or by timer.
    ///
    /// Public methods just provide inputs for the refreshTask()'s decisions
    /// (e.g. stop_requested, cancel_requested), they don't do anything significant themselves.
    void refreshTask();

    /// Perform an actual refresh: create new table, run INSERT SELECT, exchange tables, drop old table.
    /// Mutex must be unlocked. Called only from refresh_task.
    void executeRefreshUnlocked(bool append);

    /// Assigns next_refresh_*
    void advanceNextRefreshTime(std::chrono::system_clock::time_point now);

    /// Either advances next_refresh_actual using exponential backoff or does advanceNextRefreshTime().
    void scheduleRetryOrSkipToNextRefresh(std::chrono::system_clock::time_point now);

    /// Returns true if all dependencies are fulfilled now. Refills remaining_dependencies in this case.
    bool arriveDependency(const StorageID & parent);
    bool arriveTime();
    void populateDependencies();

    void interruptExecution();

    std::chrono::system_clock::time_point currentTime() const;
};

}
