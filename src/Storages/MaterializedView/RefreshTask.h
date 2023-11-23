#pragma once

#include <Storages/MaterializedView/RefreshAllCombiner.h>
#include <Storages/MaterializedView/RefreshDependencies.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MaterializedView/RefreshTask_fwd.h>
#include <Storages/MaterializedView/RefreshTimers.h>

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
    /// Just for observability.
    enum class RefreshState : RefreshTaskStateUnderlying
    {
        Disabled = 0,
        Scheduled,
        WaitingForDependencies,
        Running,
        Paused
    };

    /// Just for observability.
    enum class LastTaskResult : RefreshTaskStateUnderlying
    {
        Unknown = 0,
        Canceled,
        Exception,
        Finished
    };

    /// Never call it manual, public for shared_ptr construction only
    explicit RefreshTask(const ASTRefreshStrategy & strategy);

    /// The only proper way to construct task
    static RefreshTaskHolder create(
        const StorageMaterializedView & view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy);

    void initializeAndStart(std::shared_ptr<StorageMaterializedView> view);

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

    /// Notify dependent task
    void notify(const StorageID & parent_id);

private:
    Poco::Logger * log = nullptr;
    std::weak_ptr<IStorage> view_to_refresh;
    RefreshSet::Entry set_entry;

    /// Refresh schedule
    std::variant<RefreshEveryTimer, RefreshAfterTimer> refresh_timer;
    std::uniform_int_distribution<Int64> refresh_spread;

    /// Task execution. Non-empty iff a refresh is in progress (possibly paused).
    /// Whoever unsets these should also call storeLastState().
    std::optional<ManualPipelineExecutor> refresh_executor;
    std::optional<BlockIO> refresh_block;
    std::shared_ptr<ASTInsertQuery> refresh_query;

    /// Concurrent dependency management
    RefreshAllCombiner combiner;
    RefreshDependencies dependencies;
    std::vector<RefreshDependencies::Entry> deps_entries;

    /// Protects all fields below (they're accessed by both refreshTask() and public methods).
    /// Never locked for blocking operations (e.g. creating or dropping the internal table).
    std::mutex mutex;

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
    std::chrono::system_clock::time_point next_refresh_without_spread;
    std::chrono::system_clock::time_point next_refresh_with_spread;

    /// Calls refreshTask() from background thread.
    BackgroundSchedulePool::TaskHolder refresh_task;

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
    void initializeRefresh(std::shared_ptr<const StorageMaterializedView> view);
    bool executeRefresh();
    void completeRefresh(std::shared_ptr<StorageMaterializedView> view, LastTaskResult result);
    void cancelRefresh(LastTaskResult result);
    void cleanState();

    /// Assigns next_refresh_*
    void calculateNextRefreshTime(std::chrono::system_clock::time_point now);

    std::shared_ptr<StorageMaterializedView> lockView();

    /// Methods that push information to RefreshSet, for observability.
    void progressCallback(const Progress & progress);
    void reportState(RefreshState s);
    void reportLastResult(LastTaskResult r);
    void reportLastRefreshTime(std::chrono::system_clock::time_point last);
    void reportNextRefreshTime(std::chrono::system_clock::time_point next);
};

}
