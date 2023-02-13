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
    enum class TaskState : RefreshTaskStateUnderlying
    {
        Disabled = 0,
        Scheduled,
        Running,
        Paused
    };

    enum class LastTaskState : RefreshTaskStateUnderlying
    {
        Unknown = 0,
        Canceled,
        Finished
    };

    /// Never call it manual, public for shared_ptr construction only
    RefreshTask(const ASTRefreshStrategy & strategy);

    /// The only proper way to construct task
    static RefreshTaskHolder create(
        const StorageMaterializedView & view,
        ContextMutablePtr context,
        const DB::ASTRefreshStrategy & strategy);

    void initialize(std::shared_ptr<StorageMaterializedView> view);

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
    enum class ExecutionResult : UInt8
    {
        Finished,
        Paused,
        Cancelled
    };

    void doRefresh();

    void scheduleRefresh(std::chrono::system_clock::time_point now);

    void refresh();

    ExecutionResult executeRefresh();

    void initializeRefresh(std::shared_ptr<StorageMaterializedView> view);

    void completeRefresh(std::shared_ptr<StorageMaterializedView> view);

    std::chrono::sys_seconds calculateRefreshTime(std::chrono::system_clock::time_point now) const;

    std::chrono::seconds genSpreadSeconds();

    void progressCallback(const Progress & progress);

    auto makePoolTask()
    {
        return [self = this->weak_from_this()]
        {
            if (auto task = self.lock())
                task->doRefresh();
        };
    }

    std::shared_ptr<StorageMaterializedView> lockView();

    void storeState(TaskState task_state);

    void storeLastState(LastTaskState task_state);

    void storeLastRefresh(std::chrono::system_clock::time_point last);

    void storeNextRefresh(std::chrono::system_clock::time_point next);

    /// Task ownership
    BackgroundSchedulePool::TaskHolder refresh_task;
    std::weak_ptr<IStorage> view_to_refresh;
    RefreshSet::Entry set_entry;

    /// Task execution
    std::optional<ManualPipelineExecutor> refresh_executor;
    std::optional<BlockIO> refresh_block;
    std::shared_ptr<ASTInsertQuery> refresh_query;

    /// Concurrent dependency management
    RefreshAllCombiner combiner;
    RefreshDependencies dependencies;
    std::vector<RefreshDependencies::Entry> deps_entries;

    /// Refresh time settings and data
    std::chrono::system_clock::time_point last_refresh;
    std::chrono::system_clock::time_point next_refresh;
    std::variant<RefreshEveryTimer, RefreshAfterTimer> refresh_timer;

    /// Refresh time randomization
    std::uniform_int_distribution<Int64> refresh_spread;

    /// Task state
    std::atomic<TaskState> state{TaskState::Disabled};
    LastTaskState last_state{LastTaskState::Unknown};

    /// Outer triggers
    std::atomic_bool refresh_immediately;
    std::atomic_bool interrupt_execution;
    std::atomic_bool canceled;
};

}
