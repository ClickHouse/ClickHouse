#include <Storages/MaterializedView/RefreshTask.h>

#include <Storages/StorageMaterializedView.h>

#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/ProcessList.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <QueryPipeline/ReadProgressCallback.h>

namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_WAS_CANCELLED;
}

RefreshTask::RefreshTask(
    const ASTRefreshStrategy & strategy)
    : log(getLogger("RefreshTask"))
    , refresh_schedule(strategy)
{}

RefreshTaskHolder RefreshTask::create(
    const StorageMaterializedView & view,
    ContextMutablePtr context,
    const DB::ASTRefreshStrategy & strategy)
{
    auto task = std::make_shared<RefreshTask>(strategy);

    task->refresh_task = context->getSchedulePool().createTask("MaterializedViewRefresherTask",
        [self = task->weak_from_this()]
        {
            if (auto t = self.lock())
                t->refreshTask();
        });

    std::vector<StorageID> deps;
    if (strategy.dependencies)
        for (auto && dependency : strategy.dependencies->children)
            deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

    context->getRefreshSet().emplace(view.getStorageID(), deps, task);

    return task;
}

void RefreshTask::initializeAndStart(std::shared_ptr<StorageMaterializedView> view)
{
    view_to_refresh = view;
    if (view->getContext()->getSettingsRef().stop_refreshable_materialized_views_on_startup)
        stop_requested = true;
    populateDependencies();
    advanceNextRefreshTime(currentTime());
    refresh_task->schedule();
}

void RefreshTask::rename(StorageID new_id)
{
    std::lock_guard guard(mutex);
    set_handle.rename(new_id);
}

void RefreshTask::alterRefreshParams(const DB::ASTRefreshStrategy & new_strategy)
{
    std::lock_guard guard(mutex);

    RefreshSchedule new_schedule(new_strategy);
    std::vector<StorageID> deps;
    if (new_strategy.dependencies)
        for (auto && dependency : new_strategy.dependencies->children)
            deps.emplace_back(dependency->as<const ASTTableIdentifier &>());

    /// Reschedule next refresh.
    if (new_schedule != refresh_schedule)
    {
        refresh_schedule = new_schedule;
        next_refresh_prescribed = {};
        advanceNextRefreshTime(currentTime());
        refresh_task->schedule();
    }

    /// Update dependency graph.
    set_handle.changeDependencies(deps);

    /// Mark removed dependencies as satisfied.
    DatabaseAndTableNameSet deps_set(deps.begin(), deps.end());
    std::vector<StorageID> removed_deps;
    for (const auto & id : remaining_dependencies)
        if (!deps_set.contains(id))
            removed_deps.push_back(id);
    for (const auto & id : removed_deps)
        if (arriveDependency(id) && !std::exchange(refresh_immediately, true))
            refresh_task->schedule();

    /// TODO: Update settings once we have them.
}

RefreshInfo RefreshTask::getInfo() const
{
    std::lock_guard guard(mutex);
    auto res = info;
    res.view_id = set_handle.getID();
    res.remaining_dependencies.assign(remaining_dependencies.begin(), remaining_dependencies.end());
    if (res.last_refresh_result != LastRefreshResult::Exception)
        res.exception_message.clear();
    res.progress = progress.getValues();
    return res;
}

void RefreshTask::start()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(stop_requested, false))
        return;
    refresh_task->schedule();
}

void RefreshTask::stop()
{
    std::lock_guard guard(mutex);
    if (std::exchange(stop_requested, true))
        return;
    interruptExecution();
    refresh_task->schedule();
}

void RefreshTask::run()
{
    std::lock_guard guard(mutex);
    if (std::exchange(refresh_immediately, true))
        return;
    refresh_task->schedule();
}

void RefreshTask::cancel()
{
    std::lock_guard guard(mutex);
    interruptExecution();
    refresh_task->schedule();
}

void RefreshTask::shutdown()
{
    {
        std::lock_guard guard(mutex);
        stop_requested = true;
        interruptExecution();
    }

    /// Wait for the task to return and prevent it from being scheduled in future.
    refresh_task->deactivate();

    /// Remove from RefreshSet on DROP, without waiting for the IStorage to be destroyed.
    /// This matters because a table may get dropped and immediately created again with the same name,
    /// while the old table's IStorage still exists (pinned by ongoing queries).
    /// (Also, RefreshSet holds a shared_ptr to us.)
    std::lock_guard guard(mutex);
    set_handle.reset();
}

void RefreshTask::notify(const StorageID & parent_id, std::chrono::sys_seconds parent_next_prescribed_time)
{
    std::lock_guard guard(mutex);
    if (!set_handle)
        return; // we've shut down

    /// In the general case, it's not clear what the meaning of dependencies should be.
    /// E.g. what behavior would the user want/expect in the following cases?:
    ///  * REFRESH EVERY 3 HOUR depends on REFRESH EVERY 2 HOUR
    ///  * REFRESH AFTER 3 HOUR depends on REFRESH AFTER 2 HOUR
    ///  * REFRESH AFTER 3 HOUR depends on REFRESH EVERY 1 DAY
    /// I don't know.
    ///
    /// Cases that are important to support well include:
    /// (1) REFRESH EVERY 1 DAY depends on REFRESH EVERY 1 DAY
    ///     Here the second refresh should start only after the first refresh completed *for the same day*.
    ///     Yesterday's refresh of the dependency shouldn't trigger today's refresh of the dependent,
    ///     even if it completed today.
    /// (2) REFRESH EVERY 1 DAY OFFSET 2 HOUR depends on REFRESH EVERY 1 DAY OFFSET 1 HOUR
    /// (3) REFRESH EVERY 1 DAY OFFSET 1 HOUR depends on REFRESH EVERY 1 DAY OFFSET 23 HOUR
    ///     Here the dependency's refresh on day X should trigger dependent's refresh on day X+1.
    /// (4) REFRESH EVERY 2 HOUR depends on REFRESH EVERY 1 HOUR
    ///     The 2 HOUR refresh should happen after the 1 HOUR refresh for every other hour, e.g.
    ///     after the 2pm refresh, then after the 4pm refresh, etc.
    ///
    /// We currently don't allow dependencies in REFRESH AFTER case, because its unclear how to define
    /// it in a non-confusing way. Consider view y that depends on view x, both with
    /// REFRESH AFTER 1 hour. The user's intention is probably to make y always refresh immediately
    /// after x. But suppose y takes slightly longer to refresh than x. If we don't do anything
    /// special, x's refresh schedule will run ahead, and the DEPENDS ON will have pretty much no
    /// effect - confusing! As a dirty way to prevent this, we could just decrease refresh period by,
    /// say, 50%, if the view has dependencies at all. But that still sounds more confusing than useful.
    /// Or we could say that we only refresh y if x refreshes less than 10% of 1 HOUR ago, so in our
    /// scenario y would be refreshing every 2 hours instead of 1 hour sometimes.

    /// Only accept the dependency's refresh if its next refresh time is after ours.
    /// This takes care of cases (1)-(4).
    if (parent_next_prescribed_time <= next_refresh_prescribed)
        return;

    if (arriveDependency(parent_id) && !std::exchange(refresh_immediately, true))
        refresh_task->schedule();
}

void RefreshTask::setFakeTime(std::optional<Int64> t)
{
    std::unique_lock lock(mutex);
    fake_clock.store(t.value_or(INT64_MIN), std::memory_order_relaxed);
    /// Reschedule task with shorter delay if currently scheduled.
    refresh_task->scheduleAfter(100, /*overwrite*/ true, /*only_if_scheduled*/ true);
}

void RefreshTask::refreshTask()
{
    try
    {
        std::unique_lock lock(mutex);

        /// Whoever breaks out of this loop should assign info.state first.
        while (true)
        {
            chassert(lock.owns_lock());

            interrupt_execution.store(false);

            if (stop_requested)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                info.state = RefreshState::Disabled;
                break;
            }

            if (!refresh_immediately)
            {
                auto now = currentTime();
                if (now >= next_refresh_with_spread)
                {
                    if (arriveTime())
                        refresh_immediately = true;
                    else
                    {
                        info.state = RefreshState::WaitingForDependencies;
                        break;
                    }
                }
                else
                {
                    size_t delay_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                        next_refresh_with_spread - now).count();

                    /// If we're in a test that fakes the clock, poll every 100ms.
                    if (fake_clock.load(std::memory_order_relaxed) != INT64_MIN)
                        delay_ms = 100;

                    refresh_task->scheduleAfter(delay_ms);
                    info.state = RefreshState::Scheduled;
                    break;
                }
            }

            /// Perform a refresh.

            refresh_immediately = false;

            auto view = lockView();
            if (!view)
            {
                /// The view was dropped. This RefreshTask should be destroyed soon too.
                /// (Maybe this is unreachable.)
                info.state = RefreshState::Disabled;
                break;
            }

            info.state = RefreshState::Running;

            CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);

            lock.unlock();

            bool refreshed = false;
            std::optional<String> exception;
            auto start_time = std::chrono::steady_clock::now();

            try
            {
                executeRefreshUnlocked(view);
                refreshed = true;
            }
            catch (...)
            {
                if (!interrupt_execution.load())
                {
                    PreformattedMessage message = getCurrentExceptionMessageAndPattern(true);
                    auto text = message.text;
                    message.text = fmt::format("Refresh failed: {}", message.text);
                    LOG_ERROR(log, message);
                    exception = text;
                }
            }

            lock.lock();

            auto now = currentTime();
            auto secs = std::chrono::floor<std::chrono::seconds>(now);
            info.last_attempt_time = UInt32(secs.time_since_epoch().count());
            info.last_attempt_duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time).count();

            if (exception)
            {
                info.last_refresh_result = LastRefreshResult::Exception;
                info.exception_message = *exception;

                /// TODO: Do a few retries with exponential backoff.
                advanceNextRefreshTime(now);
            }
            else if (!refreshed)
            {
                info.last_refresh_result = LastRefreshResult::Cancelled;

                /// Make sure we don't just start another refresh immediately.
                if (!stop_requested && now >= next_refresh_with_spread)
                    advanceNextRefreshTime(now);
            }
            else
            {
                info.last_refresh_result = LastRefreshResult::Finished;
                info.last_success_time = info.last_attempt_time;
                info.refresh_count += 1;
                advanceNextRefreshTime(now);

                auto next_time = next_refresh_prescribed;

                lock.unlock();
                StorageID my_id = view->getStorageID();
                auto dependents = view->getContext()->getRefreshSet().getDependents(my_id);
                for (const RefreshTaskHolder & dep_task : dependents)
                    dep_task->notify(my_id, next_time);
                lock.lock();
            }
        }
    }
    catch (...)
    {
        std::unique_lock lock(mutex);
        stop_requested = true;
        tryLogCurrentException(log,
            "Unexpected exception in refresh scheduling, please investigate. The view will be stopped.");
#ifdef ABORT_ON_LOGICAL_ERROR
        abortOnFailedAssertion("Unexpected exception in refresh scheduling");
#endif
    }
}

void RefreshTask::executeRefreshUnlocked(std::shared_ptr<StorageMaterializedView> view)
{
    LOG_DEBUG(log, "Refreshing view {}", view->getStorageID().getFullTableName());
    progress.reset();

    /// Create a table.
    auto [refresh_context, refresh_query] = view->prepareRefresh();

    StorageID stale_table = StorageID::createEmpty();
    try
    {
        /// Run the query.
        {
            CurrentThread::QueryScope query_scope(refresh_context); // create a thread group for the query

            BlockIO block_io = InterpreterInsertQuery(refresh_query, refresh_context, false, false, false, false).execute();
            QueryPipeline & pipeline = block_io.pipeline;

            pipeline.setProgressCallback([this](const Progress & prog)
            {
                /// TODO: Investigate why most fields are not populated. Change columns in system.view_refreshes as needed, update documentation (docs/en/operations/system-tables/view_refreshes.md).
                progress.incrementPiecewiseAtomically(prog);
            });

            /// Add the query to system.processes and allow it to be killed with KILL QUERY.
            String query_for_logging = refresh_query->formatForLogging(
                refresh_context->getSettingsRef().log_queries_cut_to_length);
            block_io.process_list_entry = refresh_context->getProcessList().insert(
                query_for_logging, refresh_query.get(), refresh_context, Stopwatch{CLOCK_MONOTONIC}.getStart());
            pipeline.setProcessListElement(block_io.process_list_entry->getQueryStatus());
            refresh_context->setProcessListElement(block_io.process_list_entry->getQueryStatus());

            if (!pipeline.completed())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Pipeline for view refresh must be completed");

            PipelineExecutor executor(pipeline.processors, pipeline.process_list_element);
            executor.setReadProgressCallback(pipeline.getReadProgressCallback());

            {
                std::unique_lock exec_lock(executor_mutex);
                if (interrupt_execution.load())
                    throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");
                running_executor = &executor;
            }
            SCOPE_EXIT({
                std::unique_lock exec_lock(executor_mutex);
                running_executor = nullptr;
            });

            executor.execute(pipeline.getNumThreads(), pipeline.getConcurrencyControl());

            /// A cancelled PipelineExecutor may return without exception but with incomplete results.
            /// In this case make sure to:
            ///  * report exception rather than success,
            ///  * do it before destroying the QueryPipeline; otherwise it may fail assertions about
            ///    being unexpectedly destroyed before completion and without uncaught exception
            ///    (specifically, the assert in ~WriteBuffer()).
            if (interrupt_execution.load())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED, "Refresh cancelled");
        }

        /// Exchange tables.
        stale_table = view->exchangeTargetTable(refresh_query->table_id, refresh_context);
    }
    catch (...)
    {
        try
        {
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop, view->getContext(), refresh_context, refresh_query->table_id, /*sync*/ false, /*ignore_sync_setting*/ true);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to drop temporary table after a failed refresh");
            /// Let's ignore this and keep going, at risk of accumulating many trash tables if this keeps happening.
        }
        throw;
    }

    /// Drop the old table (outside the try-catch so we don't try to drop the other table if this fails).
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, view->getContext(), refresh_context, stale_table, /*sync*/ true, /*ignore_sync_setting*/ true);
}

void RefreshTask::advanceNextRefreshTime(std::chrono::system_clock::time_point now)
{
    std::chrono::sys_seconds next = refresh_schedule.prescribeNext(next_refresh_prescribed, now);
    next_refresh_prescribed = next;
    next_refresh_with_spread = refresh_schedule.addRandomSpread(next);

    auto secs = std::chrono::floor<std::chrono::seconds>(next_refresh_with_spread);
    info.next_refresh_time = UInt32(secs.time_since_epoch().count());
}

bool RefreshTask::arriveDependency(const StorageID & parent)
{
    remaining_dependencies.erase(parent);
    if (!remaining_dependencies.empty() || !time_arrived)
        return false;
    populateDependencies();
    return true;
}

bool RefreshTask::arriveTime()
{
    time_arrived = true;
    if (!remaining_dependencies.empty() || !time_arrived)
        return false;
    populateDependencies();
    return true;
}

void RefreshTask::populateDependencies()
{
    chassert(remaining_dependencies.empty());
    auto deps = set_handle.getDependencies();
    remaining_dependencies.insert(deps.begin(), deps.end());
    time_arrived = false;
}

void RefreshTask::interruptExecution()
{
    chassert(!mutex.try_lock());
    std::unique_lock lock(executor_mutex);
    if (interrupt_execution.exchange(true))
        return;
    if (running_executor)
    {
        running_executor->cancel();

        LOG_DEBUG(log, "Cancelling refresh");
    }
}

std::shared_ptr<StorageMaterializedView> RefreshTask::lockView()
{
    return std::static_pointer_cast<StorageMaterializedView>(view_to_refresh.lock());
}

std::chrono::system_clock::time_point RefreshTask::currentTime() const
{
    Int64 fake = fake_clock.load(std::memory_order::relaxed);
    if (fake == INT64_MIN)
        return std::chrono::system_clock::now();
    else
        return std::chrono::system_clock::time_point(std::chrono::seconds(fake));
}

void RefreshTask::setRefreshSetHandleUnlock(RefreshSet::Handle && set_handle_)
{
    set_handle = std::move(set_handle_);
}

}
