#include <Storages/MaterializedView/RefreshTask.h>

#include <Storages/StorageMaterializedView.h>

#include <Common/CurrentMetrics.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Executors/ManualPipelineExecutor.h>

namespace CurrentMetrics
{
    extern const Metric RefreshingViews;
}

namespace DB
{

RefreshTask::RefreshTask(
    const ASTRefreshStrategy & strategy)
    : log(&Poco::Logger::get("RefreshTask"))
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

    task->set_handle = context->getRefreshSet().emplace(view.getStorageID(), deps, task);

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

    refresh_schedule = new_schedule;

    /// Reschedule next refresh.
    if (new_schedule != refresh_schedule)
    {
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
        if (!deps_set.count(id))
            removed_deps.push_back(id);
    for (const auto & id : removed_deps)
        arriveDependency(id);

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
    interrupt_execution.store(true);
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
    if (std::exchange(cancel_requested, true))
        return;
    interrupt_execution.store(true);
    refresh_task->schedule();
}

void RefreshTask::pause()
{
    std::lock_guard guard(mutex);
    if (std::exchange(pause_requested, true))
        return;
    interrupt_execution.store(true);
    refresh_task->schedule();
}

void RefreshTask::resume()
{
    std::lock_guard guard(mutex);
    if (!std::exchange(pause_requested, false))
        return;
    refresh_task->schedule();
}

void RefreshTask::shutdown()
{
    {
        std::lock_guard guard(mutex);
        stop_requested = true;
        interrupt_execution.store(true);
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

void RefreshTask::notify(const StorageID & parent_id, std::chrono::sys_seconds prescribed_time, const RefreshSchedule & parent_schedule)
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
    /// (5) REFRESH AFTER 1 HOUR depends on REFRESH AFTER 1 HOUR
    ///     Here the two views should try to synchronize their schedules instead of arbitrarily drifting
    ///     apart. In particular, consider the case where the dependency refreshes slightly faster than
    ///     the dependent. If we don't do anything special, the DEPENDS ON will have pretty much no effect.
    ///     To apply some synchronization pressure, we reduce the dependent's delay by some percentage
    ///     after the dependent completed.
    /// (6) REFRESH AFTER 1 HOUR depends on REFRESH AFTER 2 HOUR
    ///     REFRESH EVERY 1 HOUR depends on REFRESH EVERY 2 HOUR
    ///     Not sure about these. Currently we just make the dependent refresh at the same rate as
    ///     the dependency, i.e. the 1 HOUR table will actually be refreshed every 2 hours.

    /// Only accept the dependency's refresh if its next refresh time is after ours.
    /// This takes care of cases (1)-(4), and seems harmless in all other cases.
    /// Might be mildly helpful in weird cases like REFRESH AFTER 3 HOUR depends on REFRESH AFTER 2 HOUR.
    if (parent_schedule.prescribeNext(prescribed_time, currentTime()) <= next_refresh_prescribed)
        return;

    if (arriveDependency(parent_id) && !std::exchange(refresh_immediately, true))
        refresh_task->schedule();

    /// Decrease delay in case (5).
    /// Maybe we should do it for all AFTER-AFTER dependencies, even if periods are different.
    if (refresh_schedule.kind == RefreshScheduleKind::AFTER &&
        parent_schedule.kind == RefreshScheduleKind::AFTER &&
        refresh_schedule.period == parent_schedule.period)
    {
        /// TODO: Implement this.
        ///        * Add setting max_after_delay_adjustment_pct
        ///        * Decrease both next_refresh_prescribed and next_refresh_with_spread,
        ///          but only if they haven't already been decreased this way during current period
        ///        * refresh_task->schedule()
    }
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

            /// Discard the active refresh if requested.
            if ((stop_requested || cancel_requested) && refresh_executor)
            {
                lock.unlock();
                cancelRefreshUnlocked(LastRefreshResult::Canceled);
                lock.lock();

                if (cancel_requested)
                {
                    /// Move on to the next refresh time according to schedule.
                    /// Otherwise we'd start another refresh immediately after canceling this one.
                    auto now = currentTime();
                    if (now >= next_refresh_with_spread)
                        advanceNextRefreshTime(now);
                }
            }

            cancel_requested = false;

            if (pause_requested && !refresh_executor)
                pause_requested = false; // no refresh to pause

            if (stop_requested || pause_requested)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                info.state = stop_requested ? RefreshState::Disabled : RefreshState::Paused;
                break;
            }

            if (!refresh_immediately && !refresh_executor)
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
            auto prescribed_time = next_refresh_prescribed;

            lock.unlock();

            bool finished = false;

            try
            {
                if (!refresh_executor)
                    initializeRefreshUnlocked(view);

                finished = executeRefreshUnlocked();

                if (finished)
                    completeRefreshUnlocked(view, LastRefreshResult::Finished, prescribed_time);

                lock.lock();
            }
            catch (...)
            {
                PreformattedMessage message = getCurrentExceptionMessageAndPattern(true);
                auto text = message.text;
                message.text = fmt::format("Refresh failed: {}", message.text);
                LOG_ERROR(log, message);

                /// Don't leave a trash table.
                if (!finished && refresh_query)
                    cancelRefreshUnlocked(LastRefreshResult::Exception);

                lock.lock();
                info.exception_message = text;

                /// TODO: Backoff. Maybe just assigning next_refresh_* will work.
            }
            chassert(lock.owns_lock());

            if (finished)
            {
                auto now = currentTime();
                auto secs = std::chrono::floor<std::chrono::seconds>(now);
                info.last_refresh_time = UInt32(secs.time_since_epoch().count());
                advanceNextRefreshTime(now);
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

void RefreshTask::initializeRefreshUnlocked(std::shared_ptr<const StorageMaterializedView> view)
{
    chassert(!refresh_query);

    progress.reset();

    auto fresh_table = view->createFreshTable();
    refresh_query = view->prepareRefreshQuery();
    refresh_query->setTable(fresh_table.table_name);
    refresh_query->setDatabase(fresh_table.database_name);
    auto refresh_context = Context::createCopy(view->getContext());
    refresh_context->makeQueryContext();
    refresh_block = InterpreterInsertQuery(refresh_query, refresh_context).execute();
    refresh_block->pipeline.setProgressCallback([this](const Progress & prog)
        {
            progress.incrementPiecewiseAtomically(prog);
        });

    refresh_executor.emplace(refresh_block->pipeline);
}

bool RefreshTask::executeRefreshUnlocked()
{
    bool not_finished{true};
    while (!interrupt_execution.load() && not_finished)
        not_finished = refresh_executor->executeStep(interrupt_execution);

    return !not_finished;
}

void RefreshTask::completeRefreshUnlocked(std::shared_ptr<StorageMaterializedView> view, LastRefreshResult result, std::chrono::sys_seconds prescribed_time)
{
    auto stale_table = view->exchangeTargetTable(refresh_query->table_id);

    cleanStateUnlocked();

    std::unique_lock lock(mutex);
    auto refresh_schedule_copy = refresh_schedule;
    lock.unlock();

    auto context = view->getContext();
    StorageID my_id = view->getStorageID();
    auto dependents = context->getRefreshSet().getDependents(my_id);
    for (const RefreshTaskHolder & dep_task : dependents)
        dep_task->notify(my_id, prescribed_time, refresh_schedule_copy);

    auto drop_context = Context::createCopy(context);
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, drop_context, drop_context, stale_table, /*sync*/ true, /*ignore_sync_setting*/ true);

    info.last_refresh_result = result;
}

void RefreshTask::cancelRefreshUnlocked(LastRefreshResult result)
{
    auto table_to_drop = refresh_query->table_id;

    /// Destroy the executor to unpin the table that we're about to drop.
    /// (Not necessary if the drop is asynchronous, but why not.)
    cleanStateUnlocked();

    if (auto view = lockView())
    {
        try
        {
            auto drop_context = Context::createCopy(view->getContext());
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop, drop_context, drop_context, table_to_drop, /*sync*/ false, /*ignore_sync_setting*/ true);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to drop temporary table after a failed refresh");
            /// Let's ignore this and keep going, at risk of accumulating many trash tables if this keeps happening.
        }
    }

    info.last_refresh_result = result;
}

void RefreshTask::cleanStateUnlocked()
{
    refresh_executor.reset();
    refresh_block.reset();
    refresh_query.reset();
}

void RefreshTask::advanceNextRefreshTime(std::chrono::system_clock::time_point now)
{
    /// TODO: Add a setting to randomize initial delay in case of AFTER, for the case when the server
    ///       is restarted more often than the refresh period.
    /// TODO: Maybe do something like skip_update_after_seconds and skip_update_after_ratio.
    ///       Or maybe that should be checked in refreshTask(), just before starting a refresh.
    ///       Probably only useful after we have concurrency limits. Or maybe it's not useful even then?

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

}
