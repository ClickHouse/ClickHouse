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

namespace
{

std::uniform_int_distribution<Int64> makeSpreadDistribution(const ASTTimePeriod * spread)
{
    if (!spread)
        return std::uniform_int_distribution<Int64>(0, 0);
    Int64 limit = spread->kind.toAvgSeconds() * spread->value / 2;
    return std::uniform_int_distribution(-limit, limit);
}

}

RefreshTask::RefreshTask(
    const ASTRefreshStrategy & strategy)
    : log(&Poco::Logger::get("RefreshTask"))
    , refresh_timer(strategy)
    , refresh_spread{makeSpreadDistribution(strategy.spread)}
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

    task->set_entry = context->getRefreshSet().emplace(view.getStorageID(), deps, task);

    return task;
}

void RefreshTask::initializeAndStart(std::shared_ptr<StorageMaterializedView> view)
{
    view_to_refresh = view;
    /// TODO: Add a setting to stop views on startup, set `stop_requested = true` in that case.
    populateDependencies();
    calculateNextRefreshTime(std::chrono::system_clock::now());
    refresh_task->schedule();
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
    std::lock_guard guard(mutex);
    set_entry.reset();
}

void RefreshTask::notify(const StorageID & parent_id, std::chrono::system_clock::time_point scheduled_time_without_spread, const RefreshTimer & parent_timer)
{
    std::lock_guard guard(mutex);
    if (!set_entry)
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
    if (parent_timer.next(scheduled_time_without_spread) <= next_refresh_without_spread)
        return;

    if (arriveDependency(parent_id) && !std::exchange(refresh_immediately, true))
        refresh_task->schedule();

    /// Decrease delay in case (5).
    /// Maybe we should do it for all AFTER-AFTER dependencies, even if periods are different.
    if (refresh_timer == parent_timer && refresh_timer.tryGetAfter())
    {
        /// TODO: Implement this:
        ///        * Add setting max_after_delay_adjustment_pct
        ///        * Decrease both next_refresh_without_spread and next_refresh_with_spread,
        ///          but only if they haven't already been decreased this way during current period
        ///        * refresh_task->schedule()
    }
}

void RefreshTask::refreshTask()
{
    try
    {
        std::unique_lock lock(mutex);

        /// Whoever breaks out of this loop should call reportState() first.
        while (true)
        {
            chassert(lock.owns_lock());

            interrupt_execution.store(false);

            /// Discard the active refresh if requested.
            if ((stop_requested || cancel_requested) && refresh_executor)
            {
                lock.unlock();
                cancelRefresh(LastTaskResult::Canceled);
                lock.lock();

                if (cancel_requested)
                {
                    /// Advance to the next refresh time according to schedule.
                    /// Otherwise we'd start another refresh immediately after canceling this one.
                    auto now = std::chrono::system_clock::now();
                    if (now >= next_refresh_with_spread)
                        calculateNextRefreshTime(std::chrono::system_clock::now());
                }
            }

            cancel_requested = false;

            if (pause_requested && !refresh_executor)
                pause_requested = false; // no refresh to pause

            if (stop_requested || pause_requested)
            {
                /// Exit the task and wait for the user to start or resume, which will schedule the task again.
                reportState(stop_requested ? RefreshState::Disabled : RefreshState::Paused);
                break;
            }

            if (!refresh_immediately && !refresh_executor)
            {
                auto now = std::chrono::system_clock::now();
                if (now >= next_refresh_with_spread)
                {
                    if (arriveTime())
                        refresh_immediately = true;
                    else
                    {
                        /// TODO: Put the array of remaining dependencies in RefreshSet, report it in the system table (update it from notify() too).
                        reportState(RefreshState::WaitingForDependencies);
                        break;
                    }
                }
                else
                {
                    refresh_task->scheduleAfter(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            next_refresh_with_spread - now).count());
                    reportState(RefreshState::Scheduled);
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
                reportState(RefreshState::Disabled);
                break;
            }

            reportState(RefreshState::Running);

            CurrentMetrics::Increment metric_inc(CurrentMetrics::RefreshingViews);
            auto scheduled_time_without_spread = next_refresh_without_spread;

            lock.unlock();

            bool finished = false;

            try
            {
                if (!refresh_executor)
                    initializeRefresh(view);

                finished = executeRefresh();

                if (finished)
                    completeRefresh(view, LastTaskResult::Finished, scheduled_time_without_spread);
            }
            catch (...)
            {
                tryLogCurrentException(log, "Refresh failed");

                /// Don't leave a trash table.
                if (!finished && refresh_query)
                    cancelRefresh(LastTaskResult::Exception);

                /// TODO: Put the exception message into RefreshSet, report it in the system table.
                /// TODO: Backoff. Maybe just assigning next_refresh_* will work.
            }

            lock.lock();

            if (finished)
            {
                auto now = std::chrono::system_clock::now();
                reportLastRefreshTime(now);
                calculateNextRefreshTime(now);
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

void RefreshTask::initializeRefresh(std::shared_ptr<const StorageMaterializedView> view)
{
    chassert(!refresh_query);

    auto fresh_table = view->createFreshTable();
    refresh_query = view->prepareRefreshQuery();
    refresh_query->setTable(fresh_table.table_name);
    refresh_query->setDatabase(fresh_table.database_name);
    auto refresh_context = Context::createCopy(view->getContext());
    refresh_block = InterpreterInsertQuery(refresh_query, refresh_context).execute();
    refresh_block->pipeline.setProgressCallback([this](const Progress & progress){ progressCallback(progress); });

    refresh_executor.emplace(refresh_block->pipeline);
}

bool RefreshTask::executeRefresh()
{
    bool not_finished{true};
    while (!interrupt_execution.load() && not_finished)
        not_finished = refresh_executor->executeStep(interrupt_execution);

    return !not_finished;
}

void RefreshTask::completeRefresh(std::shared_ptr<StorageMaterializedView> view, LastTaskResult result, std::chrono::system_clock::time_point scheduled_time_without_spread)
{
    auto stale_table = view->exchangeTargetTable(refresh_query->table_id);

    auto context = view->getContext();
    StorageID my_id = set_entry->getID();
    auto dependents = context->getRefreshSet().getDependents(my_id);
    for (const RefreshTaskHolder & dep_task : dependents)
        dep_task->notify(my_id, scheduled_time_without_spread, refresh_timer);

    auto drop_context = Context::createCopy(context);
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, drop_context, drop_context, stale_table, /*sync=*/true);

    cleanState();
    reportLastResult(result);
}

void RefreshTask::cancelRefresh(LastTaskResult result)
{
    if (auto view = lockView())
    {
        try
        {
            auto drop_context = Context::createCopy(view->getContext());
            InterpreterDropQuery::executeDropQuery(
                ASTDropQuery::Kind::Drop, drop_context, drop_context, refresh_query->table_id, /*sync=*/true);
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to drop temporary table after a failed refresh");
            /// Let's ignore this and keep going, at risk of accumulating many trash tables if this keeps happening.
        }
    }

    cleanState();
    reportLastResult(result);
}

void RefreshTask::cleanState()
{
    refresh_executor.reset();
    refresh_block.reset();
    refresh_query.reset();
}

void RefreshTask::calculateNextRefreshTime(std::chrono::system_clock::time_point now)
{
    /// TODO: Add a setting to randomize initial delay in case of AFTER, for the case when the server
    ///       is restarted more often than the refresh period.
    /// TODO: Maybe do something like skip_update_after_seconds and skip_update_after_ratio.
    ///       Unclear if that's useful at all if the last refresh timestamp is not remembered across restarts.

    /// It's important to use time without spread here, otherwise we would do multiple refreshes instead
    /// of one, if the generated spread is negative and the first refresh completes faster than the spread.
    std::chrono::sys_seconds next = refresh_timer.next(next_refresh_without_spread);
    if (next < now)
        next = refresh_timer.next(now); // fell behind, skip to current time

    next_refresh_without_spread = next;
    next_refresh_with_spread = next + std::chrono::seconds{refresh_spread(thread_local_rng)};

    reportNextRefreshTime(next_refresh_with_spread);
}

bool RefreshTask::arriveDependency(const StorageID & parent_table_or_timer)
{
    remaining_dependencies.erase(parent_table_or_timer);
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
    auto deps = set_entry->getDependencies();
    remaining_dependencies.insert(deps.begin(), deps.end());
    time_arrived = false;
}

std::shared_ptr<StorageMaterializedView> RefreshTask::lockView()
{
    return std::static_pointer_cast<StorageMaterializedView>(view_to_refresh.lock());
}

void RefreshTask::progressCallback(const Progress & progress)
{
    set_entry->read_rows.store(progress.read_rows, std::memory_order_relaxed);
    set_entry->read_bytes.store(progress.read_bytes, std::memory_order_relaxed);
    set_entry->total_rows_to_read.store(progress.total_rows_to_read, std::memory_order_relaxed);
    set_entry->total_bytes_to_read.store(progress.total_bytes_to_read, std::memory_order_relaxed);
    set_entry->written_rows.store(progress.written_rows, std::memory_order_relaxed);
    set_entry->written_bytes.store(progress.written_bytes, std::memory_order_relaxed);
    set_entry->result_rows.store(progress.result_rows, std::memory_order_relaxed);
    set_entry->result_bytes.store(progress.result_bytes, std::memory_order_relaxed);
    set_entry->elapsed_ns.store(progress.elapsed_ns, std::memory_order_relaxed);
}

void RefreshTask::reportState(RefreshState s)
{
    set_entry->state.store(static_cast<RefreshTaskStateUnderlying>(s));
}

void RefreshTask::reportLastResult(LastTaskResult r)
{
    set_entry->last_result.store(static_cast<RefreshTaskStateUnderlying>(r));
}

void RefreshTask::reportLastRefreshTime(std::chrono::system_clock::time_point last)
{
    auto secs = std::chrono::floor<std::chrono::seconds>(last);
    set_entry->last_s.store(secs.time_since_epoch().count());
}

void RefreshTask::reportNextRefreshTime(std::chrono::system_clock::time_point next)
{
    auto secs = std::chrono::floor<std::chrono::seconds>(next);
    set_entry->next_s.store(secs.time_since_epoch().count());
}

}
