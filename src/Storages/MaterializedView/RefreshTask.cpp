#include <Storages/MaterializedView/RefreshTask.h>

#include <Storages/StorageMaterializedView.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Processors/Executors/ManualPipelineExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

std::uniform_int_distribution<Int64> makeSpreadDistribution(const ASTTimePeriod * spread)
{
    if (!spread)
        return std::uniform_int_distribution<Int64>(0, 0);
    Int64 limit = spread->kind.toAvgSeconds() * spread->value / 2;
    return std::uniform_int_distribution(-limit, limit);
}

std::variant<RefreshEveryTimer, RefreshAfterTimer> makeRefreshTimer(const ASTRefreshStrategy & strategy)
{
    using enum ASTRefreshStrategy::ScheduleKind;
    switch (strategy.schedule_kind)
    {
        case EVERY:
            return RefreshEveryTimer{*strategy.period, strategy.interval};
        case AFTER:
            return RefreshAfterTimer{strategy.interval};
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown refresh strategy kind");
    }
}

}

RefreshTask::RefreshTask(
    const ASTRefreshStrategy & strategy)
    : log(&Poco::Logger::get("RefreshTask"))
    , refresh_timer(makeRefreshTimer(strategy))
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
    task->set_entry = context->getRefreshSet().emplace(view.getStorageID(), task).value();
    if (strategy.dependencies)
    {
        if (strategy.schedule_kind != ASTRefreshStrategy::ScheduleKind::AFTER)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dependencies are allowed only for AFTER refresh kind");

        task->deps_entries.reserve(strategy.dependencies->children.size());
        for (auto && dependency : strategy.dependencies->children)
        {
            StorageID dep_id(dependency->as<const ASTTableIdentifier &>());
            /// TODO:
            ///  * This depends on the order in which different tables are initialized.
            ///    Is the order guaranteed on startup?
            ///  * At what point does the table name from the query get mapped to the table's UUID?
            ///    Does it work at all? Is it reliable?
            ///  * Don't silently ignore if the table is missing.
            if (auto dep_task = context->getRefreshSet().getTask(dep_id))
                task->deps_entries.push_back(dep_task->dependencies.add(task));
        }

        /// TODO: Initialize combiner.
    }

    return task;
}

void RefreshTask::initializeAndStart(std::shared_ptr<StorageMaterializedView> view)
{
    view_to_refresh = view;
    /// TODO: Add a setting to stop views on startup, set `stop_requested = true` in that case.
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

void RefreshTask::notify(const StorageID & parent_id)
{
    std::lock_guard guard(mutex);
    if (!combiner.arriveParent(parent_id))
        return;
    if (std::exchange(refresh_immediately, true))
        return;
    refresh_task->schedule();
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
                    if (combiner.arriveTime())
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

            lock.unlock();

            bool finished = false;

            try
            {
                if (!refresh_executor)
                    initializeRefresh(view);

                finished = executeRefresh();

                if (finished)
                    completeRefresh(view, LastTaskResult::Finished);
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

void RefreshTask::completeRefresh(std::shared_ptr<StorageMaterializedView> view, LastTaskResult result)
{
    auto stale_table = view->exchangeTargetTable(refresh_query->table_id);
    dependencies.notifyAll(view->getStorageID());

    auto drop_context = Context::createCopy(view->getContext());
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

namespace
{

template <typename... Ts>
struct CombinedVisitor : Ts... { using Ts::operator()...; };
template <typename... Ts>
CombinedVisitor(Ts...) -> CombinedVisitor<Ts...>;

}

void RefreshTask::calculateNextRefreshTime(std::chrono::system_clock::time_point now)
{
    /// TODO: Add a setting to randomize initial delay in case of AFTER, for the case when the server
    ///       is restarted more often than the refresh period.
    /// TODO: Maybe do something like skip_update_after_seconds and skip_update_after_ratio.
    ///       Unclear if that's useful at all if the last refresh timestamp is not remembered across restarts.

    auto advance = [&](std::chrono::system_clock::time_point t)
    {
        CombinedVisitor refresh_time_visitor{
            [t](const RefreshAfterTimer & timer) { return timer.after(t); },
            [t](const RefreshEveryTimer & timer) { return timer.next(t); }};
        auto r = std::visit<std::chrono::sys_seconds>(std::move(refresh_time_visitor), refresh_timer);
        chassert(r > t);
        return r;
    };

    /// It's important to use time without spread here, otherwise we would do multiple refreshes instead
    /// of one, if the generated spread is negative and the first refresh completes faster than the spread.
    std::chrono::sys_seconds next = advance(next_refresh_without_spread);
    if (next < now)
        next = advance(now); // fell behind, skip to current time

    next_refresh_without_spread = next;
    next_refresh_with_spread = next + std::chrono::seconds{refresh_spread(thread_local_rng)};

    reportNextRefreshTime(next_refresh_with_spread);
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
