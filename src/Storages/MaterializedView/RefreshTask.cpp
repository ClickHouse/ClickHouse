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
    : refresh_timer(makeRefreshTimer(strategy))
    , refresh_spread{makeSpreadDistribution(strategy.spread)}
    , canceled{false}
    , refresh_immediately{false}
    , interrupt_execution{false}
{}

RefreshTaskHolder RefreshTask::create(
    const StorageMaterializedView & view,
    ContextMutablePtr context,
    const DB::ASTRefreshStrategy & strategy)
{
    auto task = std::make_shared<RefreshTask>(strategy);

    task->refresh_task = context->getSchedulePool().createTask("MaterializedViewRefresherTask", task->makePoolTask());
    task->set_entry = context->getRefreshSet().emplace(task, view.getStorageID()).value();
    if (strategy.dependencies)
    {
        if (strategy.schedule_kind != ASTRefreshStrategy::ScheduleKind::AFTER)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dependencies are allowed only for AFTER refresh kind");

        task->deps_entries.reserve(strategy.dependencies->children.size());
        for (auto && dependency : strategy.dependencies->children)
        {
            StorageID dep_id(dependency->as<const ASTTableIdentifier &>());
            if (auto dep_task = context->getRefreshSet().getTask(dep_id))
            task->deps_entries.push_back(dep_task->dependencies.add(task));
        }
    }

    return task;
}

void RefreshTask::initialize(std::shared_ptr<StorageMaterializedView> view)
{
    view_to_refresh = view;
}

void RefreshTask::start()
{
    storeState(TaskState::Scheduled);
    refresh_task->activateAndSchedule();
}

void RefreshTask::stop()
{
    refresh_task->deactivate();
    cancelSync();
    storeState(TaskState::Disabled);
}

void RefreshTask::run()
{
    refresh_immediately.store(true);
    refresh_task->activateAndSchedule();
}

void RefreshTask::cancel()
{
    std::lock_guard guard(state_mutex);
    cancelLocked();
}

void RefreshTask::cancelSync()
{
    std::unique_lock lock(state_mutex);
    cancelLocked();
    sync_canceled.wait(lock, [this] { return !canceled; });
}

void RefreshTask::pause()
{
    std::lock_guard guard(state_mutex);
    if (state == TaskState::Running)
    {
        interrupt_execution.store(true);
        state = TaskState::Paused;
    }
}

void RefreshTask::resume()
{
    std::lock_guard guard(state_mutex);
    if (state == TaskState::Paused)
    {
        refresh_immediately.store(true);
        refresh_task->schedule();
        state = TaskState::Scheduled;
    }
}

void RefreshTask::notify(const StorageID & parent_id)
{
    if (combiner.arriveParent(parent_id))
    {
        refresh_immediately.store(true);
        refresh_task->schedule();
    }
}

void RefreshTask::doRefresh()
{
    if (refresh_immediately.exchange(false))
    {
        refresh();
    }
    else
    {
        auto now = std::chrono::system_clock::now();
        if (now >= next_refresh)
        {
            if (combiner.arriveTime())
                refresh();
        }
        else
            scheduleRefresh(now);
    }
}

void RefreshTask::refresh()
{
    auto view = lockView();
    if (!view)
        return;

    std::unique_lock lock(state_mutex);

    if (!refresh_executor)
        initializeRefresh(view);

    storeState(TaskState::Running);

    switch (executeRefresh(lock))
    {
        case ExecutionResult::Paused:
            pauseRefresh(view);
            return;
        case ExecutionResult::Finished:
            completeRefresh(view);
            storeLastState(LastTaskState::Finished);
            break;
        case ExecutionResult::Cancelled:
            cancelRefresh(view);
            storeLastState(LastTaskState::Canceled);
            break;
    }

    cleanState();

    storeLastRefresh(std::chrono::system_clock::now());
    scheduleRefresh(last_refresh);
}

RefreshTask::ExecutionResult RefreshTask::executeRefresh(std::unique_lock<std::mutex> & state_lock)
{
    state_lock.unlock();

    bool not_finished{true};
    while (!interrupt_execution.load() && not_finished)
        not_finished = refresh_executor->executeStep(interrupt_execution);

    state_lock.lock();
    if (!not_finished)
        return ExecutionResult::Finished;
    if (interrupt_execution.load() && !canceled)
        return ExecutionResult::Paused;
    return ExecutionResult::Cancelled;

}

void RefreshTask::initializeRefresh(std::shared_ptr<const StorageMaterializedView> view)
{
    auto fresh_table = view->createFreshTable();
    refresh_query = view->prepareRefreshQuery();
    refresh_query->setTable(fresh_table.table_name);
    refresh_query->setDatabase(fresh_table.database_name);
    auto refresh_context = Context::createCopy(view->getContext());
    refresh_block = InterpreterInsertQuery(refresh_query, refresh_context).execute();
    refresh_block->pipeline.setProgressCallback([this](const Progress & progress){ progressCallback(progress); });

    refresh_executor.emplace(refresh_block->pipeline);
}

void RefreshTask::completeRefresh(std::shared_ptr<StorageMaterializedView> view)
{
    auto stale_table = view->exchangeTargetTable(refresh_query->table_id);
    dependencies.notifyAll(view->getStorageID());

    auto drop_context = Context::createCopy(view->getContext());
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, drop_context, drop_context, stale_table, /*sync=*/true);
}

void RefreshTask::cancelRefresh(std::shared_ptr<const StorageMaterializedView> view)
{
    auto drop_context = Context::createCopy(view->getContext());
    InterpreterDropQuery::executeDropQuery(ASTDropQuery::Kind::Drop, drop_context, drop_context, refresh_query->table_id, /*sync=*/true);
    interrupt_execution.store(false);
    if (std::exchange(canceled, false))
        sync_canceled.notify_all();
}

void RefreshTask::pauseRefresh(std::shared_ptr<const StorageMaterializedView> /*view*/)
{
    interrupt_execution.store(false);
}

void RefreshTask::scheduleRefresh(std::chrono::system_clock::time_point now)
{
    using namespace std::chrono_literals;
    auto scheduled_refresh = calculateRefreshTime(now) + genSpreadSeconds();
    storeNextRefresh(scheduled_refresh);
    auto schedule_time = std::chrono::ceil<std::chrono::milliseconds>(scheduled_refresh - now);
    storeState(TaskState::Scheduled);
    refresh_task->scheduleAfter(std::max(schedule_time, 0ms).count());
}

namespace
{

template <typename... Ts>
struct CombinedVisitor : Ts... { using Ts::operator()...; };
template <typename... Ts>
CombinedVisitor(Ts...) -> CombinedVisitor<Ts...>;

}

std::chrono::sys_seconds RefreshTask::calculateRefreshTime(std::chrono::system_clock::time_point now) const
{
    CombinedVisitor refresh_time_visitor{
        [now](const RefreshAfterTimer & timer) { return timer.after(now); },
        [now](const RefreshEveryTimer & timer) { return timer.next(now); }};
    return std::visit<std::chrono::sys_seconds>(std::move(refresh_time_visitor), refresh_timer);
}

std::chrono::seconds RefreshTask::genSpreadSeconds()
{
    return std::chrono::seconds{refresh_spread(thread_local_rng)};
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

void RefreshTask::cancelLocked()
{
    switch (state)
    {
        case TaskState::Running:
            canceled = true;
            interrupt_execution.store(true);
            break;
        case TaskState::Paused:
            if (auto view = lockView())
                cancelRefresh(view);
            cleanState();
            break;
        default:
            break;
    }
}

void RefreshTask::cleanState()
{
    refresh_executor.reset();
    refresh_block.reset();
    refresh_query.reset();
}

std::shared_ptr<StorageMaterializedView> RefreshTask::lockView()
{
    return std::static_pointer_cast<StorageMaterializedView>(view_to_refresh.lock());
}

void RefreshTask::storeState(TaskState task_state)
{
    state = task_state;
    set_entry->state.store(static_cast<RefreshTaskStateUnderlying>(task_state));
}

void RefreshTask::storeLastState(LastTaskState task_state)
{
    last_state = task_state;
    set_entry->last_state.store(static_cast<RefreshTaskStateUnderlying>(task_state));
}

void RefreshTask::storeLastRefresh(std::chrono::system_clock::time_point last)
{
    last_refresh = last;
    auto secs = std::chrono::floor<std::chrono::seconds>(last);
    set_entry->last_s.store(secs.time_since_epoch().count());
}

void RefreshTask::storeNextRefresh(std::chrono::system_clock::time_point next)
{
    next_refresh = next;
    auto secs = std::chrono::floor<std::chrono::seconds>(next);
    set_entry->next_s.store(secs.time_since_epoch().count());
}

}
