#include <Storages/MaterializedView/RefreshTask.h>

#include <Storages/StorageMaterializedView.h>

#include <Interpreters/Context.h>
#include <Interpreters/InterpreterInsertQuery.h>
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
            throw Exception("Unknown refresh strategy kind", ErrorCodes::BAD_ARGUMENTS);
    }
}

}

RefreshTask::RefreshTask(
    const ASTRefreshStrategy & strategy)
    : refresh_timer(makeRefreshTimer(strategy))
    , refresh_spread{makeSpreadDistribution(strategy.spread)}
    , refresh_immediately{false}
    , interrupt_execution{false}
    , canceled{false}
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
            throw Exception("Dependencies are allowed only for AFTER refresh kind", ErrorCodes::BAD_ARGUMENTS);

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
    cancel();
    storeState(TaskState::Disabled);
}

void RefreshTask::run()
{
    refresh_immediately.store(true);
    refresh_task->activateAndSchedule();
}

void RefreshTask::cancel()
{
    canceled.store(true);
    interrupt_execution.store(true);
}

void RefreshTask::pause()
{
    interrupt_execution.store(true);
}

void RefreshTask::resume()
{
    interrupt_execution.store(false);
    refresh_immediately.store(true);
    refresh_task->schedule();
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

    if (!refresh_executor)
        initializeRefresh(view);

    storeState(TaskState::Running);

    switch (executeRefresh())
    {
        case ExecutionResult::Paused:
            storeState(TaskState::Paused);
            return;
        case ExecutionResult::Finished:
            completeRefresh(view);
            storeLastState(LastTaskState::Finished);
            break;
        case ExecutionResult::Cancelled:
            storeLastState(LastTaskState::Canceled);
            break;
    }

    refresh_executor.reset();
    refresh_block.reset();

    storeLastRefresh(std::chrono::system_clock::now());
    scheduleRefresh(last_refresh);
}

RefreshTask::ExecutionResult RefreshTask::executeRefresh()
{
    bool not_finished{true};
    while (!interrupt_execution.load() && not_finished)
        not_finished = refresh_executor->executeStep(interrupt_execution);

    if (!not_finished)
        return ExecutionResult::Finished;
    if (interrupt_execution.load() && !canceled.load())
        return ExecutionResult::Paused;
    return ExecutionResult::Cancelled;

}

void RefreshTask::initializeRefresh(std::shared_ptr<StorageMaterializedView> view)
{
    refresh_query = view->prepareRefreshQuery();
    auto refresh_context = Context::createCopy(view->getContext());
    refresh_block = InterpreterInsertQuery(refresh_query, refresh_context).execute();
    refresh_block->pipeline.setProgressCallback([this](const Progress & progress){ progressCallback(progress); });

    canceled.store(false);
    interrupt_execution.store(false);

    refresh_executor.emplace(refresh_block->pipeline);
}

void RefreshTask::completeRefresh(std::shared_ptr<StorageMaterializedView> view)
{
    view->updateInnerTableAfterRefresh(refresh_query);
    dependencies.notifyAll(view->getStorageID());
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

std::shared_ptr<StorageMaterializedView> RefreshTask::lockView()
{
    return std::static_pointer_cast<StorageMaterializedView>(view_to_refresh.lock());
}

void RefreshTask::storeState(TaskState task_state)
{
    state.store(task_state);
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
