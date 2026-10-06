#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>

#include <mutex>
#include <ranges>

namespace DB
{

namespace Runtime::V2
{

namespace
{

constexpr size_t max_local_tasks = 128;
constexpr size_t max_sequential_full_rounds = 61;
constexpr size_t max_sequential_lifo_usages = 79;

}

Worker::Worker(WorkerPool & pool_, size_t worker_id_)
    : pool(pool_)
    , worker_id(worker_id_)
    , runner(pool.pipeline, worker_id)
{
}

std::optional<Task> Worker::pickTask()
{
    while (true)
    {
        if (pool.coordinator.stopped())
            return std::nullopt;

        if (pool.pipeline.hasReadyForRemoval())
            pool.pipeline.removeReady();

        if (auto task = tryPickTask())
            return task;

        if (!pool.coordinator.wait(*this))
            return std::nullopt;

        if (const size_t fired = taskCount(); fired > 1)
            wakeOrSpawn(fired - 1);
    }
}

std::optional<Task> Worker::tryPickTask()
{
    const size_t picks_streak = ++picks_count;

    if (picks_streak % max_sequential_full_rounds == 0)
    {
        if (poll(0) > 0)
            if (auto task = takeFromLocal())
                return task;

        if (auto task = takeFromGlobal())
            return task;
    }

    if (auto task = takeFromNext())
        return task;

    if (auto task = takeFromLocal())
        return task;

    if (auto task = takeFromGlobal())
        return task;

    return takeFromOthers();
}

std::optional<Task> Worker::takeFromNext()
{
    if (!next)
    {
        next_streak = 0;
        return std::nullopt;
    }

    Task task = *std::exchange(next, std::nullopt);
    if (++next_streak <= max_sequential_lifo_usages)
        return task;

    std::lock_guard lock(mutex);
    next_streak = 0;
    local_tasks.pushFront(task);
    local_tasks_count.store(local_tasks.size());
    return std::nullopt;
}

std::optional<Task> Worker::takeFromLocal()
{
    std::lock_guard lock(mutex);
    if (local_tasks.empty())
        return std::nullopt;

    Task task = local_tasks.popBack();
    local_tasks_count.store(local_tasks.size());
    return task;
}

std::optional<Task> Worker::takeFromGlobal()
{
    return pool.global_tasks.tryPop();
}

std::optional<Task> Worker::takeFromOthers()
{
    const size_t workers = pool.workers.size();
    const size_t start = Wyrd::randomNumber() % workers;
    for (size_t i = 0; i < workers; ++i)
    {
        Worker & other = pool.workers[(start + i) % workers];
        if (&other == this)
            continue;

        if (auto task = other.steal())
            return task;
    }

    return std::nullopt;
}

void Worker::pushToNext(Task task)
{
    if (next)
        pushToLocal(*next);
    next = task;
}

void Worker::pushToLocal(Task task)
{
    std::lock_guard lock(mutex);
    local_tasks.pushBack(task);

    if (local_tasks.size() > max_local_tasks)
        pool.global_tasks.takeFront(local_tasks, local_tasks.size() / 2);

    local_tasks_count.store(local_tasks.size());
}

void Worker::wakeOrSpawn(size_t threads_needed)
{
    size_t woken = 0;
    if (pool.coordinator.idle() > 0)
        woken = pool.coordinator.wake(threads_needed);

    if (threads_needed > woken)
        pool.grow(threads_needed - woken);
}

size_t Worker::runTask(Task task)
{
    runner.run(task);

    for (const auto & found : runner.getFoundTasks() | std::views::reverse)
        pushToNext(found);

    for (const auto & found : runner.getFoundPollRequests())
        pool.poller.add(found);

    const size_t work_found = runner.getFoundTasks().size() + runner.getFoundPollRequests().size();
    return work_found == 0 ? 0 : work_found - 1;
}

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto task = pickTask())
    {
        if (pool.coordinator.needsPoller())
            wakeOrSpawn(1);

        const size_t threads_needed = runTask(*task);

        if (threads_needed > 0)
            wakeOrSpawn(threads_needed);

        if (pool.pipeline.process_list_element && !pool.pipeline.process_list_element->checkTimeLimitSoft())
        {
            pool.pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
            pool.coordinator.stop();
            break;
        }

        if (!slot.keepGoing())
            break;

        if (yield_flag && yield_flag->load())
            break;
    }

    if (pool.pipeline.step_profiler)
        pool.pipeline.step_profiler->addWorkIntervals(runner.takeWorkIntervals());
}

std::optional<Task> Worker::steal()
{
    if (local_tasks_count.load() == 0)
        return std::nullopt;

    std::lock_guard lock(mutex);
    if (local_tasks.empty())
        return std::nullopt;

    Task task = local_tasks.popFront();
    local_tasks_count.store(local_tasks.size());
    return task;
}

size_t Worker::poll(int timeout_ms)
{
    if (pool.poller.pending() == 0)
        return 0;

    auto fired = pool.poller.poll(timeout_ms);
    for (auto * state : fired | std::views::reverse)
        pushToLocal(Task::create(*state, Task::Step::AsyncReady));

    return fired.size();
}

void Worker::drain()
{
    {
        std::lock_guard lock(mutex);
        pool.global_tasks.takeAll(local_tasks);
        local_tasks_count.store(0);
    }

    if (next)
        pool.global_tasks.push(*std::exchange(next, std::nullopt));
}

size_t Worker::taskCount() const
{
    return local_tasks_count.load();
}

void Worker::park()
{
    wake_up.wait(false);
    wake_up.store(false);
}

void Worker::wake()
{
    wake_up.store(true);
    wake_up.notify_one();
}

}

}
