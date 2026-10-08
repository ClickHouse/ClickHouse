#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>

#include <ranges>
#include <span>

namespace DB
{

namespace Runtime::V2
{

namespace
{

constexpr size_t max_sequential_full_rounds = 61;

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

        if (auto task = pool.coordinator.wait(*this))
            return task;
    }
}

std::optional<Task> Worker::tryPickTask()
{
    const size_t picks_streak = ++picks_count;

    if (picks_streak % max_sequential_full_rounds == 0)
        if (auto task = takeFromGlobal())
            return task;

    if (auto task = takeFromLocal())
        return task;

    if (auto task = takeFromGlobal())
        return task;

    if (auto task = takeFromPoller())
        return task;

    return takeFromOthers();
}

std::optional<Task> Worker::takeFromLocal()
{
    return local_tasks.pop();
}

std::optional<Task> Worker::takeFromGlobal()
{
    return pool.global_tasks.tryPop();
}

std::optional<Task> Worker::takeFromPoller()
{
    if (pool.coordinator.isPolling())
        return std::nullopt;

    return poll(0);
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

void Worker::pushToLocal(const Task & task)
{
    while (!local_tasks.push(task))
    {
        const size_t count = local_tasks.grab(offload_buffer);
        pool.global_tasks.pushMany(std::span(offload_buffer).first(count));
    }
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
        pushToLocal(found);

    for (const auto & found : runner.getFoundPollRequests())
        pool.poller.add(found);

    const size_t work_found = runner.getFoundTasks().size() + runner.getFoundPollRequests().size();
    return work_found == 0 ? 0 : work_found - 1;
}

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto task = pickTask())
    {
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

    if (const size_t moved = drain(); moved > 0)
        wakeOrSpawn(moved);

    if (pool.pipeline.step_profiler)
        pool.pipeline.step_profiler->addWorkIntervals(runner.takeWorkIntervals());
}

std::optional<Task> Worker::steal()
{
    Task stolen{};
    if (local_tasks.grab(std::span(&stolen, 1)) == 0)
        return std::nullopt;
    return stolen;
}

std::optional<Task> Worker::poll(int timeout_ms)
{
    if (pool.poller.pending() == 0)
        return std::nullopt;

    const auto fired = pool.poller.poll(timeout_ms);
    if (fired.empty())
        return std::nullopt;

    if (fired.size() > 1)
    {
        pool.global_tasks.pushMany(std::span(fired).subspan(1));
        wakeOrSpawn(fired.size() - 1);
    }

    return fired.front();
}

size_t Worker::drain()
{
    std::array<Task, LocalTasks::capacity> taken;
    size_t moved = 0;
    while (local_tasks.size() > 0)
    {
        if (const size_t count = local_tasks.grab(taken))
        {
            pool.global_tasks.pushMany(std::span(taken).first(count));
            moved += count;
        }
    }
    return moved;
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
