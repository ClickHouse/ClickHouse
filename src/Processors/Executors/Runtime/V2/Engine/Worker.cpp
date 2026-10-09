#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/SpinWait.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>

#include <fmt/format.h>

#include <Interpreters/ProcessList.h>

#include <span>

namespace DB
{

namespace Runtime::V2
{

static constexpr size_t steal_rounds = 4;
static constexpr size_t max_take_from_global = LocalTasks::capacity / 2;

Worker::Worker(WorkerPool & pool_, size_t worker_id_)
    : pool(pool_)
    , worker_id(worker_id_)
    , runner(pool.pipeline, worker_id)
    , local_tasks(fmt::format("worker[{}].local_tasks", worker_id))
{
    Wyrd::setName(wakeups, fmt::format("worker[{}].wakeups", worker_id));
}

bool Worker::keepRunning(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    if (!slot.keepGoing() || pool.coordinator.isStopped())
        return false;

    if (yield_flag && yield_flag->load())
        return false;

    if (pool.pipeline.process_list_element && !pool.pipeline.process_list_element->checkTimeLimitSoft())
    {
        pool.pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
        pool.coordinator.stop();
        return false;
    }

    return true;
}

std::optional<Task> Worker::pickTask(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (true)
    {
        if (!keepRunning(slot, yield_flag))
            return std::nullopt;

        if (auto task = tryPickTask())
            return task;

        if (pool.pipeline.hasReadyForRemoval())
            pool.pipeline.removeReady();

        if (pool.poller.pending() > 0 && pool.coordinator.startPolling())
        {
            auto task = tryPickTaskBeforePark();
            if (!task)
            {
                const auto fired = pool.poller.poll(-1);
                if (!fired.empty())
                {
                    task = pickFromBatch(fired);
                    wakeFor(fired.size() - 1);
                }
            }
            pool.coordinator.stopPolling();
            if (task)
                return task;
        }
        else
        {
            UInt32 wakeups_before = wakeups.load();
            if (!pool.coordinator.startParking(worker_id))
                continue;

            auto task = tryPickTaskBeforePark();
            if (!task)
                futexWaitFetch(wakeups, wakeups_before);
            pool.coordinator.becomeNormal(worker_id);
            if (task)
                return task;
        }
    }
}

std::optional<Task> Worker::tryPickTask()
{
    if (auto task = local_tasks.popLifo())
        return task;

    if (const size_t count = pool.global_tasks.popManyFifo(std::span(tmp_task_buffer).first(max_take_from_global), pool.coordinator.registeredWorkers()))
        return pickFromBatch(std::span(tmp_task_buffer).first(count));

    if (pool.poller.pending() > 0 && !pool.coordinator.hasPoller() && pool.coordinator.startPolling())
    {
        const auto fired = pool.poller.poll(0);
        pool.coordinator.stopPolling();

        if (!fired.empty())
        {
            auto task = pickFromBatch(fired);
            wakeFor(fired.size() - 1);
            return task;
        }
    }

    if (pool.coordinator.startStealing())
    {
        auto task = tryStealTasks(steal_rounds);
        pool.coordinator.stopStealing();
        wakeFor(task.has_value());
        return task;
    }

    return std::nullopt;
}

std::optional<Task> Worker::tryPickTaskBeforePark()
{
    if (const size_t count = pool.global_tasks.popManyFifo(std::span(tmp_task_buffer).first(max_take_from_global), pool.coordinator.registeredWorkers()))
        return pickFromBatch(std::span(tmp_task_buffer).first(count));

    auto task = tryStealTasks(/*rounds=*/1);
    wakeFor(task.has_value());
    return task;
}

std::optional<Task> Worker::tryStealTasks(size_t rounds)
{
    const size_t workers = pool.workers.size();
    for (size_t round = 0; round < rounds; ++round)
    {
        const size_t start = Wyrd::randomNumber() % workers;
        for (size_t offset = 0; offset < workers; ++offset)
        {
            Worker & other = pool.workers[(start + offset) % workers];
            if (&other == this)
                continue;

            const UInt32 count = other.stealFifo(tmp_task_buffer);
            if (count == 0)
                continue;

            return pickFromBatch(std::span(tmp_task_buffer).first(count));
        }
    }

    return std::nullopt;
}

Task Worker::pickFromBatch(std::span<const Task> batch)
{
    pushManyLifo(batch.first(batch.size() - 1));
    return batch.back();
}

void Worker::pushManyLifo(std::span<const Task> tasks)
{
    const size_t kept = std::min<size_t>(tasks.size(), local_tasks.freeSlots());
    pool.global_tasks.pushManyFifo(tasks.first(tasks.size() - kept));
    local_tasks.pushManyLifo(tasks.last(kept));
}

void Worker::pushMany(std::span<const PollRequest> requests)
{
    pool.poller.pushMany(requests);
}

void Worker::wakeFor(Int64 extra_work)
{
    if (extra_work <= 0 || pool.coordinator.hasStealer())
        return;

    if (pool.coordinator.hasSleepers() || pool.coordinator.hasPoller())
        if (pool.coordinator.wakeOne())
            return;

    pool.grow(1);
}

void Worker::runTask(Task task)
{
    runner.run(task);

    const auto & new_compute_tasks = runner.getFoundTasks();
    pushManyLifo(new_compute_tasks);

    const auto & new_poll_requests = runner.getFoundPollRequests();
    pushMany(new_poll_requests);

    const Int64 work_found = new_compute_tasks.size() + new_poll_requests.size();
    wakeFor(work_found - 1);
}

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto task = pickTask(slot, yield_flag))
        runTask(*task);

    finishRun();
}

void Worker::finishRun()
{
    handOverTasks();

    if (pool.pipeline.step_profiler)
        pool.pipeline.step_profiler->addWorkIntervals(runner.takeWorkIntervals());
}

UInt32 Worker::stealFifo(std::span<Task> out)
{
    return local_tasks.stealFifo(out);
}

void Worker::handOverTasks()
{
    size_t moved = 0;
    while (local_tasks.size() > 0)
    {
        if (const size_t count = local_tasks.stealFifo(tmp_task_buffer))
        {
            pool.global_tasks.pushManyFifo(std::span(tmp_task_buffer).first(count));
            moved += count;
        }
    }
    wakeFor(moved);
}

void Worker::wake()
{
    wakeups.fetch_add(1);
    futexWakeOne(wakeups);
}

}

}
