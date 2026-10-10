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
static constexpr size_t max_take_from_global = LocalQueue::capacity / 2;

Worker::Worker(WorkerPool & pool_, size_t worker_id_)
    : pool(pool_)
    , worker_id(worker_id_)
    , runner(pool.pipeline, pool.options, worker_id)
    , local_tasks(fmt::format("worker[{}].local_tasks", worker_id))
{
    Wyrd::setName(wakeups, fmt::format("worker[{}].wakeups", worker_id));
}

bool Worker::keepRunning(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    if (!slot.keepRunning() || pool.coordinator.isStopped())
        return false;

    if (yield_flag && yield_flag->load())
        return false;

    if (pool.options.process_list_element && !pool.options.process_list_element->checkTimeLimitSoft())
    {
        pool.pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
        pool.coordinator.stop();
        return false;
    }

    return true;
}

ExecutingProcessor * Worker::pick(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (true)
    {
        if (!keepRunning(slot, yield_flag))
            return nullptr;

        if (auto * picked = tryPick())
            return picked;

        if (pool.pipeline.hasReadyForRemoval())
            pool.pipeline.removeReady();

        if (pool.poller.pending() > 0 && pool.coordinator.startPolling())
        {
            if (anyWorkHasAppeared())
            {
                pool.coordinator.stopPolling();
                continue;
            }

            const auto fired = pool.poller.poll(-1);
            pool.coordinator.stopPolling();

            if (!fired.empty())
            {
                auto * picked = pickFromBatch(fired);
                wakeFor(fired.size() - 1);
                return picked;
            }
        }
        else
        {
            UInt32 wakeups_before = wakeups.load();
            if (!pool.coordinator.startParking(worker_id))
                continue;

            if (!anyWorkHasAppeared())
                futexWaitFetch(wakeups, wakeups_before);

            pool.coordinator.becomeNormal(worker_id);
        }
    }
}

ExecutingProcessor * Worker::tryPick()
{
    if (auto * picked = local_tasks.popLifo())
        return picked;

    if (const size_t count = pool.global_tasks.popManyFifo(std::span(tmp_buffer).first(max_take_from_global), pool.coordinator.registeredWorkers()))
        return pickFromBatch(std::span(tmp_buffer).first(count));

    if (pool.poller.pending() > 0 && !pool.coordinator.hasPoller() && pool.coordinator.startPolling())
    {
        const auto fired = pool.poller.poll(0);
        pool.coordinator.stopPolling();

        if (!fired.empty())
        {
            auto * picked = pickFromBatch(fired);
            wakeFor(fired.size() - 1);
            return picked;
        }
    }

    if (pool.coordinator.startStealing())
    {
        auto * picked = trySteal(steal_rounds);
        pool.coordinator.stopStealing();
        wakeFor(picked != nullptr);
        return picked;
    }

    return nullptr;
}

ExecutingProcessor * Worker::trySteal(size_t rounds)
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

            const UInt32 count = other.stealFifo(tmp_buffer);
            if (count == 0)
                continue;

            return pickFromBatch(std::span(tmp_buffer).first(count));
        }
    }

    return nullptr;
}

ExecutingProcessor * Worker::pickFromBatch(std::span<ExecutingProcessor * const> batch)
{
    pushManyLifo(batch.first(batch.size() - 1));
    return batch.back();
}

bool Worker::anyWorkHasAppeared() const
{
    if (pool.global_tasks.size() > 0)
        return true;

    for (const Worker & other : pool.workers)
        if (&other != this && other.local_tasks.size() > 0)
            return true;

    return false;
}

void Worker::pushManyLifo(std::span<ExecutingProcessor * const> many)
{
    const size_t kept = std::min<size_t>(many.size(), local_tasks.freeSlots());
    pool.global_tasks.pushManyFifo(many.first(many.size() - kept));
    local_tasks.pushManyLifo(many.last(kept));
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

void Worker::execute(ExecutingProcessor & executing)
{
    runner.run(executing);

    const auto & runnable = runner.getRunnable();
    pushManyLifo(runnable);

    const auto & poll_requests = runner.getPollRequests();
    pool.poller.pushMany(poll_requests);

    const auto & finished_processors = runner.getFinishedProcessors();
    pool.pipeline.recordAsFinished(finished_processors);

    const Int64 work_found = runnable.size() + poll_requests.size();
    wakeFor(work_found - 1);
}

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto * picked = pick(slot, yield_flag))
        execute(*picked);

    finishRun();
}

void Worker::finishRun()
{
    handOver();

    if (pool.options.step_profiler)
        pool.options.step_profiler->addWorkIntervals(runner.extractWorkIntervals());
}

UInt32 Worker::stealFifo(std::span<ExecutingProcessor *> out)
{
    return local_tasks.stealFifo(out);
}

void Worker::handOver()
{
    size_t moved = 0;
    while (local_tasks.size() > 0)
    {
        if (const size_t count = local_tasks.stealFifo(tmp_buffer))
        {
            pool.global_tasks.pushManyFifo(std::span(tmp_buffer).first(count));
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
