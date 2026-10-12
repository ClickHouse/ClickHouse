#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Processors/Executors/Runtime/V2/Engine/WorkerPool.h>
#include <Processors/Executors/Runtime/V2/Pipeline/ProcessorRunOrder.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Enabled.h>
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

namespace
{

constexpr size_t steal_rounds = 4;
constexpr size_t max_take_from_global = LocalQueue::capacity / 2;
constexpr size_t max_take_from_async_ready = LocalQueue::capacity / 2;
constexpr double shuffle_probability = 0.125;
constexpr double early_offload_probability = 0.125;

void injectShuffle(std::span<ExecutingProcessor *> batch)
{
    if constexpr (Wyrd::enabled())
    {
        if (batch.size() < 2 || !Wyrd::randomEvent(shuffle_probability))
            return;

        for (size_t index = batch.size(); index > 1; --index)
            std::swap(batch[index - 1], batch[Wyrd::randomNumberBetween(0, index - 1)]);
    }
}

size_t injectEarlyOffload(size_t room)
{
    if constexpr (Wyrd::enabled())
    {
        if (room == 0 || !Wyrd::randomEvent(early_offload_probability))
            return room;

        return Wyrd::randomNumberBetween(0, room);
    }
    else
    {
        return room;
    }
}

}

Worker::Worker(WorkerPool & pool_, size_t worker_id_)
    : pool(pool_)
    , worker_id(worker_id_)
    , runner(pool.pipeline, pool.options, worker_id)
    , local_tasks(fmt::format("worker[{}].local_tasks", worker_id))
{
    Wyrd::setName(wakeups, "worker[{}].wakeups", worker_id);
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

        /// Parking logic
        {
            UInt32 wakeups_before = wakeups.load();
            if (!pool.coordinator.startParking(worker_id))
                continue;

            if (!anyWorkHasAppeared())
            {
                metrics.parks += 1;
                futexWaitFetch(wakeups, wakeups_before);
            }

            pool.coordinator.becomeNormal(worker_id);
        }
    }
}

ExecutingProcessor * Worker::tryPick()
{
    if (const size_t count = pool.async_ready_tasks.popManyLifo(std::span(tmp_buffer).first(max_take_from_async_ready), pool.coordinator.registeredWorkers()))
    {
        metrics.async_picks += 1;
        return pickFromBatch(std::span(tmp_buffer).first(count), pool.async_ready_tasks.size() > 0);
    }

    if (auto * picked = local_tasks.popLifo())
    {
        metrics.local_picks += 1;
        return picked;
    }

    if (const size_t count = pool.global_tasks.popManyFifo(std::span(tmp_buffer).first(max_take_from_global), pool.coordinator.registeredWorkers()))
    {
        metrics.global_picks += 1;
        return pickFromBatch(std::span(tmp_buffer).first(count), pool.global_tasks.size() > 0);
    }

    if (pool.coordinator.startStealing())
    {
        const size_t count = trySteal(steal_rounds);
        pool.coordinator.stopStealing();
        metrics.steal_picks += count > 0;
        metrics.steal_misses += count == 0;
        return count > 0 ? pickFromBatch(std::span(tmp_buffer).first(count), true) : nullptr;
    }

    return nullptr;
}

ExecutingProcessor * Worker::pickFromBatch(std::span<ExecutingProcessor *> batch, bool source_not_empty)
{
    injectShuffle(batch);
    pushManyLifo(batch.first(batch.size() - 1));
    if (batch.size() > 1 || source_not_empty)
        wakeWorker();
    return batch.back();
}

size_t Worker::trySteal(size_t rounds)
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

            metrics.stolen_records += count;
            return count;
        }
    }

    return 0;
}

bool Worker::anyWorkHasAppeared() const
{
    if (pool.async_ready_tasks.size() > 0 || pool.global_tasks.size() > 0)
        return true;

    for (const Worker & other : pool.workers)
        if (&other != this && other.local_tasks.size() > 0)
            return true;

    return false;
}

void Worker::pushManyLifo(std::span<ExecutingProcessor * const> many)
{
    const size_t room = std::min<size_t>(many.size(), local_tasks.freeSlots());
    const size_t kept = injectEarlyOffload(room);
    metrics.offloads += many.size() - kept;
    pool.global_tasks.pushManyFifo(many.first(many.size() - kept));
    local_tasks.pushManyLifo(many.last(kept));
}

void Worker::wakeWorker()
{
    switch (pool.wakeWorker())
    {
        case WakeOutcome::Skipped:
            metrics.wake_skips += 1;
            return;
        case WakeOutcome::Woken:
            metrics.wakes += 1;
            return;
        case WakeOutcome::Spawned:
            metrics.spawns += 1;
            return;
        case WakeOutcome::AllBusy:
            metrics.no_op_wakes += 1;
            return;
    }
}

void Worker::execute(ExecutingProcessor & executing)
{
    runner.run(executing);

    const auto & runnable = runner.getRunnable();
    pushManyLifo(runnable);

    const auto & poll_requests = runner.getPollRequests();
    if (!poll_requests.empty())
    {
        pool.poller.pushMany(poll_requests);
        pool.spawnPoller();
    }

    const auto & finished_processors = runner.getFinishedProcessors();
    pool.pipeline.recordAsFinished(finished_processors);

    if (runnable.size() > 1 && !allLightWork(runnable))
        wakeWorker();
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
    if (moved > 0)
        wakeWorker();
}

void Worker::wake()
{
    wakeups.fetch_add(1);
    futexWakeOne(wakeups);
}

const WorkerMetrics & Worker::getMetrics() const
{
    return metrics;
}

}

}
