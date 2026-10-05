#include <Processors/Executors/Runtime/V2/Engine/Worker.h>
#include <Interpreters/ProcessList.h>
#include <Processors/QueryPlan/Profiling/Execution/StepProfiler.h>

#include <ranges>

namespace DB
{

namespace Runtime::V2
{

Worker::Worker(size_t worker_id_, TaskScheduler & scheduler_, WorkersCoordinator & coordinator_, ExecutingPipeline & pipeline_, WorkerPool & pool_)
    : worker_id(worker_id_)
    , scheduler(scheduler_)
    , coordinator(coordinator_)
    , pipeline(pipeline_)
    , pool(pool_)
    , runner(pipeline, worker_id)
{
}

void Worker::run(WorkerSlot & slot, std::atomic_bool * yield_flag)
{
    while (auto task = pickTask())
    {
        if (coordinator.needsPoller())
            wakeOrSpawn(1);

        const size_t threads_needed = runTask(*task);

        if (threads_needed > 0)
            wakeOrSpawn(threads_needed);

        if (pipeline.process_list_element && !pipeline.process_list_element->checkTimeLimitSoft())
        {
            pipeline.cancel(IProcessor::CancelReason::CancelledByTimeout);
            coordinator.stop();
            break;
        }

        if (!slot.keepGoing())
            break;

        if (yield_flag && yield_flag->load())
            break;
    }

    if (pipeline.step_profiler)
        pipeline.step_profiler->addWorkIntervals(runner.getWorkIntervals());
}

std::optional<Task> Worker::pickTask()
{
    while (true)
    {
        if (coordinator.stopped())
            return std::nullopt;

        if (pipeline.hasReadyForRemoval())
            pipeline.removeReady();

        if (auto task = scheduler.tryPop(worker_id))
            return task;

        if (!coordinator.wait(worker_id))
            return std::nullopt;

        if (const size_t fired = scheduler.queued(worker_id); fired > 1)
            wakeOrSpawn(fired - 1);
    }
}

void Worker::wakeOrSpawn(size_t threads_needed)
{
    size_t woken = 0;
    if (coordinator.idle() > 0)
        woken = coordinator.wake(threads_needed);

    if (threads_needed > woken)
        pool.grow(threads_needed - woken);
}

size_t Worker::runTask(Task task)
{
    runner.run(task);

    for (const auto & found : runner.getFoundTasks() | std::views::reverse)
        scheduler.push(found, worker_id);

    for (const auto & found : runner.getFoundAsyncTasks())
        scheduler.push(found);

    const size_t work_found = runner.getFoundTasks().size() + runner.getFoundAsyncTasks().size();
    return work_found == 0 ? 0 : work_found - 1;
}

}

}
