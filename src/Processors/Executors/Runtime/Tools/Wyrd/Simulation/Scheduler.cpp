#include <Processors/Executors/Runtime/Tools/Wyrd/Simulation/Scheduler.h>
#include <Processors/Executors/Runtime/Tools/Wyrd/Random.h>

namespace DB::Runtime::Wyrd
{

Scheduler::Scheduler(SchedulerParams params_)
    : params(params_)
{
}

SimulatedThread * Scheduler::pickNext()
{
    if (!current_thread)
    {
        if (runnable_threads.empty())
            return nullptr;

        current_thread = popRandom(runnable_threads);
        current_thread_slice_left = 1 + randomNumber() % params.time_slice;
    }
    return current_thread;
}

void Scheduler::onSync()
{
    --current_thread_slice_left;
    if (current_thread_slice_left == 0)
        onYield();
}

void Scheduler::onYield()
{
    runnable_threads.push_back(current_thread);
    current_thread = nullptr;
    current_thread_slice_left = 0;
}

void Scheduler::onSuspend()
{
    current_thread = nullptr;
    current_thread_slice_left = 0;
}

void Scheduler::onExit()
{
    current_thread = nullptr;
    current_thread_slice_left = 0;
}

void Scheduler::onRunnable(SimulatedThread * thread)
{
    runnable_threads.push_back(thread);
}

}
