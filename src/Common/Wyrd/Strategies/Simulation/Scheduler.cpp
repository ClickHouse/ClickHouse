#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Scheduler.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <algorithm>

namespace DB::Runtime::Wyrd
{

namespace
{

bool contains(const VectorWithMemoryTracking<SimulatedThread *> & threads, const SimulatedThread * thread)
{
    return std::ranges::contains(threads, thread);
}

void moveRandom(VectorWithMemoryTracking<SimulatedThread *> & from, VectorWithMemoryTracking<SimulatedThread *> & to)
{
    if (!from.empty())
        to.push_back(popRandom(from));
}

}

Scheduler::Scheduler(SimulationParams params_)
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
    }
    return current_thread;
}

bool Scheduler::shouldHold() const
{
    if (!contains(threads_in_lock_free_scope, current_thread))
        return false;
    if (held_threads.size() >= params.max_suspended_threads)
        return false;
    if (std::ranges::none_of(runnable_threads, [this](const SimulatedThread * thread) { return contains(threads_in_lock_free_scope, thread); }))
        return false;
    return randomEvent(params.lock_free_suspend_probability);
}

bool Scheduler::shouldWakeSpuriously() const
{
    return randomEvent(params.spurious_wakeup_probability);
}

void Scheduler::onSync()
{
    if (randomEvent(params.preempt_probability))
        onYield();
}

void Scheduler::onYield()
{
    runnable_threads.push_back(current_thread);
    current_thread = nullptr;
}

void Scheduler::onHold()
{
    held_threads.push_back(current_thread);
    current_thread = nullptr;
}

void Scheduler::onPark()
{
    current_thread = nullptr;
}

void Scheduler::onExit()
{
    current_thread = nullptr;
}

void Scheduler::onRunnable(SimulatedThread * thread)
{
    runnable_threads.push_back(thread);
}

void Scheduler::onLockFreeEnter()
{
    threads_in_lock_free_scope.push_back(current_thread);
}

void Scheduler::onLockFreeLeave()
{
    std::erase(threads_in_lock_free_scope, current_thread);
    moveRandom(held_threads, runnable_threads);
}

void Scheduler::onProgress()
{
    if (!contains(threads_in_lock_free_scope, current_thread))
        return;

    if (randomEvent(params.resume_probability))
        moveRandom(held_threads, runnable_threads);
}

}
