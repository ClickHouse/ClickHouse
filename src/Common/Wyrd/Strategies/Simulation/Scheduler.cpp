#include <Common/Wyrd/Strategies/Simulation/Scheduler.h>
#include <Common/Wyrd/Strategies/Random.h>

#include <algorithm>

namespace DB::Wyrd::SimulationMode
{

namespace
{

SimulatedThread * popRandom(VectorWithMemoryTracking<SimulatedThread *> & threads)
{
    size_t index = randomInt(0, threads.size() - 1);
    SimulatedThread * thread = threads[index];
    threads[index] = threads.back();
    threads.pop_back();
    return thread;
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
    if (!lock_free_depth.contains(current_thread))
        return false;
    if (held_threads.size() >= params.max_suspended_threads)
        return false;
    if (std::ranges::none_of(runnable_threads, [this](const SimulatedThread * thread) { return lock_free_depth.contains(thread); }))
        return false;
    return randomBool(params.lock_free_suspend_probability);
}

bool Scheduler::shouldWakeSpuriously() const
{
    return randomBool(params.spurious_wakeup_probability);
}

void Scheduler::onSync()
{
    if (randomBool(params.preempt_probability))
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
    ++lock_free_depth[current_thread];
}

void Scheduler::onLockFreeLeave()
{
    --lock_free_depth[current_thread];
    if (lock_free_depth[current_thread] > 0)
        return;

    lock_free_depth.erase(current_thread);
    moveRandom(held_threads, runnable_threads);
}

void Scheduler::onProgress()
{
    if (!lock_free_depth.contains(current_thread))
        return;

    if (randomBool(params.resume_probability))
        moveRandom(held_threads, runnable_threads);
}

}
