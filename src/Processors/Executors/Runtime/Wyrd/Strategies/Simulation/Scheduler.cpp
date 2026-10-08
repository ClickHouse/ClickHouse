#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/Scheduler.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>

#include <algorithm>

namespace DB::Runtime::Wyrd
{

namespace
{

bool contains(const std::vector<SimulatedThread *> & threads, const SimulatedThread * thread)
{
    return std::ranges::contains(threads, thread);
}

void moveRandom(std::vector<SimulatedThread *> & from, std::vector<SimulatedThread *> & to)
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
        current_thread_slice_left = 1 + randomNumber() % params.max_syncs_before_preempt;
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
    return randomEventEvery(params.lock_free_suspend_one_in);
}

bool Scheduler::shouldWakeSpuriously() const
{
    return randomEventEvery(params.spurious_wakeup_one_in);
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

void Scheduler::onHold()
{
    held_threads.push_back(current_thread);
    current_thread = nullptr;
    current_thread_slice_left = 0;
}

void Scheduler::onPark()
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

    ++progress_streak;
    if (progress_streak >= params.resume_after_progress_hints)
    {
        moveRandom(held_threads, runnable_threads);
        progress_streak = 0;
    }
}

}
