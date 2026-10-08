#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulationParams.h>

#include <vector>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;

/// Picks a random runnable thread and preempts it after a random slice of sync operations.
class Scheduler
{
public:
    explicit Scheduler(SimulationParams params_);

    SimulatedThread * pickNext();
    bool shouldHold() const;
    bool shouldWakeSpuriously() const;

    void onSync();
    void onYield();
    void onHold();
    void onPark();
    void onExit();
    void onRunnable(SimulatedThread * thread);
    void onLockFreeEnter();
    void onLockFreeLeave();
    void onProgress();

private:
    const SimulationParams params;

    /// Thread states.
    std::vector<SimulatedThread *> runnable_threads;
    std::vector<SimulatedThread *> held_threads;
    std::vector<SimulatedThread *> threads_in_lock_free_scope;

    /// Currently running thread information.
    SimulatedThread * current_thread = nullptr;
    size_t current_thread_slice_left = 0;
    size_t progress_streak = 0;
};

}
