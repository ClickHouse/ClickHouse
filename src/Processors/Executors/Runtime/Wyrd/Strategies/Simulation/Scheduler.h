#pragma once

#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/SimulationParams.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;

/// Picks a random runnable thread and preempts it at a sync operation with the configured probability.
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
    VectorWithMemoryTracking<SimulatedThread *> runnable_threads;
    VectorWithMemoryTracking<SimulatedThread *> held_threads;
    VectorWithMemoryTracking<SimulatedThread *> threads_in_lock_free_scope;

    /// Currently running thread information.
    SimulatedThread * current_thread = nullptr;
};

}
