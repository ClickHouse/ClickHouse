#pragma once

#include <Common/Wyrd/Strategies/Simulation/SimulationParams.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB::Wyrd
{

struct SimulatedThread;

/// Picks a random runnable thread and preempts it at a sync operation with the configured probability.
class Scheduler
{
public:
    explicit Scheduler(SimulationParams params_);

    SimulatedThread * pickNext();
    [[nodiscard]] bool shouldHold() const;
    [[nodiscard]] bool shouldWakeSpuriously() const;

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
    UnorderedMapWithMemoryTracking<const SimulatedThread *, size_t> lock_free_depth;

    /// Currently running thread information.
    SimulatedThread * current_thread = nullptr;
};

}
