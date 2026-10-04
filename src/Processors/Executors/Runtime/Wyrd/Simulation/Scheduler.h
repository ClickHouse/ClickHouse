#pragma once

#include <base/types.h>

#include <vector>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;

struct SchedulerParams
{
    size_t time_slice = 15;
};

/// Picks a random runnable thread and preempts it after a random slice of sync operations.
class Scheduler
{
public:
    explicit Scheduler(SchedulerParams params_);

    SimulatedThread * pickNext();
    void onSync();
    void onYield();
    void onSuspend();
    void onExit();
    void onRunnable(SimulatedThread * thread);

private:
    const SchedulerParams params;
    std::vector<SimulatedThread *> runnable_threads;
    SimulatedThread * current_thread = nullptr;
    size_t current_thread_slice_left = 0;
};

}
