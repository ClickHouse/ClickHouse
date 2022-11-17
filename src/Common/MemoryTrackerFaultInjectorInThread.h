#pragma once

#include <cstdint>
#include <Common/VariableContext.h>

/// Fault injector for MemoryTracker.
struct MemoryTrackerFaultInjectorInThread
{
private:
    static thread_local uint64_t counter;
    static thread_local double probability;
    double previous_probability = 0;

public:
    /// @probability - probability for the allocation request to fail.
    explicit MemoryTrackerFaultInjectorInThread(double probability_ = 1);
    ~MemoryTrackerFaultInjectorInThread();

    MemoryTrackerFaultInjectorInThread(const MemoryTrackerFaultInjectorInThread &) = delete;
    MemoryTrackerFaultInjectorInThread & operator=(const MemoryTrackerFaultInjectorInThread &) = delete;

    /// Does this allocation should fail?
    static bool faulty();
};
