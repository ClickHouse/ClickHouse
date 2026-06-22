#pragma once

#include <atomic>
#include <base/types.h>


class StepMemoryTracker
{
public:

    explicit StepMemoryTracker() = default;

    /// Atomically adds delta to held_amount and recomputes the peak
    void addDelta(Int64 delta)
    {
        Int64 cur_amount = held_amount.fetch_add(delta) + delta;
        Int64 prev_peak = peak.load(std::memory_order_relaxed);

        while(cur_amount > peak.load(std::memory_order_relaxed)
        && !peak.compare_exchange_weak(prev_peak, cur_amount, std::memory_order_release, std::memory_order_relaxed))
        {}
    }

    Int64 getPeak() { return peak.load(std::memory_order_acquire); }

private:

    std::atomic<Int64> held_amount{0};
    std::atomic<Int64> peak{0};
};
