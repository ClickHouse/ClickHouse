#pragma once

/// Busy-waiting helpers.
/// jemalloc: `spin.h`.

#include <allocator/Common.h>

#include <cstdint>
#include <sched.h>

namespace jemalloc
{

/// A CPU relaxation hint (`pause` on x86_64; elsewhere a volatile self-assignment, like jemalloc's fallback).
/// jemalloc: spin_cpu_spinwait
ALLOCATOR_ALWAYS_INLINE void spinCPUSpinWait()
{
    if constexpr (config::have_cpu_spin_wait)
    {
#if defined(__x86_64__)
        __asm__ volatile("pause");
#endif
    }
    else
    {
        volatile int x = 0;
        x = x;
    }
}

/// Exponential spinning: 1, 2, 4, 8, 16 spin-waits, then `sched_yield` on every following call.
/// jemalloc: spin_t
struct Spin
{
    unsigned iteration = 0;

    /// jemalloc: spin_adaptive
    void adaptive()
    {
        volatile uint32_t i;

        if (iteration < 5)
        {
            for (i = 0; i < (1U << iteration); i = i + 1)
                spinCPUSpinWait();
            ++iteration;
        }
        else
        {
            sched_yield();
        }
    }
};

}
