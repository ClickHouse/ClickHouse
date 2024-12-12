#pragma once

#include "config.h"

#if USE_GWP_ASAN

#include <gwp_asan/guarded_pool_allocator.h>
#include <Common/thread_local_rng.h>

#include <atomic>

namespace GWPAsan
{

extern gwp_asan::GuardedPoolAllocator GuardedAlloc;

bool isGWPAsanError(uintptr_t fault_address);

void printReport(uintptr_t fault_address);

extern std::atomic<bool> init_finished;

void initFinished();

extern std::atomic<double> force_sample_probability;

void setForceSampleProbability(double value);

/**
 * We'd like to postpone sampling allocations under the startup is finished. There are mainly
 * two reasons for that:
 *
 * - To avoid complex issues with initialization order
 * - Don't waste MaxSimultaneousAllocations on global objects as it's not useful
*/
inline bool shouldSample()
{
    return init_finished.load(std::memory_order_relaxed) && GuardedAlloc.shouldSample();
}

}

#endif
