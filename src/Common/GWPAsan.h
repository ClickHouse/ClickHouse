#pragma once

#include "config.h"

#if USE_GWP_ASAN

#include <gwp_asan/guarded_pool_allocator.h>
#include <Common/thread_local_rng.h>

#include <atomic>
#include <random>

namespace GWPAsan
{

extern gwp_asan::GuardedPoolAllocator GuardedAlloc;

bool isGWPAsanError(uintptr_t fault_address);

void printReport(uintptr_t fault_address);

extern std::atomic<double> force_sample_probability;

void setForceSampleProbability(double value);

inline bool shouldForceSample()
{
    std::bernoulli_distribution dist(force_sample_probability.load(std::memory_order_relaxed));
    return dist(thread_local_rng);
}

}

#endif
