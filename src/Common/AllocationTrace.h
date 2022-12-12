#pragma once
#include <cstddef>

/// This is a structure which is returned by MemoryTracker.
/// Methods onAlloc/onFree should be called after actual memory allocation if it succeed.
/// For now, it will only collect allocation trace with sample_probability.
struct AllocationTrace
{
    AllocationTrace() = default;
    explicit AllocationTrace(double sample_probability_);

    void onAlloc(void * ptr, size_t size) const;
    void onFree(void * ptr, size_t size) const;

    double sample_probability = 0;
};
