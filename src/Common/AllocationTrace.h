#pragma once
#include <cstddef>

struct AllocationTrace
{
    AllocationTrace() = default;
    explicit AllocationTrace(double sample_probability_);

    void onAlloc(void * ptr, size_t size) const;
    void onFree(void * ptr, size_t size) const;

    double sample_probability = 0;
};
