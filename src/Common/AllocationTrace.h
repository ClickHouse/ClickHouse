#pragma once
#include <cstddef>
#include <base/defines.h>

/// This is a structure which is returned by MemoryTracker.
/// Methods onAlloc/onFree should be called after actual memory allocation if it succeed.
/// For now, it will only collect allocation trace with sample_probability.
struct AllocationTrace
{
    AllocationTrace() = default;
    explicit AllocationTrace(double sample_probability_) : sample_probability(sample_probability_) {}

    ALWAYS_INLINE void onAlloc(void * ptr, size_t size) const
    {
        updateThreadStatusOnAlloc(size);
        if (likely(sample_probability <= 0))
            return;

        onAllocImpl(ptr, size);
    }

    ALWAYS_INLINE void onFree(void * ptr, size_t size) const
    {
        updateThreadStatusOnFree(size);
        if (likely(sample_probability <= 0))
            return;

        onFreeImpl(ptr, size);
    }

private:
    double sample_probability = 0;

    void updateThreadStatusOnAlloc(size_t size) const;
    void updateThreadStatusOnFree(size_t size) const;
    void onAllocImpl(void * ptr, size_t size) const;
    void onFreeImpl(void * ptr, size_t size) const;
};
