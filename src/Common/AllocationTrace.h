#pragma once

#include <cstddef>
#include <base/defines.h>

/// This is a structure which is returned by MemoryTracker.
/// Methods onAlloc/onFree should be called after actual memory allocation if it succeed.
/// For now, it will only collect allocation trace with sample_probability.
struct AllocationTrace
{
    AllocationTrace() = default;

    explicit AllocationTrace(double sample_probability_);

    ALWAYS_INLINE void onAlloc(void * ptr, size_t size) const
    {
        if (likely(sample_probability <= 0 && memory_dumper_enabled == false))
            return;

        onAllocImpl(ptr, size);
    }

    ALWAYS_INLINE void onFree(void * ptr, size_t size) const
    {
        if (likely(sample_probability <= 0 && memory_dumper_enabled == false))
            return;

        onFreeImpl(ptr, size);
    }

private:
    void onAllocImpl(void * ptr, size_t size) const;

    void onFreeImpl(void * ptr, size_t size) const;

    double sample_probability = 0;

    bool memory_dumper_enabled = false;
};
