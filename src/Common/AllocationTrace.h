#pragma once
#include <cstddef>
#include <base/defines.h>

/// This is a structure which is returned by MemoryTracker.
/// Methods onAlloc/onFree should be called after actual memory allocation if it succeed.
/// For now, it will only collect allocation trace with sample_probability.
struct AllocationTrace
{
    AllocationTrace() = default;
    explicit AllocationTrace(double sample_probability_, bool is_null_ = false) : sample_probability(sample_probability_), is_null(is_null_)
    {
    }

    ALWAYS_INLINE void onAlloc(void * ptr, size_t size) const
    {
        if (likely(sample_probability <= 0))
            return;

        onAllocImpl(ptr, size);
    }

    ALWAYS_INLINE void onFree(void * ptr, size_t size) const
    {
        if (likely(sample_probability <= 0))
            return;

        onFreeImpl(ptr, size);
    }

    ALWAYS_INLINE bool isNull() const { return is_null; }

private:
    double sample_probability = 0;
    bool is_null = false;

    void onAllocImpl(void * ptr, size_t size) const;
    void onFreeImpl(void * ptr, size_t size) const;
};
