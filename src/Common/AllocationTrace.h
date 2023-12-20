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

    ALWAYS_INLINE void onAlloc(void * ptr, size_t estimated_usable_size, size_t requested_size) const
    {
        /// Send the information to two separate tracers: TraceType::MemoryProfile and TraceType::MemorySample.
        /// Would it make sense to unify them into one? Doesn't seem so, see comment in HeapProfiler.h.
        /// Would it make sense to reuse the StackTrace if the allocation ends up traced by both tracers?
        /// In practice this should be too rare to be worth it.

        reportAllocToHeapProfiler(ptr, requested_size);

        if (unlikely(sample_probability > 0))
            onAllocImpl(ptr, estimated_usable_size);
    }

    /// estimated_usable_size doesn't necessarily match between onAlloc() and onFree().
    /// If allocatorSupportsUsableSize() is true, estimated_usable_size in onFree() must be >=
    /// requested_size in the corresponding onAlloc().
    ALWAYS_INLINE void onFree(void * ptr, size_t estimated_usable_size) const
    {
        reportFreeToHeapProfiler(ptr, estimated_usable_size);

        if (unlikely(sample_probability > 0))
            onFreeImpl(ptr, estimated_usable_size);
    }

private:
    double sample_probability = 0;

    void onAllocImpl(void * ptr, size_t size) const;
    void onFreeImpl(void * ptr, size_t size) const;

    /// These are defined in HeapProfiler.cpp so that their HeapProfiler::instance() calls are inlined.
    void reportAllocToHeapProfiler(void * ptr, size_t requested_size) const;
    void reportFreeToHeapProfiler(void * ptr, size_t estimated_usable_size) const;
};
