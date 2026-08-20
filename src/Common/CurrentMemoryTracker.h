#pragma once

#include <base/types.h>
#include <Common/AllocationTrace.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
struct CurrentMemoryTracker
{
    /// Call the following functions before calling of corresponding operations with memory allocators.
    [[nodiscard]] static AllocationTrace alloc(Int64 size);
    [[nodiscard]] static AllocationTrace allocNoThrow(Int64 size);

    /// Like allocNoThrow, but the tracker may raise MEMORY_LIMIT_EXCEEDED when
    /// the allocation size is at least `min_allocation_size_to_throw_on_memory_limit`.
    /// Use for the *throwing* operator new variants only; nothrow operator new
    /// is `noexcept` and must keep using `allocNoThrow`.
    [[nodiscard]] static AllocationTrace allocThrow(Int64 size);

    /// This function should be called after memory deallocation.
    [[nodiscard]] static AllocationTrace free(Int64 size);
    static void check();

    /// Throws MEMORY_LIMIT_EXCEEDED (if it's allowed to throw exceptions)
    static void injectFault();

    /// Minimum size, in bytes, of an `operator new` allocation that the memory
    /// tracker is allowed to refuse with MEMORY_LIMIT_EXCEEDED. Smaller
    /// allocations on this path are tracked but not refused. Does not affect
    /// the explicit `CurrentMemoryTracker::alloc` path, which always honours
    /// the memory limit regardless of size.
    ///
    /// @param value - 0 means disabled, internally use UINT64_MAX max so the hot path is a single comparison
    static void setMinAllocationSizeBytesToThrow(UInt64 value);
    static UInt64 getMinAllocationSizeBytesToThrow();

private:
    [[nodiscard]] static AllocationTrace allocImpl(Int64 size, bool enforce_memory_limit);
};
