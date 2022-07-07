#pragma once

#include <base/types.h>
#include <Common/AllocationTrace.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
struct CurrentMemoryTracker
{
    /// Call the following functions before calling of corresponding operations with memory allocators.
    static [[nodiscard]] AllocationTrace alloc(Int64 size);
    static [[nodiscard]] AllocationTrace allocNoThrow(Int64 size);
    static [[nodiscard]] AllocationTrace realloc(Int64 old_size, Int64 new_size);

    /// This function should be called after memory deallocation.
    static [[nodiscard]] AllocationTrace free(Int64 size);
    static void check();

private:
    static [[nodiscard]] AllocationTrace allocImpl(Int64 size, bool throw_if_memory_exceeded);
};
