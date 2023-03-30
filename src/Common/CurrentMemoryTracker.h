#pragma once

#include <base/types.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
struct CurrentMemoryTracker
{
    /// Call the following functions before calling of corresponding operations with memory allocators.
    static void alloc(Int64 size);
    static void allocNoThrow(Int64 size);
    static void realloc(Int64 old_size, Int64 new_size);

    /// This function should be called after memory deallocation.
    static void free(Int64 size);
    static void check();

private:
    static void allocImpl(Int64 size, bool throw_if_memory_exceeded);
};
