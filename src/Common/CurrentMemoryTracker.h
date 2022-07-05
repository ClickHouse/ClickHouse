#pragma once

#include <base/types.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
namespace CurrentMemoryTracker
{
    /// Call the following functions before calling of corresponding operations with memory allocators.
    void alloc(Int64 size);
    void allocNoThrow(Int64 size);
    void realloc(Int64 old_size, Int64 new_size);
    void allocImpl(Int64 size, bool throw_if_memory_exceeded);

    /// This function should be called after memory deallocation.
    void free(Int64 size);
    void check();
}
