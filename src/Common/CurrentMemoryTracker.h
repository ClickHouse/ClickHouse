#pragma once

#include <base/types.h>
#include <Common/AllocationTrace.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
namespace CurrentMemoryTracker
{
    [[nodiscard]] AllocationTrace alloc(Int64 size);
    [[nodiscard]] AllocationTrace allocNoThrow(Int64 size);
    [[nodiscard]] AllocationTrace realloc(Int64 old_size, Int64 new_size);
    [[nodiscard]] AllocationTrace free(Int64 size);
    void check();
}
