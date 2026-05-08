#pragma once

#include <base/types.h>
#include <Common/AllocationTrace.h>

/// Convenience methods, that use current thread's memory_tracker if it is available.
struct CurrentMemoryTracker
{
    /// Call the following functions before calling of corresponding operations with memory allocators.
    [[nodiscard]] static AllocationTrace alloc(Int64 size);
    [[nodiscard]] static AllocationTrace allocNoThrow(Int64 size);

    /// This function should be called after memory deallocation.
    [[nodiscard]] static AllocationTrace free(Int64 size);
    static void check();

    /// Throws MEMORY_LIMIT_EXCEEDED (if it's allowed to throw exceptions)
    static void injectFault();

private:
    [[nodiscard]] static AllocationTrace allocImpl(Int64 size, bool throw_if_memory_exceeded);

    /// Cold-path slow helpers used when there is no `current_thread` (i.e. before the
    /// `MainThreadStatus` is initialized or after teardown). Kept out-of-line so the
    /// hot path stays compact.
    [[nodiscard]] static AllocationTrace allocWithoutCurrentThread(Int64 size, bool throw_if_memory_exceeded);
    [[nodiscard]] static AllocationTrace freeWithoutCurrentThread(Int64 size);
};
