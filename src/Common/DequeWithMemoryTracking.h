#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <deque>

namespace DB
{

/// We do track memory allocations from standard containers by default, but we do it through
/// non-throwing methods of the `MemoryTracker` (see `trackMemory` in src/Common/memory.h).
/// The following scenario is possible:
///     1. Memory consumption is already very close to the limit
///     2. We're trying to create a huge vector/list (e.g. inside some aggregate function)
///     3. The allocation succeeds (potentially, OS overcommits memory)
///     4. We go over the limit
///     5. OOM killer rightfully kills the server process
/// To prevent this, we provide these `-WithMemoryTracking` aliases to standard containers that use the
/// `AllocatorWithMemoryTracking`, which tracks memory using throwing methods of the `MemoryTracker`.

template <typename T>
using DequeWithMemoryTracking = std::deque<T, AllocatorWithMemoryTracking<T>>;

}
