#pragma once

#include <Common/AllocatorWithMemoryTracking.h>

#include <vector>

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
using VectorWithMemoryTracking = std::vector<T, AllocatorWithMemoryTracking<T>>;

/// Like `VectorWithMemoryTracking`, but charges and enforces only the global/total memory tracker,
/// never the per-query/per-thread one. Use for process-wide containers mutated from arbitrary threads
/// (e.g. cross-thread scheduler queues), where attributing the memory to whichever query happens to do
/// the mutation would be wrong and could make a query spuriously fail on its own memory limit, while the
/// matching free on another thread makes accounting diverge. See `AllocatorWithMemoryTracking`'s `tracking_level`.
template <typename T>
using VectorWithGlobalMemoryTracking = std::vector<T, AllocatorWithMemoryTracking<T, VariableContext::User>>;

}
