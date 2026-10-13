#include <Common/PerCPUMemory.h>

namespace DB
{

/// Process-wide instance used by CurrentMemoryTracker. Constructed before main(); allocations
/// before its initialiser runs see cpu_count == 0, so sync() returns false without indexing the
/// (empty) array - a normal per-thread flush.
#if defined(OS_LINUX)
PerCPUMemory per_cpu_memory{PerCPUMemory::numberOfCPUs(), PerCPUMemory::DEFAULT_BUDGET, PerCPUMemory::DEFAULT_THREAD_BUFFER};
#else
PerCPUMemory per_cpu_memory;
#endif

}
