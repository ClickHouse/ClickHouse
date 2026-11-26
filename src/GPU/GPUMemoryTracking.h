#pragma once

#include <cstddef>

/// The device memory a query holds, accounted to its thread group (`ThreadGroup::gpu_memory_usage`) and sent to the client
/// with the profile events, which shows it in the progress bar, as the memory of the host. Every allocation of device
/// memory goes through these: `DeviceBuffer` and the memory resource of RMM, which cuDF allocates from.
///
/// Only plain types: the host code of the GPU islands, built against libstdc++, calls them.

namespace DB::GPU
{

/// Accounts `bytes` at `memory` to the query of the current thread and to the queries it is nested in. Device memory
/// allocated outside of a query, at the start of the server, is not accounted.
void trackDeviceAllocation(const void * memory, size_t bytes) noexcept;

/// Takes back what `trackDeviceAllocation` accounted for `memory`, from whichever thread frees it, even after the query
/// is gone, as the indexes on the device outlive the query that loaded them.
void trackDeviceDeallocation(const void * memory) noexcept;

}

#if !defined(__CUDACC__)

#include <memory>
#include <vector>

namespace DB
{
class ThreadGroup;
}

namespace DB::GPU
{

/// The outermost thread groups (the queries) that hold device memory now, each once; for `GPUActivitySampler`.
std::vector<std::shared_ptr<ThreadGroup>> groupsHoldingDeviceMemory();

}

#endif
