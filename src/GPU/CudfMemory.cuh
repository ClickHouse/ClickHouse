#pragma once

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// Makes cuDF allocate from `pool` with `cudaMallocAsync`, so it shares the memory pool of `DeviceBuffer` instead of
/// taking memory of its own with `cudaMalloc`.
__host__ void setCudfMemoryPool(cudaMemPool_t pool);

/// Gives cuDF back the memory resource RMM starts with.
__host__ void resetCudfMemoryPool();

}
