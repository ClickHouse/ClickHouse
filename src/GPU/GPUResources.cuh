#pragma once

#include <array>
#include <tuple>
#include <utility>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

namespace DB::GPU
{

/// What all the code on the GPU runs on and allocates device memory from, made by the `GPUDevice` at the start of the
/// server and destroyed at shutdown. `DeviceBuffer` allocates from `memory_pool`, and so do cuDF and thrust, through
/// the RMM resource that the `GPUDevice` puts over it, see `setCudfMemoryPool`.
///
/// It holds nothing of either standard library, as it is shared with the cuDF code, which nvcc builds against
/// libstdc++. RMM's resource objects are of libstdc++, so they stay on that side.
struct GPUResources
{
    cuda::stream_ref compute;
    cuda::stream_ref upload;
    cuda::stream_ref decompression;

    /// The device's default memory pool.
    cudaMemPool_t memory_pool = nullptr;

    /// Of the `GPUDevice` of the global `Context`.
    __host__ static const GPUResources & get();
};

/// Make cuDF and thrust allocate from `pool`, and back from it.
__host__ void setCudfMemoryPool(cudaMemPool_t pool);

__host__ void resetCudfMemoryPool();

}
