#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <cuda_runtime_api.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>
#include <vector>

namespace DB::GPU
{

struct GPUKernel : private boost::noncopyable
{
    std::string name;
    cudaLibrary_t library = nullptr;
    cudaKernel_t kernel = nullptr;
    uint32_t block_size = 0;
    std::vector<GPUElementType> column_types;
    size_t ptx_size = 0;

    static std::shared_ptr<GPUKernel> load(const std::string & name, const std::string & ptx, std::vector<GPUElementType> column_types);

    /// Unloads the library without waiting for the device. Whoever launches the kernel holds it (through
    /// `CompiledGPUExpression::kernel`) until the stream it was launched on is synchronized, so when the last
    /// reference goes, no launch of the kernel can be queued or running. The destructor runs also under the mutex of
    /// the cache, when an eviction drops the last reference, and a wait for the device there would stall every
    /// compilation of the server behind the work of all queries.
    ~GPUKernel();
};

struct GPUKernelWeight
{
    size_t operator()(const GPUKernel & kernel) const { return kernel.ptx_size; }
};

/// The kernels compiled for expressions, by the hash of the expression and of the device. The limits are the server
/// settings `gpu_expression_kernel_cache_size` and `gpu_expression_kernel_cache_elements_size`, which are applied again
/// when the configuration is reloaded.
class GPUKernelCache : public CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>
{
public:
    GPUKernelCache(size_t max_size_in_bytes, size_t max_count);
};

}

#endif
