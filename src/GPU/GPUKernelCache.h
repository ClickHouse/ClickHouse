#pragma once

#include "config.h"

#if USE_GPU

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <cuda_runtime_api.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <string>

namespace DB::GPU
{

/// A kernel that `GPUExpressionCompiler` compiled into PTX, loaded on the device, and unloaded once nothing keeps it:
/// neither `GPUKernelCache` nor an expression of it, see `CompiledGPUExpression::kernel`.
struct GPUKernel : private boost::noncopyable
{
    std::string name;
    cudaLibrary_t library = nullptr;
    cudaKernel_t kernel = nullptr;
    /// Of the threads of a block that keep the most threads of a multiprocessor busy, a whole number of warps.
    uint32_t block_size = 0;
    /// What the kernel weighs in the cache.
    size_t ptx_size = 0;

    /// Kernel `name` of `ptx`, loaded on the device; throws where the device does not load it.
    static std::shared_ptr<GPUKernel> load(const std::string & name, const std::string & ptx);

    ~GPUKernel();
};

struct GPUKernelWeight
{
    size_t operator()(const GPUKernel & kernel) const { return kernel.ptx_size; }
};

/// The kernels compiled for the expressions of queries, by the hash of what they compute, bounded as the CPU's
/// `CompiledExpressionCache` is. `GPUDevice` holds it, so that the kernels it keeps are unloaded at the shutdown of the
/// server, while the device is there.
class GPUKernelCache : public CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>
{
public:
    GPUKernelCache();
};

}

#endif
