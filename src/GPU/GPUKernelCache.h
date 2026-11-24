#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUTypes.cuh>

#include <Common/CacheBase.h>
#include <Common/HashTable/Hash.h>

#include <cuda_runtime_api.h>

#include <base/defines.h>

#include <boost/noncopyable.hpp>

#include <memory>
#include <mutex>
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

    /// The next kernel in the list of the evicted ones of `GPUKernelCache`, guarded by its `evicted_mutex`. The list is
    /// linked through the kernels so that an eviction allocates nothing, see `GPUKernelCache::onEntryRemoval`.
    std::shared_ptr<GPUKernel> next_evicted;

    static std::shared_ptr<GPUKernel> load(const std::string & name, const std::string & ptx, std::vector<GPUElementType> column_types);

    /// Unloads the library without waiting for the device. Whoever launches the kernel holds it (through
    /// `CompiledGPUExpression::kernel`) until the stream it was launched on is synchronized, so when the last
    /// reference goes, no launch of the kernel can be queued or running. `cudaLibraryUnload` can still wait for the
    /// device to free the code, so `GPUKernelCache` never drops a kernel under its mutex.
    ~GPUKernel();
};

struct GPUKernelWeight
{
    size_t operator()(const GPUKernel & kernel) const { return kernel.ptx_size; }
};

/// The kernels compiled for expressions, by the hash of the expression and of the device. The limits are the server
/// settings `gpu_expression_kernel_cache_size` and `gpu_expression_kernel_cache_elements_size`, which are applied again
/// when the configuration is reloaded.
/// The evicted kernels are unloaded after the mutex of the cache is released: an unload under it would stall every
/// lookup and compilation of the server behind the work queued on the device. The methods of `CacheBase` that can
/// evict are hidden here by ones that unload what they evicted on return.
class GPUKernelCache : public CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>
{
    using Base = CacheBase<UInt128, GPUKernel, UInt128Hash, GPUKernelWeight>;

public:
    GPUKernelCache(size_t max_size_in_bytes, size_t max_count);

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto result = Base::getOrSet(key, std::forward<LoadFunc>(load_func));
        unloadEvicted();
        return result;
    }

    /// A kernel set again after its eviction could be linked twice into the list of the evicted ones, so the kernels
    /// are put in the cache only by `getOrSet`.
    void set(const Key & key, const MappedPtr & mapped) = delete;

    void setMaxSizeInBytes(size_t max_size_in_bytes);
    void setMaxCount(size_t max_count);

private:
    /// Runs under the mutex of the cache, so it only keeps the kernel for `unloadEvicted`. It must not throw: the policy
    /// has already subtracted the weight of the entry and erases it only after this returns. So it allocates nothing,
    /// it links the kernel into a list through `GPUKernel::next_evicted`.
    void onEntryRemoval(size_t weight_loss, const MappedPtr & kernel) override;

    void unloadEvicted();

    std::mutex evicted_mutex;
    MappedPtr evicted TSA_GUARDED_BY(evicted_mutex);
};

}

#endif
