#include <GPU/GPUKernelCache.h>

#if USE_GPU

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Core/Defines.h>

namespace CurrentMetrics
{
    extern const Metric GPUExpressionKernelCacheBytes;
    extern const Metric GPUExpressionKernelCacheCount;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
}

namespace DB::GPU
{

namespace
{

constexpr size_t max_kernel_cache_bytes = 256UL << 20;

void checkLoading(cudaError_t status, const std::string & what)
{
    if (status == cudaSuccess)
        return;
    cudaGetLastError();
    throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot {}: {}", what, cudaGetErrorString(status));
}

uint32_t blockSizeOf(cudaKernel_t kernel, const std::string & name)
{
    const void * function = reinterpret_cast<const void *>(kernel);
    cudaFuncAttributes attributes{};
    checkLoading(cudaFuncGetAttributes(&attributes, function), "tell the attributes of kernel " + name);

    uint32_t best_size = 0;
    int best_threads = 0;
    for (int size = 32; size <= attributes.maxThreadsPerBlock; size += 32)
    {
        int blocks = 0;
        checkLoading(cudaOccupancyMaxActiveBlocksPerMultiprocessor(&blocks, function, size, 0), "tell the occupancy of kernel " + name);
        if (blocks * size > best_threads)
        {
            best_threads = blocks * size;
            best_size = static_cast<uint32_t>(size);
        }
    }
    if (best_size == 0)
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Kernel {} runs in no block of the device", name);
    return best_size;
}

}

std::shared_ptr<GPUKernel> GPUKernel::load(const std::string & name, const std::string & ptx, std::vector<GPUElementType> column_types)
{
    auto kernel = std::make_shared<GPUKernel>();
    kernel->name = name;
    kernel->column_types = std::move(column_types);
    kernel->ptx_size = ptx.size();
    checkLoading(
        cudaLibraryLoadData(&kernel->library, ptx.c_str(), nullptr, nullptr, 0, nullptr, nullptr, 0), "load the PTX of kernel " + name);
    checkLoading(cudaLibraryGetKernel(&kernel->kernel, kernel->library, name.c_str()), "find kernel " + name + " in its PTX");
    kernel->block_size = blockSizeOf(kernel->kernel, name);
    return kernel;
}

GPUKernel::~GPUKernel()
{
    if (!library)
        return;

    cudaError_t status = cudaDeviceSynchronize();
    if (status == cudaSuccess)
        status = cudaLibraryUnload(library);
    if (status != cudaSuccess)
    {
        cudaGetLastError();
        LOG_ERROR(getLogger("GPUKernelCache"), "Cannot unload kernel {}: {}", name, cudaGetErrorString(status));
    }
}

GPUKernelCache::GPUKernelCache()
    : CacheBase(
          CurrentMetrics::GPUExpressionKernelCacheBytes,
          CurrentMetrics::GPUExpressionKernelCacheCount,
          max_kernel_cache_bytes,
          DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES)
{
}

}

#endif
