#include <GPU/GPUKernelCache.h>

#if USE_GPU

#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#include <cuda.h>
#include <cudaTypedefs.h>

#include <array>

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

void checkLoading(cudaError_t status, const std::string & what)
{
    if (status == cudaSuccess)
        return;
    cudaGetLastError();
    throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot {}: {}", what, cudaGetErrorString(status));
}

/// The linker of the driver, reached through the runtime, which is linked statically and loads the driver itself.
struct DriverLinker
{
    PFN_cuLinkCreate_v6050 create = nullptr;
    PFN_cuLinkAddData_v6050 add_data = nullptr;
    PFN_cuLinkComplete_v5050 complete = nullptr;
    PFN_cuLinkDestroy_v5050 destroy = nullptr;

    DriverLinker()
    {
        entryPoint("cuLinkCreate", 6050, create);
        entryPoint("cuLinkAddData", 6050, add_data);
        entryPoint("cuLinkComplete", 5050, complete);
        entryPoint("cuLinkDestroy", 5050, destroy);
    }

    template <typename Function>
    static void entryPoint(const char * symbol, unsigned int version, Function & function)
    {
        void * address = nullptr;
        cudaDriverEntryPointQueryResult found = cudaDriverEntryPointSymbolNotFound;
        checkLoading(cudaGetDriverEntryPointByVersion(symbol, &address, version, cudaEnableDefault, &found), std::string("find ") + symbol + " in the CUDA driver");
        if (found != cudaDriverEntryPointSuccess || !address)
            throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "The CUDA driver has no {} of version {}", symbol, version);
        function = reinterpret_cast<Function>(address);
    }

    static const DriverLinker & instance()
    {
        static const DriverLinker linker;
        return linker;
    }
};

/// Links the PTX into the machine code of the device that is current for the thread, and returns its size. The code
/// is owned by `state`, which must outlive the load of it.
struct LinkedCubin
{
    CUlinkState state = nullptr;
    void * data = nullptr;
    size_t size = 0;

    LinkedCubin(const std::string & name, const std::string & ptx)
    {
        const DriverLinker & linker = DriverLinker::instance();

        /// The driver links for the context that is current for the thread, which the runtime makes current lazily.
        int device = 0;
        checkLoading(cudaGetDevice(&device), "tell the current device to link kernel " + name);
        checkLoading(cudaSetDevice(device), "make the context of the device current to link kernel " + name);

        std::array<char, 4096> error_log{};
        std::array<CUjit_option, 2> options{CU_JIT_ERROR_LOG_BUFFER, CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES};
        std::array<void *, 2> values{error_log.data(), reinterpret_cast<void *>(error_log.size())};

        check(linker.create(static_cast<unsigned>(options.size()), options.data(), values.data(), &state), name, "start linking", error_log.data());
        try
        {
            /// The PTX is passed with its terminating zero, as the driver expects of text.
            check(
                linker.add_data(state, CU_JIT_INPUT_PTX, const_cast<char *>(ptx.c_str()), ptx.size() + 1, name.c_str(), 0, nullptr, nullptr),
                name, "add the PTX", error_log.data());
            check(linker.complete(state, &data, &size), name, "link the PTX", error_log.data());
        }
        catch (...)
        {
            linker.destroy(state);
            throw;
        }
    }

    ~LinkedCubin()
    {
        if (state)
            DriverLinker::instance().destroy(state);
    }

    static void check(CUresult status, const std::string & name, const char * what, const char * log)
    {
        if (status == CUDA_SUCCESS)
            return;
        throw Exception(ErrorCodes::CANNOT_COMPILE_CODE, "Cannot {} of kernel {}: CUDA driver error {}: {}", what, name, static_cast<int>(status), log);
    }
};

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

    LinkedCubin cubin(name, ptx);
    kernel->cubin_size = cubin.size;
    checkLoading(
        cudaLibraryLoadData(&kernel->library, cubin.data, nullptr, nullptr, 0, nullptr, nullptr, 0), "load the code of kernel " + name);
    checkLoading(cudaLibraryGetKernel(&kernel->kernel, kernel->library, name.c_str()), "find kernel " + name + " in its PTX");
    kernel->block_size = blockSizeOf(kernel->kernel, name);
    return kernel;
}

GPUKernel::~GPUKernel()
{
    if (!library)
        return;

    const cudaError_t status = cudaLibraryUnload(library);
    if (status != cudaSuccess)
    {
        cudaGetLastError();
        LOG_ERROR(getLogger("GPUKernelCache"), "Cannot unload kernel {}: {}", name, cudaGetErrorString(status));
    }
}

GPUKernelCache::GPUKernelCache(size_t max_size_in_bytes, size_t max_count)
    : CacheBase(CurrentMetrics::GPUExpressionKernelCacheBytes, CurrentMetrics::GPUExpressionKernelCacheCount, max_size_in_bytes, max_count)
{
}

}

#endif
