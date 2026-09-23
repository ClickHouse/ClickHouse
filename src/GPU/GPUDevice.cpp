#include <GPU/GPUDevice.h>

#if USE_GPU

#include <limits>
#include <mutex>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

void initializeDevice()
{
    static std::once_flag once;
    std::call_once(once, []
    {
        int count = 0;
        checkCuda(cudaGetDeviceCount(&count), "Cannot count the CUDA devices");
        if (count == 0)
            throw Exception(ErrorCodes::GPU_ERROR, "There is no CUDA device");

        checkCuda(cudaFree(nullptr), "Cannot initialize a CUDA context");

        int device = 0;
        checkCuda(cudaGetDevice(&device), "Cannot tell the current CUDA device");

        cudaMemPool_t pool = nullptr;
        checkCuda(cudaDeviceGetDefaultMemPool(&pool, device), "Cannot get the device's default memory pool");

        uint64_t release_threshold = std::numeric_limits<uint64_t>::max();
        checkCuda(
            cudaMemPoolSetAttribute(pool, cudaMemPoolAttrReleaseThreshold, &release_threshold),
            "Cannot tell the device's memory pool to keep freed memory");
    });
}

const StreamRegistry & StreamRegistry::get()
{
    static const StreamRegistry registry = []
    {
        initializeDevice();

        StreamRegistry made;
        made.compute = cudaStreamLegacy;
        checkCuda(cudaStreamCreateWithFlags(&made.upload, cudaStreamNonBlocking), "Cannot create a stream for uploads");
        checkCuda(cudaStreamCreateWithFlags(&made.decompression, cudaStreamNonBlocking), "Cannot create a stream for decompression");
        return made;
    }();
    return registry;
}

void synchronizeDevice()
{
    checkCuda(cudaStreamSynchronize(StreamRegistry::get().compute), "Cannot wait for the device to finish");
}

const String & deviceProbeError()
{
    static const String error = []
    {
        try
        {
            initializeDevice();
            return String{};
        }
        catch (const Exception & e)
        {
            return e.message();
        }
    }();

    return error;
}

}

#endif
