#include <GPU/GPUDevice.h>

#if USE_GPU

#include <limits>
#include <string_view>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

void throwGPUError(const char * message)
{
    clearDeviceError();
    throw Exception(ErrorCodes::GPU_ERROR, "{}", std::string_view(message));
}

bool isClickHouseException(const std::exception & exception)
{
    return dynamic_cast<const Exception *>(&exception) != nullptr;
}

const StreamRegistry & StreamRegistry::get()
{
    static const StreamRegistry registry = []
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

        cudaStream_t upload_stream = nullptr;
        cudaStream_t decompression_stream = nullptr;
        checkCuda(cudaStreamCreateWithFlags(&upload_stream, cudaStreamNonBlocking), "Cannot create a stream for uploads");
        checkCuda(cudaStreamCreateWithFlags(&decompression_stream, cudaStreamNonBlocking), "Cannot create a stream for decompression");

        StreamRegistry made;
        made.compute = rmm::cuda_stream_view{cudaStreamLegacy};
        made.upload = rmm::cuda_stream_view{upload_stream};
        made.decompression = rmm::cuda_stream_view{decompression_stream};
        return made;
    }();
    return registry;
}

void synchronizeStream(rmm::cuda_stream_view stream)
{
    checkCuda(cudaStreamSynchronize(stream.value()), "Cannot wait for a CUDA stream");
}

}

#endif
