#include <GPU/GPUDevice.h>

#if USE_GPU

#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>

#include <string_view>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::ServerSetting
{
    extern const ServerSettingsUInt64 gpu_memory_pool_release_threshold;
    extern const ServerSettingsUInt64 gpu_pinned_memory_pool_release_threshold;
}

namespace DB::GPU
{

void throwGPUError(const char * message)
{
    clearDeviceError();
    throw Exception(ErrorCodes::GPU_ERROR, "{}", std::string_view(message));
}

void throwCudaError(cudaError_t status, const char * message)
{
    clearDeviceError();
    throw Exception(ErrorCodes::GPU_ERROR, "{}: {}", std::string_view(message), cudaGetErrorString(status));
}

bool isClickHouseException(const std::exception & exception)
{
    return dynamic_cast<const Exception *>(&exception) != nullptr;
}

GPUDeviceSettings GPUDeviceSettings::fromServerSettings(const ServerSettings & server_settings)
{
    return {
        .memory_pool_release_threshold = server_settings[ServerSetting::gpu_memory_pool_release_threshold],
        .pinned_memory_pool_release_threshold = server_settings[ServerSetting::gpu_pinned_memory_pool_release_threshold],
    };
}

namespace
{

cudaMemPool_t defaultMemoryPool()
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
    return pool;
}

}

GPUDevice::GPUDevice(const GPUDeviceSettings & settings)
    : gpu_resources{
          .compute = cuda::stream_ref{cudaStreamLegacy},
          .upload = {},
          .decompression = {},
          .memory_pool = defaultMemoryPool(),
      }
    , upload_stream(createStream())
    , decompression_stream(createStream())
{
    gpu_resources.upload = cuda::stream_ref{upload_stream.get()};
    gpu_resources.decompression = cuda::stream_ref{decompression_stream.get()};

    uint64_t release_threshold = settings.memory_pool_release_threshold;
    checkCuda(
        cudaMemPoolSetAttribute(gpu_resources.memory_pool, cudaMemPoolAttrReleaseThreshold, &release_threshold),
        "Cannot set the release threshold of the device's memory pool");

    PinnedMemoryPool::instance().configure(settings.pinned_memory_pool_release_threshold);

    setCudfMemoryPool(gpu_resources.memory_pool);
}

GPUDevice::~GPUDevice()
{
    try
    {
        checkCuda(cudaDeviceSynchronize(), "Cannot wait for the device");
        resetCudfMemoryPool();
        checkCuda(cudaMemPoolTrimTo(gpu_resources.memory_pool, 0), "Cannot return the memory of the device's memory pool");
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    PinnedMemoryPool::instance().drain();
}

GPUDevice & GPUDevice::instance()
{
    const ContextPtr context = Context::getGlobalContextInstance();
    if (!context)
        throw Exception(ErrorCodes::GPU_ERROR, "The GPU is used before the server has started");
    return context->getGPU();
}

const GPUResources & GPUResources::get()
{
    return GPUDevice::instance().resources();
}

void synchronizeStream(cuda::stream_ref stream)
{
    checkCuda(cudaStreamSynchronize(stream.get()), "Cannot wait for a CUDA stream");
}

}

#endif
