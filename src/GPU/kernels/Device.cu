#include "Common.cuh"

#include <cstring>

extern "C" int ch_gpu_device_count()
{
    int count = 0;
    const cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess)
    {
        /// No driver, no device, or a driver/runtime version mismatch. Not an error the
        /// caller has to handle -- a CPU-only host is a supported configuration -- but
        /// worth leaving a trace for whoever wonders why the GPU path went unused.
        ch_gpu::fail(CH_GPU_NO_DEVICE, "cudaGetDeviceCount", err);
        return 0;
    }
    ch_gpu::clearError();
    return count;
}

extern "C" int ch_gpu_device_name(int device, char * out, size_t out_size)
{
    if (out == nullptr || out_size == 0)
        return CH_GPU_INVALID_ARGUMENT;

    out[0] = '\0';

    cudaDeviceProp props;
    const cudaError_t err = cudaGetDeviceProperties(&props, device);
    if (err != cudaSuccess)
        return ch_gpu::fail(CH_GPU_NO_DEVICE, "cudaGetDeviceProperties", err);

    std::strncpy(out, props.name, out_size - 1);
    out[out_size - 1] = '\0';

    ch_gpu::clearError();
    return CH_GPU_OK;
}

extern "C" const char * ch_gpu_last_error()
{
    return ch_gpu::last_error;
}
