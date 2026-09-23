#pragma once

#include "config.h"

#if USE_GPU

#include <Common/Exception.h>

#include <cuda_runtime_api.h>

#include <fmt/format.h>

#include <exception>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int GPU_ERROR;
}

namespace DB::GPU
{

inline cudaStream_t deviceStream()
{
    return cudaStreamLegacy;
}

cudaStream_t copyStream();

cudaStream_t decompressStream();

inline void clearDeviceError()
{
    cudaGetLastError();
}

template <typename... Args>
void checkCuda(cudaError_t status, fmt::format_string<Args...> what, Args &&... args)
{
    if (status != cudaSuccess)
    {
        clearDeviceError();
        throw Exception(ErrorCodes::GPU_ERROR, "{}: {}", fmt::format(what, std::forward<Args>(args)...), cudaGetErrorString(status));
    }
}

template <typename Body, typename... Args>
auto onDevice(Body && body, fmt::format_string<Args...> what, Args &&... args)
{
    try
    {
        return body();
    }
    catch (const Exception &)
    {
        clearDeviceError();
        throw;
    }
    catch (const std::exception & e)
    {
        clearDeviceError();
        throw Exception(ErrorCodes::GPU_ERROR, "{}: {}", fmt::format(what, std::forward<Args>(args)...), e.what());
    }
}

void initializeDevice();

void synchronizeDevice();

const String & deviceProbeError();

}

#endif
