#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUStreams.cuh>
#include <GPU/GPUTypes.cuh>

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

void synchronizeStream(rmm::cuda_stream_view stream);

}

#endif
