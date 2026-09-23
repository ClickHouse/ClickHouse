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

/// Every kernel and copy back is queued on the device's default stream, in the order they were
/// queued. It is also cuDF's default stream, which `initializeCudf` checks.
inline cudaStream_t deviceStream()
{
    return cudaStreamLegacy;
}

/// A stream of its own for uploads, which the default stream does not wait for: a copy queued
/// here runs on the copy engine while the default stream's kernels run, and what depends on it
/// waits for a `DeviceEvent` recorded after it.
cudaStream_t copyStream();

/// A stream of its own for nvcomp, which the default stream does not wait for either: a batch is
/// expanded here while the default stream's kernels group the last one.
cudaStream_t decompressStream();

/// A failed CUDA call leaves its error in the calling thread, where the next kernel launch on that
/// thread reads it back and fails as well - a query that ran out of device memory would take the
/// next query on its thread down with it. Reading the error takes it out.
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

/// Runs a call into the cuDF side and reports what it throws as a `GPU_ERROR` that says what was
/// being done. A ClickHouse exception passes as it is.
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

/// Probes the device and tells its default memory pool to keep what is freed, so that a buffer
/// that grows and shrinks per query does not go back to the driver each time. Runs once; called by
/// everything that first touches the device.
void initializeDevice();

void synchronizeDevice();

/// Empty when there is a usable CUDA device, and the reason there is not when there is not. Probed
/// once, so that planning a query does not have to talk to the driver.
const String & deviceProbeError();

}

#endif
