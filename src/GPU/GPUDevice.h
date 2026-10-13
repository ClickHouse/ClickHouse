#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUStreams.cuh>
#include <GPU/GPUTypes.cuh>

#include <Common/Exception.h>

#include <cuda_runtime_api.h>

#include <boost/noncopyable.hpp>

#include <fmt/format.h>

#include <exception>
#include <limits>
#include <string>
#include <utility>

namespace Poco::Util
{
class AbstractConfiguration;
}

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

/// Runs `body`, which queues work on `stream`. If it throws, waits for what it queued before the exception goes on, as
/// that work may still read or write memory that the unwinding frees, and that the pools give to other queries.
template <typename Body>
void waitOnFailure(rmm::cuda_stream_view stream, Body && body)
{
    try
    {
        body();
    }
    catch (...)
    {
        cudaStreamSynchronize(stream.value());
        clearDeviceError();
        throw;
    }
}

/// The `gpu` section of the server config.
struct GPUDeviceSettings
{
    /// Pinned host memory that `PinnedMemoryPool` keeps for reuse, in all and of one buffer.
    size_t max_pooled_pinned_bytes = 1024UL * 1024 * 1024;
    size_t max_pooled_pinned_capacity = 256UL * 1024 * 1024;

    /// Freed device memory that the device's default memory pool keeps instead of returning it to the driver. With
    /// the default, the pool grows to the peak of the queries and stays there until the server shuts down.
    uint64_t memory_pool_release_threshold = std::numeric_limits<uint64_t>::max();

    static GPUDeviceSettings fromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & prefix);
};

/// The GPU the server runs queries on and what is shared by all of them: the streams, the pinned host memory and the
/// device's memory pool, which cuDF allocates from as well. It is made at the start of the server from the `gpu`
/// section of its config, held in the global `Context`, and waits for the device and releases all of it at shutdown.
class GPUDevice : private boost::noncopyable
{
public:
    explicit GPUDevice(const GPUDeviceSettings & settings);
    ~GPUDevice();

    /// The device of the global `Context`; throws when the config has no `gpu` section.
    static GPUDevice & instance();

    const StreamRegistry & streams() const { return stream_registry; }

    PinnedMemoryPool & pinnedPool() { return PinnedMemoryPool::instance(); }

private:
    cudaMemPool_t memory_pool = nullptr;

    StreamPtr upload_stream;
    StreamPtr decompression_stream;
    StreamRegistry stream_registry;
};

}

#endif
