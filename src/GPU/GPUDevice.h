#pragma once

#include "config.h"

#if USE_GPU

#include <GPU/GPUMemory.h>
#include <GPU/GPUResources.cuh>
#include <GPU/GPUTypes.cuh>

#include <cuda_runtime_api.h>

#include <boost/noncopyable.hpp>

namespace DB
{
struct ServerSettings;
}

namespace DB::GPU
{


inline void clearDeviceError()
{
    cudaGetLastError();
}

void synchronizeStream(cuda::stream_ref stream);

/// Runs `body`, which queues work on `stream`. If it throws, waits for what it queued before the exception goes on, as
/// that work may still read or write memory that the unwinding frees, and that the pools give to other queries.
template <typename Body>
void waitOnFailure(cuda::stream_ref stream, Body && body)
{
    try
    {
        body();
    }
    catch (...)
    {
        cudaStreamSynchronize(stream.get());
        clearDeviceError();
        throw;
    }
}

/// The `gpu_*` server settings; see their descriptions in `ServerSettings`.
struct GPUDeviceSettings
{
    /// Freed device memory that the device's default memory pool keeps instead of returning it to the driver.
    uint64_t memory_pool_release_threshold = 0;

    /// Freed pinned host memory that `PinnedMemoryPool` keeps instead of returning it to the system.
    size_t pinned_memory_pool_release_threshold = 0;

    static GPUDeviceSettings fromServerSettings(const ServerSettings & server_settings);
};

/// The GPU the server runs queries on and what is shared by all of them: the `GPUResources`, which are the streams and
/// the device's memory pool that cuDF allocates from as well, and the pinned host memory. It is made at the start of
/// the server when `enable_gpu` is set, from the `gpu_*` server settings, held in the global `Context`, and waits for
/// the device and releases all of it at shutdown.
class GPUDevice : private boost::noncopyable
{
public:
    explicit GPUDevice(const GPUDeviceSettings & settings);
    ~GPUDevice();

    /// The device of the global `Context`; throws when `enable_gpu` is not set.
    static GPUDevice & instance();

    const GPUResources & resources() const { return gpu_resources; }

    PinnedMemoryPool & pinnedPool() { return PinnedMemoryPool::instance(); }

private:
    /// Its pool is taken first, which checks that there is a device; the streams are set once they are made.
    GPUResources gpu_resources;

    StreamPtr upload_stream;
    StreamPtr decompression_stream;
};

}

#endif
