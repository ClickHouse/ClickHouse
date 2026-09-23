#pragma once

#include "config.h"

#if USE_GPU

#include <cuda_runtime_api.h>

#include <cstddef>
#include <string_view>

namespace DB::GPU
{

/// Host memory the device can read straight out of, taken from a pool: pinning a page is a call
/// into the driver, so a query that fills and drains the same staging buffer per block should pay
/// for it once.
class PinnedBuffer
{
public:
    PinnedBuffer() = default;
    ~PinnedBuffer();

    PinnedBuffer(PinnedBuffer && other) noexcept;
    PinnedBuffer & operator=(PinnedBuffer && other) noexcept;

    PinnedBuffer(const PinnedBuffer &) = delete;
    PinnedBuffer & operator=(const PinnedBuffer &) = delete;

    void reserve(size_t bytes);

    void append(std::string_view bytes);

    /// Makes room for `bytes` more and answers where they go, for the device to write them.
    char * grow(size_t bytes);

    void clear() { used = 0; }

    std::string_view bytes() const { return {memory, used}; }
    const char * data() const { return memory; }
    char * data() { return memory; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }

private:
    char * memory = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


/// A run of bytes in device memory, filled from the host. Grows by doubling and keeps its capacity
/// across `clear`, so a pipe that fills and drains it repeatedly allocates once. Every operation
/// on it is queued on its stream, the default one unless told otherwise.
class DeviceBuffer
{
public:
    DeviceBuffer() = default;
    explicit DeviceBuffer(cudaStream_t stream_) : stream(stream_) { }
    ~DeviceBuffer();

    DeviceBuffer(DeviceBuffer && other) noexcept;
    DeviceBuffer & operator=(DeviceBuffer && other) noexcept;

    DeviceBuffer(const DeviceBuffer &) = delete;
    DeviceBuffer & operator=(const DeviceBuffer &) = delete;

    void reserve(size_t bytes);

    /// Queues a copy from host memory onto the end. From pinned memory it is asynchronous, and the
    /// memory has to stay put until a `DeviceEvent` recorded after it says it has landed.
    void append(std::string_view host_bytes);

    /// Makes room for `bytes` more and answers where they go, for a kernel to write them.
    char * grow(size_t bytes);

    void clear() { used = 0; }

    const char * data() const { return memory; }
    char * data() { return memory; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }

private:
    cudaStream_t stream = cudaStreamLegacy;
    char * memory = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


/// A point in a stream, to wait for everything queued before it.
class DeviceEvent
{
public:
    DeviceEvent();
    ~DeviceEvent();

    DeviceEvent(DeviceEvent && other) noexcept;
    DeviceEvent & operator=(DeviceEvent && other) noexcept;

    DeviceEvent(const DeviceEvent &) = delete;
    DeviceEvent & operator=(const DeviceEvent &) = delete;

    void record();
    void record(cudaStream_t stream);

    /// Blocks the host until the point is reached.
    void wait() const;

    /// Makes everything queued on `stream` after this wait for the point, without blocking the host.
    void waitOn(cudaStream_t stream) const;

    /// Whether the point has been reached. True for an event never recorded.
    bool isComplete() const;

private:
    cudaEvent_t event = nullptr;
};

}

#endif
