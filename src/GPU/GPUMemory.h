#pragma once

#include "config.h"

#if USE_GPU

#include <cuda_runtime_api.h>
#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <string_view>

namespace DB::GPU
{

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

    /// Queues a copy of `bytes` bytes from the device on `stream` to the end of the buffer. They
    /// land when the stream reaches the copy; until then the buffer is not to be read or grown.
    void appendFromDevice(const char * device_bytes, size_t bytes, rmm::cuda_stream_view stream);

    char * grow(size_t bytes);

    void clear() { used = 0; }

    std::string_view bytes() const { return {memory, used}; }
    const char * data() const { return memory; }
    char * data() { return memory; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }
    /// Bytes that fit after `size` without growing.
    size_t available() const { return capacity - used; }

private:
    char * memory = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


class DeviceBuffer
{
public:
    DeviceBuffer() = default;
    explicit DeviceBuffer(rmm::cuda_stream_view stream_) : stream(stream_) { }
    ~DeviceBuffer();

    DeviceBuffer(DeviceBuffer && other) noexcept;
    DeviceBuffer & operator=(DeviceBuffer && other) noexcept;

    DeviceBuffer(const DeviceBuffer &) = delete;
    DeviceBuffer & operator=(const DeviceBuffer &) = delete;

    void reserve(size_t bytes);

    void append(std::string_view host_bytes);

    char * grow(size_t bytes);

    void clear() { used = 0; }

    const char * data() const { return memory; }
    char * data() { return memory; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }

private:
    rmm::cuda_stream_view stream{cudaStreamLegacy};
    char * memory = nullptr;
    size_t capacity = 0;
    size_t used = 0;
};


/// A non-blocking stream of the device's own, for work that must not queue behind the shared
/// streams of `StreamRegistry`. Destroying it waits for what was queued on it.
class DeviceStream
{
public:
    DeviceStream();
    ~DeviceStream();

    DeviceStream(DeviceStream && other) noexcept;
    DeviceStream & operator=(DeviceStream && other) noexcept;

    DeviceStream(const DeviceStream &) = delete;
    DeviceStream & operator=(const DeviceStream &) = delete;

    rmm::cuda_stream_view get() const { return rmm::cuda_stream_view{stream}; }

    void synchronize() const;

private:
    /// Owned, so kept as the handle it is created and destroyed by.
    cudaStream_t stream = nullptr;
};


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
    void record(rmm::cuda_stream_view stream);

    void wait() const;

    void waitOn(rmm::cuda_stream_view stream) const;

    bool isComplete() const;

private:
    cudaEvent_t event = nullptr;
};

}

#endif
