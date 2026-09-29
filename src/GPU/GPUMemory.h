#pragma once

#include "config.h"

#if USE_GPU

#include <cuda_runtime_api.h>
#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <memory>
#include <string_view>
#include <type_traits>

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

    void appendFromDevice(const char * device_bytes, size_t bytes, rmm::cuda_stream_view stream);

    char * grow(size_t bytes);

    void clear() { used = 0; }

    std::string_view bytes() const { return {memory, used}; }
    const char * data() const { return memory; }
    char * data() { return memory; }
    size_t size() const { return used; }
    bool empty() const { return used == 0; }
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


struct StreamDeleter
{
    void operator()(cudaStream_t stream) const noexcept
    {
        cudaStreamSynchronize(stream);
        cudaStreamDestroy(stream);
    }
};

/// Owns a non-blocking CUDA stream; waits for its work before destroying it.
using StreamPtr = std::unique_ptr<std::remove_pointer_t<cudaStream_t>, StreamDeleter>;

StreamPtr createStream();

struct EventDeleter
{
    void operator()(cudaEvent_t event) const noexcept { cudaEventDestroy(event); }
};

/// Owns a CUDA event for holders that are moved around.
using EventPtr = std::unique_ptr<std::remove_pointer_t<cudaEvent_t>, EventDeleter>;

EventPtr createEvent();

}

#endif
