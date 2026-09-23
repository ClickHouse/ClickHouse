#pragma once

#include "config.h"

#if USE_GPU

#include <cuda_runtime_api.h>

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

    void append(std::string_view host_bytes);

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

    void wait() const;

    void waitOn(cudaStream_t stream) const;

    bool isComplete() const;

private:
    cudaEvent_t event = nullptr;
};

}

#endif
