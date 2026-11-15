#pragma once

#include "config.h"

#if USE_GPU

#include <Common/PODArray.h>

#include <base/defines.h>

#include <cuda_runtime_api.h>

#include <array>
#include <tuple>
#include <utility>

#include <cuda/memory_pool>
#include <cuda/stream_ref>

#include <boost/noncopyable.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>

namespace DB::GPU
{

/// Pinned host memory from a CUDA memory pool of host memory, `cuda::pinned_memory_pool` of CCCL, as `cudaHostAlloc`
/// is slow. The pool keeps the memory freed into it for reuse, up to its release threshold, so a large query does not
/// keep its peak of unswappable memory after it ends.
///
/// The `GPUDevice` makes the pool at the start of the server and drains it at shutdown, but the pool is a static, which
/// is destroyed only at the exit of the process: a buffer may be freed after the device is gone, by a query that
/// outlived the shutdown, and `PinnedAllocator::free` must not throw.
class PinnedMemoryPool : private boost::noncopyable
{
public:
    static PinnedMemoryPool & instance();

    /// Before anything is allocated.
    void configure(size_t release_threshold);

    /// Gives back to the system all the memory the pool keeps, and makes it keep none of what is freed later.
    void drain() noexcept;

    char * acquire(size_t bytes);

    /// Takes back a buffer `acquire` gave for `bytes`.
    void release(char * buffer, size_t bytes) noexcept;

private:
    PinnedMemoryPool() = default;

    /// Only set by `configure`, before any query.
    std::optional<cuda::pinned_memory_pool> pool;
};


/// Pinned host memory, which the device copies to and from without staging it, in the interface of `Allocator`. It
/// is taken from the `PinnedMemoryPool`, only while there is a `GPUDevice`, and counted by the memory tracker while it
/// is held, and not while it is kept in the pool.
///
/// The pool takes a buffer back without waiting for the device, so a buffer that a copy is queued to or from must not
/// be freed before the copy is done: see `waitOnFailure`.
class PinnedAllocator
{
public:
    void * alloc(size_t size, size_t alignment = 0);

    void free(void * buf, size_t size, size_t alignment = 0) noexcept;

    void * realloc(void * buf, size_t old_size, size_t new_size, size_t alignment = 0);

protected:
    static constexpr size_t getStackThreshold() { return 0; }

    static constexpr bool clear_memory = false;
};

using PinnedBuffer = PODArray<char, 4096, PinnedAllocator>;

/// Makes room for `bytes` more bytes at the end of `buffer` and returns where they start.
char * growBy(PinnedBuffer & buffer, size_t bytes);

void appendHostBytes(PinnedBuffer & buffer, std::string_view bytes);

/// Queues a copy of `bytes` bytes from the device to the end of `buffer`, which they fill once `stream` is done.
void appendFromDevice(PinnedBuffer & buffer, const char * device_bytes, size_t bytes, cuda::stream_ref stream);

inline std::string_view bytesOf(const PinnedBuffer & buffer)
{
    return {buffer.data(), buffer.size()};
}


/// Device memory from the device's default memory pool, allocated and freed in the order of `stream`.
class DeviceBuffer
{
public:
    explicit DeviceBuffer(cuda::stream_ref stream_) : stream(stream_) { }
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
    cuda::stream_ref stream;
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
