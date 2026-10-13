#pragma once

#include "config.h"

#if USE_GPU

#include <Common/PODArray.h>

#include <base/defines.h>

#include <cuda_runtime_api.h>
#include <rmm/cuda_stream_view.hpp>

#include <boost/noncopyable.hpp>

#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <string_view>
#include <type_traits>

namespace DB::GPU
{

/// Keeps pinned host buffers for reuse, as `cudaHostAlloc` is slow. The buffers are by power-of-two size class, and a
/// request takes one of exactly its class, so a large buffer is never kept for small requests. Buffers larger than
/// `max_pooled_capacity` are allocated to size and freed as soon as they are released, and at most `max_pooled_bytes`
/// are kept in all, so a large query does not keep its peak of unswappable memory after it ends.
///
/// The `GPUDevice` sizes it at the start of the server and drains it at shutdown, but it lives as long as the process:
/// a buffer may be freed after the device is gone, by a query that outlived the shutdown, and `PinnedAllocator::free`
/// must not throw.
class PinnedMemoryPool : private boost::noncopyable
{
public:
    static PinnedMemoryPool & instance();

    /// Before anything is allocated.
    void configure(size_t max_pooled_bytes_, size_t max_pooled_capacity_);

    /// Frees the buffers it keeps and keeps no more of those released later.
    void drain() noexcept;

    /// What a buffer of `bytes` takes: the size class for one that is pooled, or `bytes` for one that is not.
    size_t capacityFor(size_t bytes) const;

    /// A buffer of `capacityFor(bytes)`.
    char * acquire(size_t bytes);

    /// Takes back a buffer `acquire` gave for `bytes`.
    void release(char * buffer, size_t bytes) noexcept;

private:
    PinnedMemoryPool() = default;

    /// Only set by `configure`, before any query.
    size_t max_pooled_capacity = 0;

    std::mutex mutex;
    size_t max_pooled_bytes TSA_GUARDED_BY(mutex) = 0;
    std::multimap<size_t, char *> free_buffers TSA_GUARDED_BY(mutex);
    size_t pooled_bytes TSA_GUARDED_BY(mutex) = 0;
};


/// Pinned host memory, which the device copies to and from without staging it, in the interface of `Allocator`. It
/// is taken from the `PinnedMemoryPool`, only while there is a `GPUDevice`, and counted by the memory tracker while it
/// is held, by the capacity of its size class, and not while it is kept in the pool.
///
/// The pool takes a buffer back without waiting for the device, so a buffer that a copy is queued to or from must not
/// be freed before the copy is done: see `waitOnFailure`.
class PinnedAllocator
{
public:
    void * alloc(size_t size, size_t alignment = 0);

    void free(void * buf, size_t size, size_t alignment = 0) noexcept;

    /// Keeps the buffer when the new size is of the same size class.
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
void appendFromDevice(PinnedBuffer & buffer, const char * device_bytes, size_t bytes, rmm::cuda_stream_view stream);

inline std::string_view bytesOf(const PinnedBuffer & buffer)
{
    return {buffer.data(), buffer.size()};
}


/// Device memory from the device's default memory pool, allocated and freed in the order of `stream`.
class DeviceBuffer
{
public:
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
    rmm::cuda_stream_view stream;
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
