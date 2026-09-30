#include <GPU/GPUMemory.h>

#if USE_GPU

#include <GPU/GPUDevice.h>

#include <Common/Exception.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <map>
#include <mutex>
#include <utility>

namespace DB::GPU
{

namespace
{

/// Keeps pinned host buffers for reuse, as `cudaHostAlloc` is slow. The buffers are by power-of-two size class, and a
/// request takes one of exactly its class, so a large buffer is never kept for small requests. Buffers larger than
/// `max_pooled_capacity` are allocated to size and freed as soon as they are released, and at most `max_pooled_bytes`
/// are kept in all, so a large query does not keep its peak of unswappable memory after it ends.
class PinnedBufferPool
{
public:
    static PinnedBufferPool & instance()
    {
        static PinnedBufferPool pool;
        return pool;
    }

    std::pair<char *, size_t> acquire(size_t bytes)
    {
        const size_t capacity = bytes <= max_pooled_capacity ? std::bit_ceil(bytes) : bytes;

        if (capacity <= max_pooled_capacity)
        {
            std::lock_guard lock(mutex);
            const auto it = free_buffers.find(capacity);
            if (it != free_buffers.end())
            {
                char * const taken = it->second;
                pooled_bytes -= capacity;
                free_buffers.erase(it);
                return {taken, capacity};
            }
        }

        void * fresh = nullptr;
        checkCuda(cudaHostAlloc(&fresh, capacity, cudaHostAllocDefault), "Cannot allocate {} bytes of pinned host memory", capacity);

        return {static_cast<char *>(fresh), capacity};
    }

    void release(char * buffer, size_t capacity) noexcept
    {
        if (buffer == nullptr)
            return;

        if (capacity <= max_pooled_capacity)
        {
            std::lock_guard lock(mutex);
            if (pooled_bytes + capacity <= max_pooled_bytes)
            {
                free_buffers.emplace(capacity, buffer);
                pooled_bytes += capacity;
                return;
            }
        }

        cudaFreeHost(buffer);
    }

private:
    ~PinnedBufferPool() = default;

    /// The largest staging buffer of `GPUAccumulator`.
    static constexpr size_t max_pooled_capacity = 256UL * 1024 * 1024;
    static constexpr size_t max_pooled_bytes = 1024UL * 1024 * 1024;

    std::mutex mutex;
    std::multimap<size_t, char *> free_buffers TSA_GUARDED_BY(mutex);
    size_t pooled_bytes TSA_GUARDED_BY(mutex) = 0;
};

}

namespace
{

constexpr size_t min_capacity = 1024 * 1024;

}

PinnedBuffer::~PinnedBuffer()
{
    PinnedBufferPool::instance().release(memory, capacity);
}

PinnedBuffer::PinnedBuffer(PinnedBuffer && other) noexcept
    : memory(std::exchange(other.memory, nullptr))
    , capacity(std::exchange(other.capacity, 0))
    , used(std::exchange(other.used, 0))
{
}

PinnedBuffer & PinnedBuffer::operator=(PinnedBuffer && other) noexcept
{
    if (this != &other)
    {
        PinnedBufferPool::instance().release(memory, capacity);
        memory = std::exchange(other.memory, nullptr);
        capacity = std::exchange(other.capacity, 0);
        used = std::exchange(other.used, 0);
    }
    return *this;
}

void PinnedBuffer::reserve(size_t bytes)
{
    if (bytes <= capacity)
        return;

    const auto [fresh, fresh_capacity] = PinnedBufferPool::instance().acquire(std::max({bytes, capacity * 2, min_capacity}));

    if (used != 0)
        memcpy(fresh, memory, used);

    PinnedBufferPool::instance().release(memory, capacity);
    memory = fresh;
    capacity = fresh_capacity;
}

void PinnedBuffer::append(std::string_view bytes)
{
    if (bytes.empty())
        return;

    memcpy(grow(bytes.size()), bytes.data(), bytes.size());
}

void PinnedBuffer::appendFromDevice(const char * device_bytes, size_t bytes, rmm::cuda_stream_view stream)
{
    if (bytes == 0)
        return;

    checkCuda(
        cudaMemcpyAsync(grow(bytes), device_bytes, bytes, cudaMemcpyDeviceToHost, stream),
        "Cannot copy {} bytes back from the device",
        bytes);
}

char * PinnedBuffer::grow(size_t bytes)
{
    reserve(used + bytes);

    char * const destination = memory + used;
    used += bytes;
    return destination;
}


DeviceBuffer::~DeviceBuffer()
{
    if (memory != nullptr)
        cudaFreeAsync(memory, stream);
}

DeviceBuffer::DeviceBuffer(DeviceBuffer && other) noexcept
    : stream(other.stream)
    , memory(std::exchange(other.memory, nullptr))
    , capacity(std::exchange(other.capacity, 0))
    , used(std::exchange(other.used, 0))
{
}

DeviceBuffer & DeviceBuffer::operator=(DeviceBuffer && other) noexcept
{
    if (this != &other)
    {
        if (memory != nullptr)
            cudaFreeAsync(memory, stream);
        stream = other.stream;
        memory = std::exchange(other.memory, nullptr);
        capacity = std::exchange(other.capacity, 0);
        used = std::exchange(other.used, 0);
    }
    return *this;
}

void DeviceBuffer::reserve(size_t bytes)
{
    if (bytes <= capacity)
        return;

    DeviceBuffer grown(stream);
    grown.capacity = std::max(bytes, capacity * 2);

    void * fresh = nullptr;
    checkCuda(cudaMallocAsync(&fresh, grown.capacity, stream), "Cannot allocate {} bytes on the device", grown.capacity);
    grown.memory = static_cast<char *>(fresh);

    if (used != 0)
        checkCuda(
            cudaMemcpyAsync(grown.memory, memory, used, cudaMemcpyDeviceToDevice, stream),
            "Cannot move {} bytes within the device",
            used);
    grown.used = used;

    *this = std::move(grown);
}

void DeviceBuffer::append(std::string_view host_bytes)
{
    if (host_bytes.empty())
        return;

    checkCuda(
        cudaMemcpyAsync(grow(host_bytes.size()), host_bytes.data(), host_bytes.size(), cudaMemcpyHostToDevice, stream),
        "Cannot send {} bytes to the device",
        host_bytes.size());
}

char * DeviceBuffer::grow(size_t bytes)
{
    reserve(used + bytes);

    char * const destination = memory + used;
    used += bytes;
    return destination;
}


StreamPtr createStream()
{
    cudaStream_t stream = nullptr;
    checkCuda(cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking), "Cannot create a CUDA stream");
    return StreamPtr(stream);
}

EventPtr createEvent()
{
    cudaEvent_t event = nullptr;
    checkCuda(cudaEventCreateWithFlags(&event, cudaEventDisableTiming), "Cannot create a CUDA event");
    return EventPtr(event);
}

}

#endif
