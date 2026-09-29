#include <GPU/GPUMemory.h>

#if USE_GPU

#include <GPU/GPUDevice.h>

#include <Common/Exception.h>

#include <algorithm>
#include <cstring>
#include <map>
#include <mutex>
#include <utility>

namespace DB::GPU
{

namespace
{

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
        {
            std::lock_guard lock(mutex);
            const auto it = free_buffers.lower_bound(bytes);
            if (it != free_buffers.end())
            {
                const std::pair<char *, size_t> taken{it->second, it->first};
                pooled_bytes -= it->first;
                free_buffers.erase(it);
                return taken;
            }
        }

        void * fresh = nullptr;
        checkCuda(cudaHostAlloc(&fresh, bytes, cudaHostAllocDefault), "Cannot allocate {} bytes of pinned host memory", bytes);

        return {static_cast<char *>(fresh), bytes};
    }

    void release(char * buffer, size_t capacity) noexcept
    {
        if (buffer == nullptr)
            return;

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

    static constexpr size_t max_pooled_bytes = 16UL * 1024 * 1024 * 1024;

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
