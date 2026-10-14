#include <GPU/GPUMemory.h>

#if USE_GPU

#include <GPU/GPUDevice.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

namespace
{

/// What `cudaHostAlloc` aligns to, and the smallest size class, as `cudaHostAlloc` gives whole pages anyway.
constexpr size_t pinned_alignment = 4096;

}

PinnedMemoryPool & PinnedMemoryPool::instance()
{
    /// Never destroyed, see the comment of the class.
    static PinnedMemoryPool * const pool = new PinnedMemoryPool;
    return *pool;
}

void PinnedMemoryPool::configure(size_t max_pooled_bytes_, size_t max_pooled_capacity_)
{
    std::lock_guard lock(mutex);
    max_pooled_bytes = max_pooled_bytes_;
    max_pooled_capacity = max_pooled_capacity_;
}

void PinnedMemoryPool::drain() noexcept
{
    std::lock_guard lock(mutex);
    for (const auto & [capacity, buffer] : free_buffers)
        cudaFreeHost(buffer);
    free_buffers.clear();
    pooled_bytes = 0;
    max_pooled_bytes = 0;
}

size_t PinnedMemoryPool::capacityFor(size_t bytes) const
{
    if (bytes > max_pooled_capacity)
        return bytes;
    return std::bit_ceil(std::max(bytes, pinned_alignment));
}

char * PinnedMemoryPool::acquire(size_t bytes)
{
    const size_t capacity = capacityFor(bytes);

    if (bytes <= max_pooled_capacity)
    {
        std::lock_guard lock(mutex);
        const auto it = free_buffers.find(capacity);
        if (it != free_buffers.end())
        {
            char * const taken = it->second;
            pooled_bytes -= capacity;
            free_buffers.erase(it);
            return taken;
        }
    }

    void * fresh = nullptr;
    checkCuda(cudaHostAlloc(&fresh, capacity, cudaHostAllocDefault), "Cannot allocate {} bytes of pinned host memory", capacity);
    return static_cast<char *>(fresh);
}

void PinnedMemoryPool::release(char * buffer, size_t bytes) noexcept
{
    if (buffer == nullptr)
        return;

    if (bytes <= max_pooled_capacity)
    {
        const size_t capacity = capacityFor(bytes);

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


void * PinnedAllocator::alloc(size_t size, size_t alignment)
{
    if (alignment > pinned_alignment)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pinned host memory aligned to {} bytes, more than {}", alignment, pinned_alignment);

    PinnedMemoryPool & pool = GPUDevice::instance().pinnedPool();
    const size_t capacity = pool.capacityFor(size);

    auto trace = CurrentMemoryTracker::alloc(capacity);
    char * buffer = nullptr;
    try
    {
        buffer = pool.acquire(size);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(capacity);
        throw;
    }

    trace.onAlloc(buffer, capacity);
    return buffer;
}

void PinnedAllocator::free(void * buf, size_t size, size_t /* alignment */) noexcept
{
    PinnedMemoryPool & pool = PinnedMemoryPool::instance();
    const size_t capacity = pool.capacityFor(size);

    pool.release(static_cast<char *>(buf), size);

    auto trace = CurrentMemoryTracker::free(capacity);
    trace.onFree(buf, capacity);
}

void * PinnedAllocator::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    const PinnedMemoryPool & pool = PinnedMemoryPool::instance();
    if (pool.capacityFor(old_size) == pool.capacityFor(new_size))
        return buf;

    void * const fresh = alloc(new_size, alignment);
    memcpy(fresh, buf, std::min(old_size, new_size));
    free(buf, old_size, alignment);
    return fresh;
}

char * growBy(PinnedBuffer & buffer, size_t bytes)
{
    const size_t start = buffer.size();
    buffer.resize(start + bytes);
    return buffer.data() + start;
}

void appendHostBytes(PinnedBuffer & buffer, std::string_view bytes)
{
    if (bytes.empty())
        return;

    buffer.insert(bytes.data(), bytes.data() + bytes.size());
}

void appendFromDevice(PinnedBuffer & buffer, const char * device_bytes, size_t bytes, rmm::cuda_stream_view stream)
{
    if (bytes == 0)
        return;

    checkCuda(
        cudaMemcpyAsync(growBy(buffer, bytes), device_bytes, bytes, cudaMemcpyDeviceToHost, stream),
        "Cannot copy {} bytes back from the device",
        bytes);
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
    checkCuda(
        cudaMallocFromPoolAsync(&fresh, grown.capacity, GPUDevice::instance().memoryPool(), stream),
        "Cannot allocate {} bytes on the device",
        grown.capacity);
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
