#include <GPU/GPUMemory.h>

#if USE_GPU

#include <GPU/GPUDevice.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>

#include <algorithm>
#include <cstring>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::GPU
{

PinnedMemoryPool & PinnedMemoryPool::instance()
{
    /// Outlives the `GPUDevice`, see the comment of the class.
    static PinnedMemoryPool instance;
    return instance;
}

void PinnedMemoryPool::configure(size_t release_threshold)
{
    if (pool)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The pool of pinned host memory is configured twice");

    pool.emplace(cuda::memory_pool_properties{.release_threshold = release_threshold});
}

void PinnedMemoryPool::drain() noexcept
{
    if (!pool)
        return;

    try
    {
        pool->set_attribute(cuda::memory_pool_attributes::release_threshold, 0);
        pool->trim_to(0);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

char * PinnedMemoryPool::acquire(size_t bytes)
{
    if (!pool)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Pinned host memory is taken before its pool is configured");

    return static_cast<char *>(pool->allocate_sync(bytes));
}

void PinnedMemoryPool::release(char * buffer, size_t bytes) noexcept
{
    if (buffer != nullptr)
        pool->deallocate_sync(buffer, bytes);
}


void * PinnedAllocator::alloc(size_t size, size_t alignment)
{
    if (alignment > cuda::mr::default_cuda_malloc_alignment)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Pinned host memory aligned to {} bytes, more than {}",
            alignment,
            cuda::mr::default_cuda_malloc_alignment);

    PinnedMemoryPool & pool = GPUDevice::instance().pinnedPool();

    auto trace = CurrentMemoryTracker::alloc(size);
    char * buffer = nullptr;
    try
    {
        buffer = pool.acquire(size);
    }
    catch (...)
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
        throw;
    }

    trace.onAlloc(buffer, size);
    return buffer;
}

void PinnedAllocator::free(void * buf, size_t size, size_t /* alignment */) noexcept
{
    PinnedMemoryPool::instance().release(static_cast<char *>(buf), size);

    auto trace = CurrentMemoryTracker::free(size);
    trace.onFree(buf, size);
}

void * PinnedAllocator::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
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

void appendFromDevice(PinnedBuffer & buffer, const char * device_bytes, size_t bytes, cuda::stream_ref stream)
{
    if (bytes == 0)
        return;

    checkCuda(
        cudaMemcpyAsync(growBy(buffer, bytes), device_bytes, bytes, cudaMemcpyDeviceToHost, stream.get()),
        "Cannot copy bytes back from the device");
}


DeviceBuffer::~DeviceBuffer()
{
    if (memory != nullptr)
        cudaFreeAsync(memory, stream.get());
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
            cudaFreeAsync(memory, stream.get());
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
        cudaMallocFromPoolAsync(&fresh, grown.capacity, GPUResources::get().memory_pool, stream.get()),
        "Cannot allocate device memory");
    grown.memory = static_cast<char *>(fresh);

    if (used != 0)
        checkCuda(
            cudaMemcpyAsync(grown.memory, memory, used, cudaMemcpyDeviceToDevice, stream.get()),
            "Cannot move bytes within the device");
    grown.used = used;

    *this = std::move(grown);
}

void DeviceBuffer::append(std::string_view host_bytes)
{
    if (host_bytes.empty())
        return;

    checkCuda(
        cudaMemcpyAsync(grow(host_bytes.size()), host_bytes.data(), host_bytes.size(), cudaMemcpyHostToDevice, stream.get()),
        "Cannot send bytes to the device");
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
