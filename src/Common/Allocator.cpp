#include <Common/Allocator.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/VersionNumber.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <Common/AllocationInterceptors.h>

#include <base/errnoToString.h>
#include <base/getPageSize.h>

#include <Poco/Environment.h>
#include <Poco/Logger.h>
#include <sys/mman.h> /// MADV_POPULATE_WRITE


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int LOGICAL_ERROR;
}

}

namespace
{

using namespace DB;

#if defined(MADV_POPULATE_WRITE)
/// Address passed to madvise is required to be aligned to the page boundary.
auto adjustToPageSize(void * buf, size_t len, size_t page_size)
{
    const uintptr_t address_numeric = reinterpret_cast<uintptr_t>(buf);
    const size_t next_page_start = ((address_numeric + page_size - 1) / page_size) * page_size;
    return std::make_pair(reinterpret_cast<void *>(next_page_start), len - (next_page_start - address_numeric));
}

bool madviseSupportsMadvPopulateWrite()
{
    /// Can't rely for detection on madvise(MADV_POPULATE_WRITE) == EINVAL, since this will be returned in many other cases.
    VersionNumber linux_version(Poco::Environment::osVersion());
    VersionNumber supported_version(5, 14, 0);
    bool is_supported = linux_version >= supported_version;
    if (!is_supported)
        LOG_TRACE(getLogger("Allocator"), "Disabled page pre-faulting (kernel is too old).");
    return is_supported;
}

const bool is_supported_by_kernel = madviseSupportsMadvPopulateWrite();

#endif

void prefaultPages([[maybe_unused]] void * buf_, [[maybe_unused]] size_t len_)
{
#if defined(MADV_POPULATE_WRITE)
    if (len_ < POPULATE_THRESHOLD)
        return;

    if (unlikely(!is_supported_by_kernel))
        return;

    auto [buf, len] = adjustToPageSize(buf_, len_, staticPageSize);
    ::madvise(buf, len, MADV_POPULATE_WRITE);
#endif
}

template <bool clear_memory, bool populate>
void * allocNoTrack(size_t size, size_t alignment)
{
    void * buf;
    if (likely(alignment <= MALLOC_MIN_ALIGNMENT))
    {
        if constexpr (clear_memory)
            buf = __real_calloc(size, 1);
        else
            buf = __real_malloc(size);

        if (unlikely(nullptr == buf))
            throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot malloc {}.", ReadableSize(size));
    }
    else
    {
        buf = nullptr;
        int res = __real_posix_memalign(&buf, alignment, size);

        if (unlikely(0 != res))
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate memory (posix_memalign) {}.", ReadableSize(size));

        if constexpr (clear_memory)
            memset(buf, 0, size);
    }

    if constexpr (populate)
        prefaultPages(buf, size);

    return buf;
}

void freeNoTrack(void * buf)
{
    __real_free(buf);
}

void checkSize(size_t size)
{
    /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
    if (unlikely(size >= 0x8000000000000000ULL))
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

}


/// Constant is chosen almost arbitrarily, what I observed is 128KB is too small, 1MB is almost indistinguishable from 64MB and 1GB is too large.
extern const size_t POPULATE_THRESHOLD = std::max(Int64{16 * 1024 * 1024}, ::getPageSize());

template <bool clear_memory_, bool populate>
void * Allocator<clear_memory_, populate>::alloc(size_t size, size_t alignment)
{
    checkSize(size);
    auto trace = CurrentMemoryTracker::alloc(size);
    void * ptr = allocNoTrack<clear_memory_, populate>(size, alignment);
    trace.onAlloc(ptr, size);
    return ptr;
}


template <bool clear_memory_, bool populate>
void Allocator<clear_memory_, populate>::free(void * buf, size_t size)
{
    try
    {
        checkSize(size);
        freeNoTrack(buf);
        auto trace = CurrentMemoryTracker::free(size);
        trace.onFree(buf, size);
    }
    catch (...)
    {
        DB::tryLogCurrentException("Allocator::free");
        throw;
    }
}

template <bool clear_memory_, bool populate>
void * Allocator<clear_memory_, populate>::realloc(void * buf, size_t old_size, size_t new_size, size_t alignment)
{
    checkSize(new_size);

    if (old_size == new_size)
    {
        /// nothing to do.
        /// BTW, it's not possible to change alignment while doing realloc.
        return buf;
    }

    if (likely(alignment <= MALLOC_MIN_ALIGNMENT))
    {
        /// Resize malloc'd memory region with no special alignment requirement.
        /// Realloc can do 2 possible things:
        /// - expand existing memory region
        /// - allocate new memory block and free the old one
        /// Because we don't know which option will be picked we need to make sure there is enough
        /// memory for all options
        auto trace_alloc = CurrentMemoryTracker::alloc(new_size);

        void * new_buf = __real_realloc(buf, new_size);
        if (unlikely(nullptr == new_buf))
        {
            [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(new_size);
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Allocator: Cannot realloc from {} to {}",
                ReadableSize(old_size),
                ReadableSize(new_size));
        }

        auto trace_free = CurrentMemoryTracker::free(old_size);
        trace_free.onFree(buf, old_size);
        trace_alloc.onAlloc(new_buf, new_size);
        buf = new_buf;

        if constexpr (clear_memory)
            if (new_size > old_size)
                memset(reinterpret_cast<char *>(buf) + old_size, 0, new_size - old_size);
    }
    else
    {
        /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        free(buf, old_size);
        buf = new_buf;
    }

    if constexpr (populate)
        prefaultPages(buf, new_size);

    return buf;
}


template class Allocator<false, false>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<true, true>;
