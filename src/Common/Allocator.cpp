#include <Common/Allocator.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/GWPAsan.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <base/errnoToString.h>
#include <base/getPageSize.h>

#include <Poco/Logger.h>
#include <sys/mman.h> /// MADV_POPULATE_WRITE

namespace ProfileEvents
{
    extern const Event GWPAsanAllocateSuccess;
    extern const Event GWPAsanAllocateFailed;
    extern const Event GWPAsanFree;
}

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
#endif

void prefaultPages([[maybe_unused]] void * buf_, [[maybe_unused]] size_t len_)
{
#if defined(MADV_POPULATE_WRITE)
    if (len_ < POPULATE_THRESHOLD)
        return;

    static const size_t page_size = ::getPageSize();
    if (len_ < page_size) /// Rounded address should be still within [buf, buf + len).
        return;

    auto [buf, len] = adjustToPageSize(buf_, len_, page_size);
    if (::madvise(buf, len, MADV_POPULATE_WRITE) < 0)
        LOG_TRACE(
            LogFrequencyLimiter(getLogger("Allocator"), 1),
            "Attempt to populate pages failed: {} (EINVAL is expected for kernels < 5.14)",
            errnoToString(errno));
#endif
}

template <bool clear_memory, bool populate>
void * allocNoTrack(size_t size, size_t alignment)
{
    void * buf;
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::shouldSample()))
    {
        if (void * ptr = GWPAsan::GuardedAlloc.allocate(size, alignment))
        {
            if constexpr (clear_memory)
                memset(ptr, 0, size);

            if constexpr (populate)
                prefaultPages(ptr, size);

            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);

            return ptr;
        }

        ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
    }
#endif
    if (alignment <= MALLOC_MIN_ALIGNMENT)
    {
        if constexpr (clear_memory)
            buf = ::calloc(size, 1);
        else
            buf = ::malloc(size);

        if (nullptr == buf)
            throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot malloc {}.", ReadableSize(size));
    }
    else
    {
        buf = nullptr;
        int res = posix_memalign(&buf, alignment, size);

        if (0 != res)
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
#if USE_GWP_ASAN
    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(buf)))
    {
        ProfileEvents::increment(ProfileEvents::GWPAsanFree);
        GWPAsan::GuardedAlloc.deallocate(buf);
        return;
    }
#endif

    ::free(buf);
}

void checkSize(size_t size)
{
    /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
    if (size >= 0x8000000000000000ULL)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

}


/// Constant is chosen almost arbitrarily, what I observed is 128KB is too small, 1MB is almost indistinguishable from 64MB and 1GB is too large.
extern const size_t POPULATE_THRESHOLD = 16 * 1024 * 1024;

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

#if USE_GWP_ASAN
    if (unlikely(GWPAsan::shouldSample()))
    {
        auto trace_alloc = CurrentMemoryTracker::alloc(new_size);
        if (void * ptr = GWPAsan::GuardedAlloc.allocate(new_size, alignment))
        {
            memcpy(ptr, buf, std::min(old_size, new_size));
            free(buf, old_size);
            trace_alloc.onAlloc(buf, new_size);

            if constexpr (clear_memory)
                if (new_size > old_size)
                    memset(reinterpret_cast<char *>(ptr) + old_size, 0, new_size - old_size);

            if constexpr (populate)
                prefaultPages(ptr, new_size);

            ProfileEvents::increment(ProfileEvents::GWPAsanAllocateSuccess);
            return ptr;
        }

        [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(new_size);
        ProfileEvents::increment(ProfileEvents::GWPAsanAllocateFailed);
    }

    if (unlikely(GWPAsan::GuardedAlloc.pointerIsMine(buf)))
    {
        /// Big allocs that requires a copy. MemoryTracker is called inside 'alloc', 'free' methods.
        void * new_buf = alloc(new_size, alignment);
        memcpy(new_buf, buf, std::min(old_size, new_size));
        free(buf, old_size);
        buf = new_buf;

        if constexpr (populate)
            prefaultPages(buf, new_size);

        return buf;
    }
#endif

    if (alignment <= MALLOC_MIN_ALIGNMENT)
    {
        /// Resize malloc'd memory region with no special alignment requirement.
        /// Realloc can do 2 possible things:
        /// - expand existing memory region
        /// - allocate new memory block and free the old one
        /// Because we don't know which option will be picked we need to make sure there is enough
        /// memory for all options
        auto trace_alloc = CurrentMemoryTracker::alloc(new_size);

        void * new_buf = ::realloc(buf, new_size);
        if (nullptr == new_buf)
        {
            [[maybe_unused]] auto trace_free = CurrentMemoryTracker::free(new_size);
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY,
                "Allocator: Cannot realloc from {} to {}",
                ReadableSize(old_size),
                ReadableSize(new_size));
        }

        buf = new_buf;
        auto trace_free = CurrentMemoryTracker::free(old_size);
        trace_free.onFree(buf, old_size);
        trace_alloc.onAlloc(buf, new_size);

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
