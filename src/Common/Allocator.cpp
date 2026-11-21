#include <Common/AllocationInterceptors.h>
#include <Common/Allocator.h>
#include <Common/BitHelpers.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/VersionNumber.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <base/errnoToString.h>
#include <base/getPageSize.h>

#include <atomic>

#include <Poco/Environment.h>
#include <Poco/Logger.h>
#include <sys/mman.h> /// MADV_POPULATE_WRITE


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_MUNMAP;
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

/// Large hash tables are accessed at random, and with 4 KiB pages almost every probe of a table that is much larger
/// than the TLB reach also misses the TLB and needs a page walk. On distributions that ship with
/// `transparent_hugepage/enabled = madvise` the tables never get transparent huge pages unless we ask for them.
/// So the buffers of `HashTableAllocator` (the only user of `populate = true`) that are at least one huge page large
/// are mapped directly, aligned to the huge page size, and advised with `MADV_HUGEPAGE` before they are populated.
/// Returns 0 if this is disabled or not supported.
size_t readHugePageSizeForHashTables()
{
#if defined(OS_LINUX) && defined(MADV_HUGEPAGE)
    /// Experiment knob, off by default: `CLICKHOUSE_HASH_TABLE_HUGE_PAGES=1` enables it.
    /// Every qualifying buffer gets a fresh mapping instead of memory reused by jemalloc, and that makes
    /// workloads that create and drop many large tables (dictionaries, `DISTINCT`, aggregation) slower.
    if (const char * env = getenv("CLICKHOUSE_HASH_TABLE_HUGE_PAGES"); !env || env[0] != '1') // NOLINT(concurrency-mt-unsafe)
        return 0;

    auto read_first_line = [](const char * path) -> std::string
    {
        FILE * f = fopen(path, "r");
        if (!f)
            return {};
        char line[128] = {};
        std::string result;
        if (fgets(line, sizeof(line), f))
            result = line;
        (void)fclose(f);
        return result;
    };

    const std::string enabled = read_first_line("/sys/kernel/mm/transparent_hugepage/enabled");
    if (enabled.empty() || enabled.contains("[never]"))
        return 0;

    const std::string huge_page_size_str = read_first_line("/sys/kernel/mm/transparent_hugepage/hpage_pmd_size");
    if (huge_page_size_str.empty())
        return 0;
    char * end = nullptr;
    errno = 0;
    const uint64_t value = std::strtoull(huge_page_size_str.c_str(), &end, 10);
    if (errno != 0 || end == huge_page_size_str.c_str())
        return 0;
    const size_t huge_page_size = value;

    /// With 64 KiB base pages a PMD huge page is 512 MiB - too coarse to be useful for hash tables.
    if (huge_page_size == 0 || huge_page_size > 32 * 1024 * 1024 || !isPowerOf2(huge_page_size) || huge_page_size <= static_cast<size_t>(::getPageSize()))
        return 0;
    return huge_page_size;
#else
    return 0;
#endif
}

/// Minimum size for which we even consider huge pages, to avoid touching the function-local static on small allocations.
constexpr size_t MIN_HUGE_PAGE_SIZE = 2 * 1024 * 1024;

size_t hugePageSizeForHashTables()
{
    static const size_t value = readHugePageSizeForHashTables();
    return value;
}

template <bool populate>
bool useHugePages(size_t size)
{
    if constexpr (!populate)
        return false;
    if (size < MIN_HUGE_PAGE_SIZE)
        return false;
    const size_t huge_page_size = hugePageSizeForHashTables();
    return huge_page_size && size >= huge_page_size;
}

size_t roundUpToPageSize(size_t size)
{
    const size_t page_size = static_cast<size_t>(::getPageSize());
    return (size + page_size - 1) / page_size * page_size;
}

std::atomic<size_t> directly_mapped_bytes = 0;

/// The memory is zero-filled by the kernel.
void * mmapWithHugePages(size_t size, size_t alignment)
{
#if defined(OS_LINUX) && defined(MADV_HUGEPAGE)
    const size_t huge_page_size = std::max(hugePageSizeForHashTables(), alignment);
    const size_t length = roundUpToPageSize(size);
    const size_t map_length = length + huge_page_size;

    void * map = ::mmap(nullptr, map_length, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (MAP_FAILED == map)
        return nullptr;

    /// Trim the mapping to a huge-page-aligned region.
    const uintptr_t begin = reinterpret_cast<uintptr_t>(map);
    const uintptr_t aligned = (begin + huge_page_size - 1) & ~(huge_page_size - 1);
    const uintptr_t end = begin + map_length;
    if (aligned != begin)
        ::munmap(map, aligned - begin);
    if (aligned + length != end)
        ::munmap(reinterpret_cast<void *>(aligned + length), end - (aligned + length));

    /// Only an advice: if it fails, the memory is still usable with regular pages.
    ::madvise(reinterpret_cast<void *>(aligned), length, MADV_HUGEPAGE);
    directly_mapped_bytes.fetch_add(length, std::memory_order_relaxed);
    return reinterpret_cast<void *>(aligned);
#else
    UNUSED(size, alignment);
    return nullptr;
#endif
}

void munmapWithHugePages(void * buf, size_t size)
{
    const size_t length = roundUpToPageSize(size);
    if (0 != ::munmap(buf, length))
        DB::ErrnoException::throwWithErrno(DB::ErrorCodes::CANNOT_MUNMAP, errno, "Allocator: Cannot munmap {}.", ReadableSize(size));
    directly_mapped_bytes.fetch_sub(length, std::memory_order_relaxed);
}

void prefaultPages([[maybe_unused]] void * buf_, [[maybe_unused]] size_t len_)
{
#if defined(MADV_POPULATE_WRITE)
    if (len_ < POPULATE_THRESHOLD)
        return;

    if (!is_supported_by_kernel) [[unlikely]]
        return;

    auto [buf, len] = adjustToPageSize(buf_, len_, ::getPageSize());
    ::madvise(buf, len, MADV_POPULATE_WRITE);
#endif
}

template <bool clear_memory, bool populate>
void * allocImpl(size_t size, size_t alignment)
{
    auto trace = CurrentMemoryTracker::alloc(size);

    void * buf = nullptr;
    if (useHugePages<populate>(size))
    {
        /// Fresh anonymous memory is already zero-filled, so `clear_memory` needs no work here.
        buf = mmapWithHugePages(size, alignment);
        if (nullptr == buf) [[unlikely]]
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
            DB::ErrnoException::throwWithErrno(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, errno, "Allocator: Cannot mmap {}.", ReadableSize(static_cast<double>(size)));
        }
    }
    else if (alignment <= MALLOC_MIN_ALIGNMENT) [[likely]]
    {
        if constexpr (clear_memory)
            buf = __real_calloc(size, 1);
        else
            buf = __real_malloc(size);

        if (nullptr == buf) [[unlikely]]
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
            throw DB::ErrnoException(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot malloc {}.", ReadableSize(static_cast<double>(size)));
        }
    }
    else
    {
        buf = nullptr;
        int res = __real_posix_memalign(&buf, alignment, size);

        if (0 != res) [[unlikely]]
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(size);
            // The value of `errno` is not set according to the man: https://man7.org/linux/man-pages/man3/posix_memalign.3.html
            DB::ErrnoException::throwWithErrno(
                DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, res, "Cannot allocate memory (posix_memalign) {}.", ReadableSize(size));
        }

        if constexpr (clear_memory)
            memset(buf, 0, size);
    }

    if constexpr (populate)
        prefaultPages(buf, size);

    trace.onAlloc(buf, size);
    return buf;
}

template <bool populate>
void freeImpl(void * buf, size_t size)
{
    if (useHugePages<populate>(size))
        munmapWithHugePages(buf, size);
    else
        __real_free(buf);
}

void checkSize(size_t size)
{
    /// More obvious exception in case of possible overflow (instead of just "Cannot mmap").
    if (size >= MAX_ALLOCATION_SIZE) [[unlikely]]
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Too large size ({}) passed to allocator. It indicates an error.", size);
}

}


/// Constant is chosen almost arbitrarily, what I observed is 128KB is too small, 1MB is almost indistinguishable from 64MB and 1GB is too large.
extern const size_t POPULATE_THRESHOLD = std::max(Int64{16 * 1024 * 1024}, ::getPageSize());

template <bool clear_memory_, bool populate>
void * Allocator<clear_memory_, populate>::alloc(size_t size, size_t alignment)
{
    checkSize(size);
    return allocImpl<clear_memory_, populate>(size, alignment);
}


template <bool clear_memory_, bool populate>
void Allocator<clear_memory_, populate>::free(void * buf, size_t size, size_t /* alignment */)
{
    try
    {
        checkSize(size);
        freeImpl<populate>(buf, size);
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

    if (alignment <= MALLOC_MIN_ALIGNMENT && !useHugePages<populate>(old_size) && !useHugePages<populate>(new_size)) [[likely]]
    {
        /// Resize malloc'd memory region with no special alignment requirement.
        /// Realloc can do 2 possible things:
        /// - expand existing memory region
        /// - allocate new memory block and free the old one
        /// Because we don't know which option will be picked we need to make sure there is enough
        /// memory for all options
        auto trace_alloc = CurrentMemoryTracker::alloc(new_size);

        void * new_buf = __real_realloc(buf, new_size);
        if (nullptr == new_buf) [[unlikely]]
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


size_t getAllocatorDirectlyMappedBytes()
{
    return directly_mapped_bytes.load(std::memory_order_relaxed);
}

template class Allocator<false, false>;
template class Allocator<true, false>;
template class Allocator<false, true>;
template class Allocator<true, true>;
