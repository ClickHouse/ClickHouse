#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>
#include <base/errnoToString.h>
#include <base/getPageSize.h>

#include <cerrno>
#include <cstring>

#include <sys/mman.h>

namespace DB
{

namespace ErrorCodes
{
extern const int CANNOT_ALLOCATE_MEMORY;
extern const int LOGICAL_ERROR;
}

namespace
{

/// Whether the running kernel accepts `MADV_POPULATE_WRITE` (Linux 5.14+). Probed once on a private page,
/// because an older kernel reports the unknown advice as EINVAL, which is also what a bad range returns.
bool populateWriteSupported()
{
#if defined(MADV_POPULATE_WRITE)
    static const bool supported = []
    {
        const size_t page = ::getPageSize();
        void * probe = ::mmap(nullptr, page, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (probe == MAP_FAILED)
            return false;
        const bool ok = ::madvise(probe, page, MADV_POPULATE_WRITE) == 0;
        ::munmap(probe, page);
        return ok;
    }();
    return supported;
#else
    return false;
#endif
}

}

RangeCommittedBuffer::RangeCommittedBuffer(size_t bytes_)
    : bytes(bytes_)
{
    if (bytes == 0)
        return;

    void * mapped = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (mapped == MAP_FAILED)
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot reserve {} for the partitioned join hash table", ReadableSize(bytes));
    ptr = static_cast<char *>(mapped);

#if defined(MADV_HUGEPAGE)
    /// Best effort: the table is probed at random, so fewer TLB misses matter and nothing here depends
    /// on the advice being honoured.
    if (bytes >= (2u << 20))
        ::madvise(ptr, bytes, MADV_HUGEPAGE);
#endif
}

RangeCommittedBuffer::~RangeCommittedBuffer()
{
    if (!ptr)
        return;
    /// Not `tryLogCurrentException`: no exception is in flight here, so that would rethrow nothing and
    /// terminate. Report the errno and carry on with the accounting.
    if (0 != ::munmap(ptr, bytes))
        LOG_ERROR(getLogger("RangeCommittedBuffer"), "Cannot munmap {} bytes at {}: {}", bytes, static_cast<const void *>(ptr), errnoToString());
    const size_t accounted = committed.load(std::memory_order_relaxed);
    if (accounted)
    {
        auto trace = CurrentMemoryTracker::free(static_cast<Int64>(accounted));
        trace.onFree(ptr, accounted);
    }
}

void RangeCommittedBuffer::commit(size_t offset, size_t len)
{
    if (len == 0)
        return;
    if (offset + len > bytes || offset + len < offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RangeCommittedBuffer: commit of [{}, {}) outside a buffer of {} bytes", offset, offset + len, bytes);

    /// Charged before the first touch, so a memory limit fails the query here rather than in a page fault.
    auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(len));
    committed.fetch_add(len, std::memory_order_relaxed);
    trace.onAlloc(ptr + offset, len);

    char * begin = ptr + offset;
    if (populateWriteSupported())
    {
#if defined(MADV_POPULATE_WRITE)
        const size_t page = ::getPageSize();
        const auto address = reinterpret_cast<uintptr_t>(begin);
        const uintptr_t aligned_begin = address & ~(page - 1);
        const uintptr_t aligned_end = (address + len + page - 1) & ~(page - 1);
        const uintptr_t buffer_end = reinterpret_cast<uintptr_t>(ptr) + bytes;
        const uintptr_t end = std::min<uintptr_t>(aligned_end, (buffer_end + page - 1) & ~(page - 1));
        if (0 == ::madvise(reinterpret_cast<void *>(aligned_begin), end - aligned_begin, MADV_POPULATE_WRITE)) /// NOLINT(performance-no-int-to-ptr)
            return;
#endif
    }
    /// The pages are zero already; touching exactly this range faults them in on the committing thread
    /// without racing a neighbour's cells.
    memset(begin, 0, len);
}

}
