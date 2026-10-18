#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>

#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <base/getPageSize.h>

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

/// Whether the running kernel accepts `MADV_POPULATE_WRITE` (Linux 5.14+). Probed once on a private page.
/// An older kernel reports the unknown advice as EINVAL, and EINVAL is also what a bad range returns.
/// A real call cannot double as the capability check. `Allocator`'s own prefaulting is not reused because
/// it skips anything under 16 MiB, and a range is L2-sized.
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

    /// Page aligned, so the ranges the owners commit and zero never share a page with anything else.
    /// Allocated through the untracked entry point, as `Allocator` does underneath its own accounting.
    /// The memory tracker learns about this buffer range by range, in `commit`. A tracker blocker would
    /// not do here, because it hides the allocation from the query's tracker while the global total still
    /// counts it.
    void * buf = nullptr;
    if (int res = __real_posix_memalign(&buf, ::getPageSize(), bytes); res != 0)
        ErrnoException::throwWithErrno(
            ErrorCodes::CANNOT_ALLOCATE_MEMORY, res, "Cannot allocate {} for the partitioned join hash table", ReadableSize(bytes));
    ptr = static_cast<char *>(buf);
}

RangeCommittedBuffer::~RangeCommittedBuffer()
{
    reset();
}

RangeCommittedBuffer::RangeCommittedBuffer(RangeCommittedBuffer && other) noexcept
    : ptr(other.ptr)
    , bytes(other.bytes)
    , committed(other.committed.load(std::memory_order_relaxed))
{
    other.ptr = nullptr;
    other.bytes = 0;
    other.committed.store(0, std::memory_order_relaxed);
}

RangeCommittedBuffer & RangeCommittedBuffer::operator=(RangeCommittedBuffer && other) noexcept
{
    if (this != &other)
    {
        reset();
        ptr = other.ptr;
        bytes = other.bytes;
        committed.store(other.committed.load(std::memory_order_relaxed), std::memory_order_relaxed);
        other.ptr = nullptr;
        other.bytes = 0;
        other.committed.store(0, std::memory_order_relaxed);
    }
    return *this;
}

void RangeCommittedBuffer::reset()
{
    if (!ptr)
        return;
    __real_free(ptr);
    const size_t accounted = committed.load(std::memory_order_relaxed);
    if (accounted)
    {
        auto trace = CurrentMemoryTracker::free(static_cast<Int64>(accounted));
        trace.onFree(ptr, accounted);
    }
    ptr = nullptr;
    bytes = 0;
    committed.store(0, std::memory_order_relaxed);
}

void RangeCommittedBuffer::commit(size_t offset, size_t len)
{
    if (len == 0)
        return;
    if (offset + len > bytes || offset + len < offset)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "RangeCommittedBuffer: commit of [{}, {}) outside a buffer of {} bytes", offset, offset + len, bytes);

    /// Charged before the first touch. A memory limit fails the query here rather than in a page fault.
    auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(len));
    committed.fetch_add(len, std::memory_order_relaxed);
    trace.onAlloc(ptr + offset, len);

    /// The kernel faults the fresh pages in bulk first. Taking one page fault per 4 KiB page from user
    /// space, with every owner faulting into the same mapping at once, costs several times more than the
    /// population loop. On a 4 GiB table: 17 s of build CPU against 8 s. Then the range is zeroed, because
    /// reused allocator memory is not zero. The zeroing touches exactly this range. It never races a
    /// neighbour's cells.
    if (populateWriteSupported())
    {
#if defined(MADV_POPULATE_WRITE)
        const size_t page = ::getPageSize();
        const auto address = reinterpret_cast<uintptr_t>(ptr + offset);
        const uintptr_t aligned_begin = address & ~(page - 1);
        const uintptr_t aligned_end = (address + len + page - 1) & ~(page - 1);
        ::madvise(reinterpret_cast<void *>(aligned_begin), aligned_end - aligned_begin, MADV_POPULATE_WRITE); /// NOLINT(performance-no-int-to-ptr)
#endif
    }
    memset(ptr + offset, 0, len);
}

}
