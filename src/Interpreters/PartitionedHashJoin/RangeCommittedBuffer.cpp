#include <Interpreters/PartitionedHashJoin/RangeCommittedBuffer.h>

#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
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

    /// Page aligned, so the ranges the owners commit and zero never share a page with anything else. The
    /// untracked allocator entry point, as `Allocator` uses underneath its own accounting: the memory tracker
    /// learns about this buffer range by range, in `commit`, and a blocked tracker would still charge the
    /// global total here.
    alignment = ::getPageSize();
    void * buf = nullptr;
    if (int res = __real_posix_memalign(&buf, alignment, bytes); res != 0)
    {
        /// `posix_memalign` returns the error instead of setting `errno`.
        errno = res;
        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Cannot allocate {} for the partitioned join hash table", ReadableSize(bytes));
    }
    ptr = static_cast<char *>(buf);
}

RangeCommittedBuffer::~RangeCommittedBuffer()
{
    reset();
}

RangeCommittedBuffer::RangeCommittedBuffer(RangeCommittedBuffer && other) noexcept
    : ptr(other.ptr)
    , bytes(other.bytes)
    , alignment(other.alignment)
    , committed(other.committed.load(std::memory_order_relaxed))
{
    other.ptr = nullptr;
    other.bytes = 0;
    other.alignment = 0;
    other.committed.store(0, std::memory_order_relaxed);
}

RangeCommittedBuffer & RangeCommittedBuffer::operator=(RangeCommittedBuffer && other) noexcept
{
    if (this != &other)
    {
        reset();
        ptr = other.ptr;
        bytes = other.bytes;
        alignment = other.alignment;
        committed.store(other.committed.load(std::memory_order_relaxed), std::memory_order_relaxed);
        other.ptr = nullptr;
        other.bytes = 0;
        other.alignment = 0;
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
    alignment = 0;
    committed.store(0, std::memory_order_relaxed);
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

    /// Fresh pages are faulted in by the kernel in bulk first: one page fault per 4 KiB page taken from
    /// user space, with every owner faulting into the same mapping at once, costs several times more than
    /// the population loop (measured on a 4 GiB table: 17 s of build CPU against 8 s). Then the range is
    /// zeroed: reused allocator memory is not zero, and the zeroing touches exactly this range, so it never
    /// races a neighbour's cells.
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
