#include <base/getThreadId.h>

#include <Common/GuardedPoolAllocatorCommon.h>

namespace clickhouse_gwp_asan
{

const char * errorToString(const Error & error)
{
    switch (error)
    {
        case Error::UNKNOWN:
            return "Unknown";
        case Error::USE_AFTER_FREE:
            return "Use After Free";
        case Error::DOUBLE_FREE:
            return "Double Free";
        case Error::INVALID_FREE:
            return "Invalid (Wild) Free";
        case Error::BUFFER_OVERFLOW:
            return "Buffer Overflow";
        case Error::BUFFER_UNDERFLOW:
            return "Buffer Underflow";
    }
    __builtin_trap();
}

size_t GuardedPoolAllocatorState::maximumAllocationSize() const
{
    return slot_size;
}


uintptr_t GuardedPoolAllocatorState::slotToAddr(size_t slot_index) const
{
    return guarded_page_pool + (page_size * (1 + slot_index)) + (maximumAllocationSize() * slot_index);
}

bool GuardedPoolAllocatorState::isGuardPage(uintptr_t ptr) const
{
    assert(pointerIsMine(reinterpret_cast<void *>(ptr)));
    size_t page_offset_from_pool_start = (ptr - guarded_page_pool) / page_size;
    size_t pages_per_slot = maximumAllocationSize() / page_size;
    return (page_offset_from_pool_start % (pages_per_slot + 1)) == 0;
}

size_t GuardedPoolAllocatorState::addrToSlot(uintptr_t ptr) const
{
    size_t byte_offset_from_pool_start = ptr - guarded_page_pool;
    return byte_offset_from_pool_start / (maximumAllocationSize() + page_size);
}

size_t GuardedPoolAllocatorState::getNearestSlot(uintptr_t ptr) const
{
    if (ptr <= guarded_page_pool + page_size)
        return 0;
    if (ptr > guarded_page_pool_end - page_size)
        return max_simultaneous_allocations - 1;

    if (!isGuardPage(ptr))
        return addrToSlot(ptr);

    if (ptr % page_size <= page_size / 2)
        return addrToSlot(ptr - page_size); /// Round down.
    return addrToSlot(ptr + page_size); /// Round up.
}

void AllocationMetadata::recordAllocation(uintptr_t allocation_addr, size_t allocation_size)
{
    addr = allocation_addr;
    requested_size = allocation_size;
    is_deallocated = false;

    allocation_trace.thread_id = getThreadId();
    deallocation_trace.thread_id = kInvalidThreadID;
}

void AllocationMetadata::recordDeallocation()
{
    is_deallocated = true;
    deallocation_trace.thread_id = getThreadId();
}

void AllocationMetadata::CallSiteInfo::recordBacktrace()
{
    trace = StackTrace();
}

}
