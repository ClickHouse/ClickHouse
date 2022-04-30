#include <cassert>
#include <cinttypes>
#include <cstring>

#include <Common/GuardedPoolAllocatorCommon.h>
#include <Common/GuardedPoolAllocatorCrashHandler.h>
#include <Common/StackTrace.h>
#include <Common/logger_useful.h>

#include <base/getThreadId.h>
#include <fmt/format.h>

namespace
{
struct ScopedEndOfReportDecorator
{
    explicit ScopedEndOfReportDecorator(Poco::Logger * log_) : log(log_) { LOG_FATAL(log, "*** GWP-ASan detected a memory error ***"); }
    ~ScopedEndOfReportDecorator() { LOG_FATAL(log, "*** End GWP-ASan report ***\n"); }
    Poco::Logger * log;
};

void printBacktrace(Poco::Logger * log, const StackTrace & trace)
{
    trace.toStringEveryLine([&](const std::string & s) { LOG_FATAL(log, fmt::runtime(s)); });
}

}

namespace clickhouse_gwp_asan
{

GuardedPoolAllocatorCrashHandler::GuardedPoolAllocatorCrashHandler(
    const GuardedPoolAllocatorState * allocator_state_,
    const AllocationMetadata * allocator_metadata_)
    : log(&Poco::Logger::get("GWP-Asan reporter")), allocator_state(allocator_state_), allocator_metadata(allocator_metadata_)
{
    assert(allocator_state && "missing allocator_state in report.");
    assert(allocator_metadata && "missing allocator_metadata in report.");
}

bool GuardedPoolAllocatorCrashHandler::errorIsMine(uintptr_t error_ptr)
{
    if (allocator_state->failure_type != Error::UNKNOWN && allocator_state->failure_address != 0)
        return true;

    return allocator_state->pointerIsMine(reinterpret_cast<void *>(error_ptr));
}

uintptr_t GuardedPoolAllocatorCrashHandler::getInternalCrashAddress()
{
    return allocator_state->failure_address;
}

const AllocationMetadata * GuardedPoolAllocatorCrashHandler::addrToMetadata(uintptr_t ptr)
{
    /// Note - Similar implementation in guarded_pool_allocator.cpp.
    return &allocator_metadata[allocator_state->getNearestSlot(ptr)];
}

Error GuardedPoolAllocatorCrashHandler::diagnoseError(uintptr_t error_ptr)
{
    if (!errorIsMine(error_ptr))
        return Error::UNKNOWN;

    if (allocator_state->failure_type != Error::UNKNOWN)
        return allocator_state->failure_type;

    /// Let's try and figure out what the source of this error is.
    if (allocator_state->isGuardPage(error_ptr))
    {
        size_t slot = allocator_state->getNearestSlot(error_ptr);
        const AllocationMetadata * slot_meta = addrToMetadata(allocator_state->slotToAddr(slot));

        /// Ensure that this slot was allocated once upon a time.
        if (!slot_meta->addr)
            return Error::UNKNOWN;

        if (slot_meta->addr < error_ptr)
            return Error::BUFFER_OVERFLOW;
        return Error::BUFFER_UNDERFLOW;
    }

    /// Access wasn't a guard page, check for use-after-free.
    const AllocationMetadata * slot_meta = addrToMetadata(error_ptr);
    if (slot_meta->is_deallocated)
    {
        return Error::USE_AFTER_FREE;
    }

    /// If we have reached here, the error is still unknown.
    return Error::UNKNOWN;
}

const AllocationMetadata * GuardedPoolAllocatorCrashHandler::getMetadata(uintptr_t error_ptr)
{
    if (!errorIsMine(error_ptr))
        return nullptr;

    if (error_ptr >= allocator_state->guarded_page_pool_end || allocator_state->guarded_page_pool > error_ptr)
        return nullptr;

    const AllocationMetadata * meta = addrToMetadata(error_ptr);
    if (meta->addr == 0)
        return nullptr;

    return meta;
}

uintptr_t GuardedPoolAllocatorCrashHandler::getAllocationAddress(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->addr;
}

size_t GuardedPoolAllocatorCrashHandler::getAllocationSize(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->requested_size;
}

uint64_t GuardedPoolAllocatorCrashHandler::getAllocationThreadId(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->allocation_trace.thread_id;
}

const StackTrace & GuardedPoolAllocatorCrashHandler::getAllocationTrace(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->allocation_trace.trace;
}

bool GuardedPoolAllocatorCrashHandler::isDeallocated(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->is_deallocated;
}

uint64_t GuardedPoolAllocatorCrashHandler::getDeallocationThreadId(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->deallocation_trace.thread_id;
}

const StackTrace & GuardedPoolAllocatorCrashHandler::getDeallocationTrace(const AllocationMetadata * allocation_meta)
{
    return allocation_meta->deallocation_trace.trace;
}

/// Prints the provided error and metadata information.
void GuardedPoolAllocatorCrashHandler::printHeader(
    Error error, uintptr_t access_ptr, uint32_t access_thread_id, const AllocationMetadata * allocation_meta)
{
    std::string description;

    if (error != Error::UNKNOWN && allocation_meta != nullptr)
    {
        uintptr_t address = getAllocationAddress(allocation_meta);
        size_t size = getAllocationSize(allocation_meta);
        if (error == Error::USE_AFTER_FREE)
        {
            description = fmt::format("({} byte{} into a {}-byte allocation at {})", access_ptr - address, (access_ptr - address == 1) ? "" : "s", size, reinterpret_cast<void *>(address));
        }
        else if (access_ptr < address)
        {
            description = fmt::format("({} byte{} to the left of a {}-byte allocation at {})", address - access_ptr, (address - access_ptr == 1) ? "" : "s", size, reinterpret_cast<void *>(address));
        }
        else if (access_ptr > address)
        {
            description = fmt::format("({} byte{} to the right of a {}-byte allocation at {})", access_ptr - address, (access_ptr - address == 1) ? "" : "s", size, reinterpret_cast<void *>(address));
        }
        else
        {
            description = fmt::format("(a {}-byte allocation) ", size);
        }
    }

    /// Possible number of digits of a 64-bit number: ceil(log10(2^64)) == 20. Add
    /// a null terminator, and round to the nearest 8-byte boundary.
    uint64_t thread_id = access_thread_id;

    std::string thread_description;
    if (thread_id == kInvalidThreadID)
    {
        thread_description = "<unknown>";
    }
    else
    {
        thread_description = fmt::format("{}", thread_id);
    }

    LOG_FATAL(
        log,
        "{} at {} {} by thread {} here:\n",
        errorToString(error),
        reinterpret_cast<void *>(access_ptr),
        description,
        thread_description);
}

void GuardedPoolAllocatorCrashHandler::dumpReport(uintptr_t error_ptr, uint32_t access_thread_id)
{
    ScopedEndOfReportDecorator decorator(log);

    uintptr_t internal_error_ptr = getInternalCrashAddress();
    if (internal_error_ptr != 0u)
        error_ptr = internal_error_ptr;

    Error error = diagnoseError(error_ptr);

    if (error == Error::UNKNOWN)
    {
        LOG_FATAL(
            log,
            "GWP-ASan cannot provide any more information about this error. "
            "This may occur due to a wild memory access into the GWP-ASan pool, "
            "or an overflow/underflow that is > 512B in length.");
        return;
    }

    const AllocationMetadata * alloc_meta = getMetadata(error_ptr);

    /// Print the error header.
    printHeader(error, error_ptr, access_thread_id, alloc_meta);

    if (alloc_meta == nullptr)
        return;

    /// Maybe print the deallocation trace.
    if (isDeallocated(alloc_meta))
    {
        uint64_t thread_id = getDeallocationThreadId(alloc_meta);
        if (thread_id == kInvalidThreadID)
            LOG_FATAL(log, "{} was deallocated by thread <unknown> here:", reinterpret_cast<void *>(error_ptr));
        else
            LOG_FATAL(log, "{} was deallocated by thread {} here:\n", reinterpret_cast<void *>(error_ptr), thread_id);

        auto trace = getDeallocationTrace(alloc_meta);
        if (trace.getSize())
            printBacktrace(log, trace);
        else
            LOG_FATAL(log, "Allocation stacktrace size is 0. Is stacktrace collection supported on your system / enabled in this build?");
    }

    /// Print the allocation trace.
    uint64_t thread_id = getAllocationThreadId(alloc_meta);
    if (thread_id == kInvalidThreadID)
        LOG_FATAL(log, "{} was allocated by thread <unknown> here:", reinterpret_cast<void *>(error_ptr));
    else
        LOG_FATAL(log, "{} was allocated by thread {} here:\n", reinterpret_cast<void *>(error_ptr), thread_id);

    auto trace = getAllocationTrace(alloc_meta);
    if (trace.getSize())
        printBacktrace(log, trace);
    else
        LOG_FATAL(log, "Allocation stacktrace size is 0. Is stacktrace collection supported on your system / enabled in this build?");

    if (error == Error::DOUBLE_FREE || error == Error::INVALID_FREE)
        LOG_FATAL(log, "Below will be stack trace of deallocation, which caused this error");
    else
        LOG_FATAL(log, "Below will be stack trace of faulty access");
}

}
