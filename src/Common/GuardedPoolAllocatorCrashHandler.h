#pragma once

#include <Common/GuardedPoolAllocatorCommon.h>

#include <Poco/Logger.h>

namespace clickhouse_gwp_asan
{

class GuardedPoolAllocatorCrashHandler
{
public:
    GuardedPoolAllocatorCrashHandler(
        const GuardedPoolAllocatorState * allocator_state_,
        const AllocationMetadata * allocator_metadata_);

    // When a process crashes, there are three possible outcomes:
    //  1. The crash is unrelated to GWP-ASan - in which case this function returns
    //     false.
    //  2. The crash is internally detected within GWP-ASan itself (e.g. a
    //     double-free bug is caught in GuardedPoolAllocator::deallocate(), and
    //     GWP-ASan will terminate the process). In this case - this function
    //     returns true.
    //  3. The crash is caused by a memory error at `AccessPtr` that's caught by the
    //     system, but GWP-ASan is responsible for the allocation. In this case -
    //     the function also returns true.
    // This function takes an optional `AccessPtr` parameter. If the pointer that
    // was attempted to be accessed is available, you should provide it here. In the
    // case of some internally-detected errors, the crash may manifest as an abort
    // or trap may or may not have an associated pointer. In these cases, the
    // pointer can be obtained by a call to getInternalCrashAddress.
    bool errorIsMine(uintptr_t error_ptr = 0u);

    void printHeader(
        Error error, uintptr_t access_ptr, uint32_t access_thread_id, const AllocationMetadata * allocation_meta);

    void dumpReport(uintptr_t error_ptr, uint32_t access_thread_id);

private:
    Poco::Logger * log;
    const GuardedPoolAllocatorState * allocator_state;
    const AllocationMetadata * allocator_metadata;

    const AllocationMetadata * addrToMetadata(uintptr_t ptr);

    // Diagnose and return the type of error that occurred at `error_ptr`. If
    // `error_ptr` is unrelated to GWP-ASan, or if the error type cannot be deduced,
    // this function returns Error::UNKNOWN.
    Error diagnoseError(uintptr_t error_ptr);

    // For internally-detected errors (double free, invalid free), this function
    // returns the pointer that the error occurred at. If the error is unrelated to
    // GWP-ASan, or if the error was caused by a non-internally detected failure,
    // this function returns zero.
    uintptr_t getInternalCrashAddress();

    // Returns a pointer to the metadata for the allocation that's responsible for
    // the crash. Returns nullptr if there is no metadata available for
    // this crash.
    const AllocationMetadata * getMetadata(uintptr_t error_ptr);

    // +---------------------------------------------------------------------------+
    // | Error Information Functions                                               |
    // +---------------------------------------------------------------------------+
    // Functions below return information about the type of error that was caught by
    // GWP-ASan, or information about the allocation that caused the error. These
    // functions generally take an `AllocationMeta` argument, which should be
    // retrieved via. getMetadata.

    // Returns the start of the allocation whose metadata is in `AllocationMeta`.
    static uintptr_t getAllocationAddress(const AllocationMetadata * allocation_meta);

    // Returns the size of the allocation whose metadata is in `AllocationMeta`
    static size_t getAllocationSize(const AllocationMetadata * allocation_meta);

    // Returns the Thread ID that allocated the memory that caused the error at
    // `error_ptr`.
    static uint64_t getAllocationThreadId(const AllocationMetadata * allocation_meta);

    // Retrieve the allocation trace for the allocation whose metadata is in
    // `AllocationMeta`.
    static const StackTrace & getAllocationTrace(const AllocationMetadata * allocation_meta);

    // Returns whether the allocation whose metadata is in `AllocationMeta` has been
    // deallocated.
    static bool isDeallocated(const AllocationMetadata * allocation_meta);

    // Returns the Thread ID that deallocated the memory whose metadata is in
    // `AllocationMeta`. This function may not be called if
    // isDeallocated() returns false.
    static uint64_t getDeallocationThreadId(const AllocationMetadata * allocation_meta);

    // Retrieve the deallocation trace for the allocation whose metadata is in
    // `AllocationMeta`.
    static const StackTrace & getDeallocationTrace(const AllocationMetadata * allocation_meta);
};

}
