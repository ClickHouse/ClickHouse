#pragma once

#include <base/defines.h>
#include <Common/StackTrace.h>

#include <cassert>
#include <cstddef>
#include <cstdint>

#include <cstdio>

namespace clickhouse_gwp_asan
{

struct ThreadLocalPackedVariables
{
    constexpr ThreadLocalPackedVariables() : next_sample_counter(0), recursive_guard(false) { }
    uint32_t next_sample_counter = 0;
    bool recursive_guard = false;
};

static_assert(sizeof(ThreadLocalPackedVariables) <= sizeof(uint64_t), "Thread-locals do not fit in one cache line (uint64_t)");

enum class Error : uint8_t
{
    UNKNOWN,
    USE_AFTER_FREE,
    DOUBLE_FREE,
    INVALID_FREE,
    BUFFER_OVERFLOW,
    BUFFER_UNDERFLOW
};

const char * errorToString(const Error & error);

static constexpr uint64_t kInvalidThreadID = UINT64_MAX;

struct GuardedPoolAllocatorState
{
    constexpr GuardedPoolAllocatorState() { }

    /// Returns whether the provided pointer is a current sampled allocation that
    /// is owned by this pool.
    inline ALWAYS_INLINE bool pointerIsMine(const void * ptr) const
    {
        uintptr_t p = reinterpret_cast<uintptr_t>(ptr);
        return p < guarded_page_pool_end && guarded_page_pool <= p;
    }

    /// Returns the address of the slot_index guarded slot.
    uintptr_t slotToAddr(size_t slot_index) const;

    /// Returns the slot responsible for address ptr.
    size_t addrToSlot(uintptr_t ptr) const;

    /// Returns the largest allocation that is supported by this pool.
    size_t maximumAllocationSize() const;

    /// Gets the nearest slot to the provided address.
    size_t getNearestSlot(uintptr_t ptr) const;

    /// Returns whether the provided pointer is a guard page or not. The pointer
    /// must be within memory owned by this pool, else the result is undefined.
    bool isGuardPage(uintptr_t ptr) const;

    /// The number of guarded slots that this pool holds.
    size_t max_simultaneous_allocations = 0;

    /// Pointer to the pool of guarded slots. Note that this points to the start of
    /// the pool (which is a guard page), not a pointer to the first guarded page.
    uintptr_t guarded_page_pool = 0;
    uintptr_t guarded_page_pool_end = 0;

    /// Cached page size for this system in bytes.
    size_t page_size = 0;

    /// Size of a single slot for used allocation. Calculated as page_size
    size_t slot_size = 0;

    /// The type and address of an internally-detected failure. For INVALID_FREE
    /// and DOUBLE_FREE, these errors are detected in GWP-ASan, which will set
    /// these values and terminate the process.
    Error failure_type = Error::UNKNOWN;
    uintptr_t failure_address = 0;
};

// This struct contains all the metadata recorded about a single allocation made
// by GWP-ASan. If `AllocationMetadata.addr` is zero, the metadata is non-valid.
struct AllocationMetadata
{
    // Records the given allocation metadata into this struct.
    void recordAllocation(uintptr_t allocation_addr, size_t allocation_size);
    // Record that this allocation is now deallocated.
    void recordDeallocation();

    // The address of this allocation. If zero, the rest of this struct isn't
    // valid, as the allocation has never occurred.
    uintptr_t addr = 0;
    // Represents the actual size of the allocation.
    size_t requested_size = 0;

    uint64_t thread_id = kInvalidThreadID;

    struct CallSiteInfo
    {
        void recordBacktrace();

        uint64_t thread_id = kInvalidThreadID;
        StackTrace trace;
    };

    CallSiteInfo allocation_trace;
    CallSiteInfo deallocation_trace;

    /// Whether this allocation has been deallocated yet.
    bool is_deallocated = false;
};

inline ThreadLocalPackedVariables * getThreadLocals()
{
    alignas(8) static thread_local ThreadLocalPackedVariables locals;
    return &locals;
}

}
