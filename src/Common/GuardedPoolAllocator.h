#pragma once

#include <Common/GuardedPoolAllocatorCommon.h>
#include <Common/GuardedPoolAllocatorOptions.h>

#include <mutex>

#include <cinttypes>

namespace clickhouse_gwp_asan
{

class GuardedPoolAllocator;

GuardedPoolAllocator * getAllocator();

class GuardedPoolAllocator
{
public:
    constexpr GuardedPoolAllocator() = default;
    GuardedPoolAllocator(const GuardedPoolAllocator &) = delete;
    GuardedPoolAllocator & operator=(const GuardedPoolAllocator &) = delete;

    ~GuardedPoolAllocator() = default;

    void init(const clickhouse_gwp_asan::Options & options);

    /// Stop GuardedPoolAllocator to prevent further allocations.
    /// Intended to be used in signal handler on crash to prevent any possible
    /// metadata corruption when trying to diagnose error.
    void stop();

    void * allocate(size_t size, size_t alignment);
    void deallocate(void * ptr);

    inline ALWAYS_INLINE bool shouldSample()
    {
        /// next_sample_counter == 0 means we "should regenerate the counter".
        ///                   == 1 means we "should sample this allocation".
        /// AdjustedSampleRatePlusOne is designed to intentionally underflow. This
        /// class must be valid when zero-initialised, and we wish to sample as
        /// infrequently as possible when this is the case, hence we underflow to
        /// UINT32_MAX.
        if (unlikely(getThreadLocals()->next_sample_counter == 0))
            getThreadLocals()->next_sample_counter = (getRandom() % (adjusted_sample_rate_plus_one - 1)) + 1;
        return unlikely(--getThreadLocals()->next_sample_counter == 0);
    }

    inline ALWAYS_INLINE bool pointerIsMine(const void * ptr) const { return state.pointerIsMine(ptr); }

    /// Get allocation size by ptr (for memory tracker)
    inline ALWAYS_INLINE size_t allocationSize(const void * ptr) const
    {
        uintptr_t p = reinterpret_cast<uintptr_t>(ptr);
        size_t slot = state.getNearestSlot(p);
        return metadata[slot].requested_size;
    }

    /// Returns a pointer to the Metadata region, or nullptr if it doesn't exist.
    const AllocationMetadata * getMetadataRegion() const { return metadata; }

    /// Returns a pointer to the AllocatorState region.
    const GuardedPoolAllocatorState * getAllocatorState() const { return &state; }

private:
    static constexpr size_t kInvalidSlotID = SIZE_MAX;

    /// Map and unmap memory with mmap
    static void * map(size_t Size);

    static void * reserveGuardedPool(size_t size);

    size_t reserveSlotInGuardedPool();
    /// Mark slot as free.
    void freeSlotInGuardedPool(size_t slot_index);

    /// allocateInGuardedPool() ptr and size must be a subrange of the previously
    /// reserved pool range.
    void allocateInGuardedPool(void * ptr, size_t size) const;
    /// deallocateInGuardedPool() ptr and size must be an exact pair previously
    /// passed to allocateInGuardedPool().
    void deallocateInGuardedPool(void * ptr, size_t size) const;

    static uintptr_t alignUp(uintptr_t ptr, size_t alignment);
    static uintptr_t alignDown(uintptr_t ptr, size_t alignment);

    [[noreturn]] void trapOnAddress(uintptr_t address, Error err);

    static size_t getPlatformPageSize();

    /// Return random uint32_t from [0, UINT32_MAX] with
    /// std::uniform_int_distribution<uint32_t>
    static uint32_t getRandom();

    class ScopedRecursiveGuard
    {
    public:
        ScopedRecursiveGuard() { getThreadLocals()->recursive_guard = true; }
        ~ScopedRecursiveGuard() { getThreadLocals()->recursive_guard = false; }
    };

    /// Array of free slots and its size. When a slot is freed it is added
    /// to this array. When a free slot is requested, a random element of this
    /// array is chosen, swapped with last element and the array is shrunk.
    size_t * free_slots = nullptr;
    size_t free_slots_length = 0;

    size_t adjusted_sample_rate_plus_one = 0;

    size_t currently_allocated_slots = 0;

    std::atomic<bool> stopped{false};
    AllocationMetadata * metadata;
    GuardedPoolAllocatorState state;
    std::mutex pool_reservation_mutex;
    std::mutex trap_mutex;
    static constexpr size_t total_size = 4096U * 1024U;
};
}
