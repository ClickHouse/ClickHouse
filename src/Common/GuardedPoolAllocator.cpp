#include <cassert>
#include <sys/mman.h>

#include <base/defines.h>
#include <base/getPageSize.h>

#include <Common/GuardedPoolAllocator.h>
#include <Common/GuardedPoolAllocatorCommon.h>
#include <Common/thread_local_rng.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event GuardedPoolAllocations;
    extern const Event GuardedPoolDeallocations;
}

namespace clickhouse_gwp_asan
{

/// Required for older Darwin builds, that lack definition of MAP_ANONYMOUS
#ifndef MAP_ANONYMOUS
#    define MAP_ANONYMOUS MAP_ANON
#endif

[[noreturn]] void die(const char * message)
{
    fprintf(stderr, "%s", message);
    abort();
}

/// checks that `condition` is true, otherwise dies with `message`.
inline ALWAYS_INLINE void check(bool condition, const char * message)
{
    if (likely(condition))
        return;

    die(message);
}

namespace
{

GuardedPoolAllocator * allocator_ptr = nullptr;

size_t roundUpTo(size_t size, size_t Boundary) { return (size + Boundary - 1) & ~(Boundary - 1); }

uintptr_t getPageAddr(uintptr_t ptr, uintptr_t page_size) { return ptr & ~(page_size - 1); }

[[maybe_unused]] bool isPowerOfTwo(uintptr_t X) { return (X & (X - 1)) == 0; }

}

GuardedPoolAllocator * getAllocator()
{
    return allocator_ptr;
}

void * GuardedPoolAllocator::map(size_t size)
{
    void * ptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    check(ptr != MAP_FAILED, "Failed to map guarded pool allocator memory");
    return ptr;
}

void * GuardedPoolAllocator::reserveGuardedPool(size_t size)
{
    void * ptr = mmap(nullptr, size, PROT_NONE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0);
    check(ptr != MAP_FAILED, "Failed to reserve guarded pool allocator memory");
    return ptr;
}

void GuardedPoolAllocator::allocateInGuardedPool(void * ptr, size_t size) const
{
    assert((reinterpret_cast<uintptr_t>(ptr) % state.page_size) == 0);
    assert((size % state.page_size) == 0);
    check(mprotect(ptr, size, PROT_READ | PROT_WRITE) == 0, "Failed to allocate in guarded pool allocator memory");
}

void GuardedPoolAllocator::deallocateInGuardedPool(void * ptr, size_t size) const
{
    assert((reinterpret_cast<uintptr_t>(ptr) % state.page_size) == 0);
    assert((size % state.page_size) == 0);
    /// mmap() a PROT_NONE page over the address to release it to the system.
    /// If we used mprotect, this memory will be counted in RSS thus we will still
    /// be occupying physical memory.
    check(
        mmap(ptr, size, PROT_NONE, MAP_FIXED | MAP_ANONYMOUS | MAP_PRIVATE, -1, 0) != MAP_FAILED,
        "Failed to deallocate in guarded pool allocator memory");
}

uintptr_t GuardedPoolAllocator::alignUp(uintptr_t ptr, size_t alignment)
{
    assert(isPowerOfTwo(alignment));
    assert(alignment != 0);
    if ((ptr & (alignment - 1)) == 0)
        return ptr;

    ptr += alignment - (ptr & (alignment - 1));
    return ptr;
}

uintptr_t GuardedPoolAllocator::alignDown(uintptr_t ptr, size_t alignment)
{
    assert(isPowerOfTwo(alignment));
    assert(alignment != 0);
    if ((ptr & (alignment - 1)) == 0)
        return ptr;

    ptr -= ptr & (alignment - 1);
    return ptr;
}

void GuardedPoolAllocator::init(const clickhouse_gwp_asan::Options & options)
{
    if (!options.enabled || options.sample_rate == 0)
        return;

    check(options.sample_rate >= 0, "GWP-ASan Error: sample_rate is < 0.");
    check(options.sample_rate < (1 << 30), "GWP-ASan Error: sample_rate is >= 2^30.");
    check(options.max_simultaneous_allocations >= 0, "GWP-ASan Error: max_simultaneous_allocations is < 0.");

    state.max_simultaneous_allocations = options.max_simultaneous_allocations;

    const size_t page_size = getPlatformPageSize();
    state.page_size = page_size;
    state.slot_size = page_size * options.slot_size;

    /// check that page size is a power of 2
    assert(isPowerOfTwo(page_size));
    size_t pool_size
        = page_size * (1 + state.max_simultaneous_allocations) + state.max_simultaneous_allocations * state.maximumAllocationSize();

    assert(pool_size % page_size == 0);
    void * guarded_pool_memory = reserveGuardedPool(pool_size);
    state.guarded_page_pool = reinterpret_cast<uintptr_t>(guarded_pool_memory);
    state.guarded_page_pool_end = reinterpret_cast<uintptr_t>(guarded_pool_memory) + pool_size;

    size_t free_slots_required_bytes = roundUpTo(state.max_simultaneous_allocations * sizeof(*free_slots), page_size);
    assert((free_slots_required_bytes % state.page_size) == 0);
    free_slots = reinterpret_cast<size_t *>(map(free_slots_required_bytes));

    size_t metadata_required_bytes = roundUpTo(state.max_simultaneous_allocations * sizeof(*metadata), page_size);
    assert((metadata_required_bytes % state.page_size) == 0);
    metadata = reinterpret_cast<AllocationMetadata *>(map(metadata_required_bytes));

    /// Multiply the sample rate by 2 to give a good, fast approximation for (1 /
    /// sample_rate) chance of sampling.
    if (options.sample_rate != 1)
        adjusted_sample_rate_plus_one = static_cast<uint32_t>(options.sample_rate) * 2 + 1;
    else
        adjusted_sample_rate_plus_one = 2;

    getThreadLocals()->next_sample_counter = (getRandom() % (adjusted_sample_rate_plus_one - 1)) + 1;
    allocator_ptr = this;
}

void GuardedPoolAllocator::stop()
{
    getThreadLocals()->recursive_guard = true;

    stopped.store(true);

    /// Lock mutex to lock any in-progress allocations
    pool_reservation_mutex.lock();
}

void * GuardedPoolAllocator::allocate(size_t size, size_t alignment)
{
    if (size > state.maximumAllocationSize())
        return nullptr;

    /// Protect against recursivity.
    if (getThreadLocals()->recursive_guard)
        return nullptr;
    ScopedRecursiveGuard recursive_guard;

    if (stopped.load())
        return nullptr;

    if (alignment == 0)
        alignment = alignof(max_align_t);

    /// Acquires mutex inside
    size_t slot_index = reserveSlotInGuardedPool();

    if (slot_index == kInvalidSlotID)
        return nullptr;

    uintptr_t slot_start = state.slotToAddr(slot_index);
    uintptr_t slot_end = state.slotToAddr(slot_index) + state.maximumAllocationSize();
    AllocationMetadata * meta = &metadata[slot_index];

    uintptr_t user_ptr;
    /// Randomly choose whether to left-align or right-align the allocation with
    /// respect to allocated chunk, and then apply the necessary adjustments
    /// to get an aligned pointer.
    if (getRandom() % 2 == 0)
        user_ptr = alignUp(slot_start, alignment);
    else
        user_ptr = alignDown(slot_end - size, alignment);

    assert(user_ptr >= slot_start);
    assert(user_ptr + size <= slot_end);

    /// If a slot is multiple pages in size, and the allocation takes up a single
    /// page, we can improve overflow detection by leaving the unused pages as
    /// unmapped. Thus we do set PROT_READ | PROT_WRITE on entire slot. Instead
    /// we set those only to pages, which will be used
    const size_t page_size = state.page_size;
    allocateInGuardedPool(reinterpret_cast<void *>(getPageAddr(user_ptr, page_size)), roundUpTo(size, page_size));

    meta->recordAllocation(user_ptr, size);
    meta->allocation_trace.recordBacktrace();

    ProfileEvents::increment(ProfileEvents::GuardedPoolAllocations);
    return reinterpret_cast<void *>(user_ptr);
}

/**
 * To prevent 2 errors being reported concurrently `trap_mutex` is used.
 * This function does not return, so `guard` will never be destroyed.
 * So `trap_mutex` is locked forever.
 */
void GuardedPoolAllocator::trapOnAddress(uintptr_t address, Error err)
{
    std::lock_guard guard{trap_mutex};
    state.failure_type = err;
    state.failure_address = address;

    /// Raise SIGSEGV by touching first guard page.
    volatile char * p = reinterpret_cast<char *>(state.guarded_page_pool);
    *p = 0;

    __builtin_trap();
}

void GuardedPoolAllocator::deallocate(void * ptr)
{
    uintptr_t u_ptr = reinterpret_cast<uintptr_t>(ptr);
    size_t slot = state.getNearestSlot(u_ptr);
    uintptr_t slot_start = state.slotToAddr(slot);
    AllocationMetadata * meta = &metadata[slot];

    if (meta->addr != u_ptr)
        trapOnAddress(u_ptr, Error::INVALID_FREE);

    if (meta->is_deallocated)
        trapOnAddress(u_ptr, Error::DOUBLE_FREE);

    {
        std::lock_guard guard{pool_reservation_mutex};

        meta->recordDeallocation();

        if (!getThreadLocals()->recursive_guard)
        {
            ScopedRecursiveGuard recursive_guard;
            meta->deallocation_trace.recordBacktrace();
        }
    }

    deallocateInGuardedPool(reinterpret_cast<void *>(slot_start), state.maximumAllocationSize());

    /// Acquires mutex inside
    freeSlotInGuardedPool(slot);

    ProfileEvents::increment(ProfileEvents::GuardedPoolDeallocations);
}

size_t GuardedPoolAllocator::reserveSlotInGuardedPool()
{
    /// Acquire mutex to access shared state: free_slots and free_slots_length
    std::lock_guard guard{pool_reservation_mutex};

    /// Do not reuse slots until we've given out all initial slots
    if (currently_allocated_slots < state.max_simultaneous_allocations)
        return currently_allocated_slots++;

    if (free_slots_length == 0)
        return kInvalidSlotID;

    size_t reserved_index = getRandom() % free_slots_length;
    size_t slot_index = free_slots[reserved_index];
    free_slots[reserved_index] = free_slots[--free_slots_length];
    return slot_index;
}

void GuardedPoolAllocator::freeSlotInGuardedPool(size_t slot_index)
{
    /// Acquire mutex to access shared state: free_slots and free_slots_length
    std::lock_guard guard{pool_reservation_mutex};

    assert(free_slots_length < state.max_simultaneous_allocations);
    free_slots[free_slots_length++] = slot_index;
}

size_t GuardedPoolAllocator::getPlatformPageSize()
{
    /// Use function from ClickHouse base
    return static_cast<size_t>(getPageSize());
}

uint32_t GuardedPoolAllocator::getRandom()
{
    std::uniform_int_distribution<uint32_t> distribution(0, UINT32_MAX);
    return distribution(thread_local_rng);
}

}
