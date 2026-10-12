#pragma once

/// The data layout of the thread cache (jemalloc: `tcache_structs.h`, `tcache_types.h`), so that `ThreadState` (which
/// embeds the automatic tcache) is complete. The tcache logic (`tcache.c`) is in ThreadCache.h/.cpp.
///
/// The tcache state is split into the slow and hot path data. Each has a pointer to the other, and the data always
/// comes in pairs. `ThreadCacheSlow` lives in the TSD for the automatic tcache, and as part of a dynamic allocation
/// for explicit tcaches. Keeping a pointer to it lets both cases be treated uniformly.

#include <allocator/CacheBin.h>
#include <allocator/Common.h>
#include <allocator/IntrusiveList.h>
#include <allocator/Nanoseconds.h>
#include <allocator/Options.h>
#include <allocator/SizeClassConstants.h>

#include <cstdint>

namespace jemalloc
{

class Arena;
struct ThreadCache;

/// jemalloc: TCACHES_ELM_NEED_REINIT (used for explicit tcaches only: flushed but not destroyed)
inline ThreadCache * const EXPLICIT_THREAD_CACHES_ELEMENT_NEED_REINIT = reinterpret_cast<ThreadCache *>(uintptr_t(1));

/// jemalloc: TCACHE_GC_NEIGHBOR_LIMIT (2 MiB)
inline constexpr uintptr_t THREAD_CACHE_GC_NEIGHBOR_LIMIT = uintptr_t(1) << 21;
/// jemalloc: TCACHE_GC_INTERVAL_NS (10 ms)
inline constexpr uint64_t THREAD_CACHE_GC_INTERVAL_NS = uint64_t(10) * 1000000;
/// jemalloc: TCACHE_GC_SMALL_NBINS_MAX
inline constexpr unsigned THREAD_CACHE_GC_SMALL_NUM_BINS_MAX = (SIZE_CLASS_NUM_BINS > 8) ? (SIZE_CLASS_NUM_BINS >> 3) : 1;
/// jemalloc: TCACHE_GC_LARGE_NBINS_MAX
inline constexpr unsigned THREAD_CACHE_GC_LARGE_NUM_BINS_MAX = 1;

/// jemalloc: tcache_slow_t (TCACHE_SLOW_ZERO_INITIALIZER)
struct ThreadCacheSlow
{
    /// Lets us track all the tcaches in an arena.
    RingLink<ThreadCacheSlow> link{};
    /// Lets the arena find our cache bins without seeing the tcache definition, to aggregate stats across tcaches.
    CacheBinArrayDescriptor cache_bin_array_descriptor;
    /// The arena this tcache is associated with.
    Arena * arena = nullptr;
    /// The number of bins activated in the tcache.
    unsigned thread_cache_num_bins = 0;
    /// Last time GC has been performed.
    Nanoseconds last_gc_time = Nanoseconds::zero();
    /// Next bin to GC.
    SizeClassIdx next_gc_bin = 0;
    SizeClassIdx next_gc_bin_small = 0;
    SizeClassIdx next_gc_bin_large = 0;
    /// For small bins, help determine how many items to fill at a time.
    CacheBinFillControl bin_fill_control_do_not_access_directly[SIZE_CLASS_NUM_BINS] = {};
    /// For small bins, whether has been refilled since last GC.
    bool bin_refilled[SIZE_CLASS_NUM_BINS] = {};
    /// For small bins, the number of items we can pretend to flush before actually flushing.
    uint8_t bin_flush_delay_items[SIZE_CLASS_NUM_BINS] = {};
    /// The start of the allocation containing the dynamic allocation for either the cache bins alone, or the cache
    /// bin memory as well as this `ThreadCacheSlow` and its associated `ThreadCache`.
    void * dynamic_alloc = nullptr;
    /// The associated bins.
    ThreadCache * thread_cache = nullptr;
};

/// jemalloc: tcache_t (TCACHE_ZERO_INITIALIZER)
struct ThreadCache
{
    ThreadCacheSlow * thread_cache_slow = nullptr;
    CacheBin bins[THREAD_CACHE_NUM_BINS_MAX];
};

/// Linkage for the list of available (previously used) explicit tcache IDs.
/// jemalloc: tcaches_t
struct ThreadCaches
{
    union
    {
        ThreadCache * thread_cache;
        ThreadCaches * next;
    };
};

/// The sizes are observable: explicit tcaches are allocated as one internal allocation of
/// `sizeof(ThreadCache) + sizeof(ThreadCacheSlow) + stacks` (`stats.metadata`, size classes).
static_assert(sizeof(CacheBinArrayDescriptor) == 24);
static_assert(offsetof(ThreadCacheSlow, last_gc_time) == 56);
static_assert(offsetof(ThreadCacheSlow, bin_fill_control_do_not_access_directly) == 76);
static_assert(sizeof(ThreadCacheSlow) == alignmentCeiling(76 + 4 * SIZE_CLASS_NUM_BINS, 8) + 16, "Must have the size of tcache_slow_t");
static_assert(sizeof(ThreadCache) == 8 + 24 * THREAD_CACHE_NUM_BINS_MAX, "Must have the size of tcache_t");
static_assert(sizeof(ThreadCaches) == 8);

}
