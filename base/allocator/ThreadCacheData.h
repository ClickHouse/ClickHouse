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
#include <allocator/NsTime.h>
#include <allocator/Options.h>
#include <allocator/SizeClassConstants.h>

#include <cstdint>

namespace jemalloc
{

class Arena;
struct ThreadCache;

/// jemalloc: TCACHES_ELM_NEED_REINIT (used for explicit tcaches only: flushed but not destroyed)
inline ThreadCache * const TCACHES_ELM_NEED_REINIT = reinterpret_cast<ThreadCache *>(uintptr_t(1));

/// jemalloc: TCACHE_GC_NEIGHBOR_LIMIT (2 MiB)
inline constexpr uintptr_t TCACHE_GC_NEIGHBOR_LIMIT = uintptr_t(1) << 21;
/// jemalloc: TCACHE_GC_INTERVAL_NS (10 ms)
inline constexpr uint64_t TCACHE_GC_INTERVAL_NS = uint64_t(10) * 1000000;
/// jemalloc: TCACHE_GC_SMALL_NBINS_MAX
inline constexpr unsigned TCACHE_GC_SMALL_NBINS_MAX = (SC_NBINS > 8) ? (SC_NBINS >> 3) : 1;
/// jemalloc: TCACHE_GC_LARGE_NBINS_MAX
inline constexpr unsigned TCACHE_GC_LARGE_NBINS_MAX = 1;

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
    unsigned tcache_nbins = 0;
    /// Last time GC has been performed.
    NsTime last_gc_time = NsTime::zero();
    /// Next bin to GC.
    szind_t next_gc_bin = 0;
    szind_t next_gc_bin_small = 0;
    szind_t next_gc_bin_large = 0;
    /// For small bins, help determine how many items to fill at a time.
    CacheBinFillCtl bin_fill_ctl_do_not_access_directly[SC_NBINS] = {};
    /// For small bins, whether has been refilled since last GC.
    bool bin_refilled[SC_NBINS] = {};
    /// For small bins, the number of items we can pretend to flush before actually flushing.
    uint8_t bin_flush_delay_items[SC_NBINS] = {};
    /// The start of the allocation containing the dynamic allocation for either the cache bins alone, or the cache
    /// bin memory as well as this `ThreadCacheSlow` and its associated `ThreadCache`.
    void * dyn_alloc = nullptr;
    /// The associated bins.
    ThreadCache * tcache = nullptr;
};

/// jemalloc: tcache_t (TCACHE_ZERO_INITIALIZER)
struct ThreadCache
{
    ThreadCacheSlow * tcache_slow = nullptr;
    CacheBin bins[TCACHE_NBINS_MAX];
};

/// Linkage for the list of available (previously used) explicit tcache IDs.
/// jemalloc: tcaches_t
struct ThreadCaches
{
    union
    {
        ThreadCache * tcache;
        ThreadCaches * next;
    };
};

/// The sizes are observable: explicit tcaches are allocated as one internal allocation of
/// `sizeof(ThreadCache) + sizeof(ThreadCacheSlow) + stacks` (`stats.metadata`, size classes).
static_assert(sizeof(CacheBinArrayDescriptor) == 24);
static_assert(offsetof(ThreadCacheSlow, last_gc_time) == 56);
static_assert(offsetof(ThreadCacheSlow, bin_fill_ctl_do_not_access_directly) == 76);
static_assert(sizeof(ThreadCacheSlow) == alignmentCeiling(76 + 4 * SC_NBINS, 8) + 16, "Must have the size of tcache_slow_t");
static_assert(sizeof(ThreadCache) == 8 + 24 * TCACHE_NBINS_MAX, "Must have the size of tcache_t");
static_assert(sizeof(ThreadCaches) == 8);

}
