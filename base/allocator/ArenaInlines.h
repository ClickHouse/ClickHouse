#pragma once

/// The front-end dispatch helpers of the arena that call into the thread cache, and the ones built on the emap
/// lookup of a pointer. jemalloc: `arena_inlines_b.h` (`arena_malloc`, `arena_aalloc`, `arena_salloc`,
/// `arena_vsalloc`, `arena_dalloc*`, `arena_sdalloc*`, `arena_prof_*`).

#include <allocator/Arena.h>
#include <allocator/Arenas.h>
#include <allocator/Common.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadState.h>

namespace jemalloc
{

/// jemalloc: arena_prof_info_get
ALLOCATOR_ALWAYS_INLINE void arenaProfilingInfoGet(
    ThreadState & thread_state, const void * ptr, AllocContext * alloc_context, ProfilingInfo * profiling_info, bool reset_recent)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(profiling_info != nullptr);

    Extent * extent = nullptr;
    bool is_slab;

    /// Static check.
    if (alloc_context == nullptr)
    {
        extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
        is_slab = extent->slab();
    }
    else if (ALLOCATOR_UNLIKELY(!(is_slab = alloc_context->slab)))
    {
        extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
    }

    if (ALLOCATOR_UNLIKELY(!is_slab))
    {
        /// edata must have been initialized at this point.
        ALLOCATOR_ASSERT(extent != nullptr);
        size_t usable_size = (alloc_context == nullptr) ? extent->usableSize() : alloc_context->usableSizeGet();
        if (reset_recent && largeDeallocateSafetyChecks(extent, ptr, usable_size))
        {
            profiling_info->alloc_thread_context = PROFILING_THREAD_CONTEXT_SENTINEL;
            return;
        }
        largeProfilingInfoGet(thread_state, extent, profiling_info, reset_recent);
    }
    else
    {
        /// No need to set other fields in prof_info; they will never be accessed if alloc_tctx == PROFILING_THREAD_CONTEXT_SENTINEL.
        profiling_info->alloc_thread_context = PROFILING_THREAD_CONTEXT_SENTINEL;
    }
}

/// jemalloc: arena_prof_tctx_reset
ALLOCATOR_ALWAYS_INLINE void arenaProfilingThreadContextReset(ThreadState & thread_state, const void * ptr, AllocContext * alloc_context)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(ptr != nullptr);

    /// Static check.
    if (alloc_context == nullptr)
    {
        Extent * extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
        if (ALLOCATOR_UNLIKELY(!extent->slab()))
            largeProfilingThreadContextReset(extent);
    }
    else
    {
        if (ALLOCATOR_UNLIKELY(!alloc_context->slab))
        {
            Extent * extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
            largeProfilingThreadContextReset(extent);
        }
    }
}

/// jemalloc: arena_prof_tctx_reset_sampled
ALLOCATOR_ALWAYS_INLINE void arenaProfilingThreadContextResetSampled(ThreadState & thread_state, const void * ptr)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(ptr != nullptr);

    Extent * extent = arena_extent_map_global.extentLookup(&thread_state, ptr);
    ALLOCATOR_ASSERT(!extent->slab());

    largeProfilingThreadContextReset(extent);
}

/// jemalloc: arena_prof_info_set
ALLOCATOR_ALWAYS_INLINE void
arenaProfilingInfoSet(ThreadState & /*tsd*/, Extent * extent, ProfilingThreadContext * thread_context, size_t size)
{
    static_assert(config::profiling);
    ALLOCATOR_ASSERT(!extent->slab());
    largeProfilingInfoSet(extent, thread_context, size);
}

/// jemalloc: arena_malloc
ALLOCATOR_ALWAYS_INLINE void * arenaMalloc(
    ThreadState * thread_state,
    Arena * arena,
    size_t size,
    SizeClassIdx idx,
    bool zero,
    bool slab,
    ThreadCache * thread_cache,
    bool slow_path)
{
    ALLOCATOR_ASSERT(thread_state != nullptr || thread_cache == nullptr);

    if (ALLOCATOR_LIKELY(thread_cache != nullptr))
    {
        if (ALLOCATOR_LIKELY(slab))
        {
            ALLOCATOR_ASSERT(size_classes::canUseSlab(size));
            return threadCacheAllocSmall(*thread_state, arena, thread_cache, size, idx, zero, slow_path);
        }
        else if (ALLOCATOR_LIKELY(
                     idx < threadCacheNumBinsGet(thread_cache->thread_cache_slow)
                     && !threadCacheBinDisabled(idx, &thread_cache->bins[idx], thread_cache->thread_cache_slow)))
        {
            return threadCacheAllocLarge(*thread_state, arena, thread_cache, size, idx, zero, slow_path);
        }
        /// (size > tcache_max) case falls through.
    }

    return arenaMallocHard(thread_state, arena, size, idx, zero, slab);
}

/// jemalloc: arena_aalloc
ALLOCATOR_ALWAYS_INLINE Arena * arenaOfPointer(ThreadState * thread_state, const void * ptr)
{
    Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
    unsigned arena_idx = extent->arenaIdx();
    return arenas[arena_idx].load(std::memory_order_relaxed);
}

/// jemalloc: arena_salloc
ALLOCATOR_ALWAYS_INLINE size_t arenaAllocationSize(ThreadState * thread_state, const void * ptr)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    AllocContext alloc_context;
    arena_extent_map_global.allocContextLookup(thread_state, ptr, &alloc_context);
    ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);

    return alloc_context.usableSizeGet();
}

/// Return 0 if ptr is not within an extent managed by jemalloc. This function has two extra costs relative to
/// `allocationSize`:
/// - The rtree calls cannot claim to be dependent lookups, which induces rtree lookup load dependencies.
/// - The lookup may fail, so there is an extra branch to check for failure.
/// jemalloc: arena_vsalloc
ALLOCATOR_ALWAYS_INLINE size_t arenaAllocationSizeIfOwned(ThreadState * thread_state, const void * ptr)
{
    FullAllocContext full_alloc_context;
    bool missing = arena_extent_map_global.fullAllocContextTryLookup(thread_state, ptr, &full_alloc_context);
    if (missing)
        return 0;

    if (full_alloc_context.extent == nullptr)
        return 0;
    ALLOCATOR_ASSERT(full_alloc_context.extent->state() == extent_state_active);
    /// Only slab members should be looked up via interior pointers.
    ALLOCATOR_ASSERT(full_alloc_context.extent->addr() == ptr || full_alloc_context.extent->slab());

    ALLOCATOR_ASSERT(full_alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);

    return full_alloc_context.extent->usableSize();
}

/// `size_class_idx` is still needed in this function mainly because `size_class_idx < SIZE_CLASS_NUM_BINS` determines not only if this is a
/// small
/// alloc, but also if `size_class_idx` is valid (an inactive extent would have `size_class_idx == SIZE_CLASS_NUM_SIZES`).
/// jemalloc: arena_dalloc_large_no_tcache
inline void arenaDeallocateLargeNoThreadCache(ThreadState * thread_state, void * ptr, SizeClassIdx size_class_idx, size_t usable_size)
{
    if (config::profiling && ALLOCATOR_UNLIKELY(size_class_idx < SIZE_CLASS_NUM_BINS))
    {
        arenaDeallocatePromoted(thread_state, ptr, nullptr, true);
    }
    else
    {
        Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
        if (largeDeallocateSafetyChecks(extent, ptr, usable_size))
        {
            /// See the comment in isfree.
            return;
        }
        largeDeallocate(thread_state, extent);
    }
}

/// jemalloc: arena_dalloc_no_tcache
inline void arenaDeallocateNoThreadCache(ThreadState * thread_state, void * ptr)
{
    ALLOCATOR_ASSERT(ptr != nullptr);

    AllocContext alloc_context;
    arena_extent_map_global.allocContextLookup(thread_state, ptr, &alloc_context);

    if constexpr (config::debug)
    {
        Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
        ALLOCATOR_ASSERT(alloc_context.size_class_idx == extent->sizeClassIdx());
        ALLOCATOR_ASSERT(alloc_context.size_class_idx < SIZE_CLASS_NUM_SIZES);
        ALLOCATOR_ASSERT(alloc_context.slab == extent->slab());
        ALLOCATOR_ASSERT(alloc_context.usableSizeGet() == extent->usableSize());
    }

    if (ALLOCATOR_LIKELY(alloc_context.slab))
    {
        /// Small allocation.
        arenaDeallocateSmall(thread_state, ptr);
    }
    else
    {
        arenaDeallocateLargeNoThreadCache(thread_state, ptr, alloc_context.size_class_idx, alloc_context.usableSizeGet());
    }
}

/// jemalloc: arena_dalloc_large
ALLOCATOR_ALWAYS_INLINE void arenaDeallocateLarge(
    ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, SizeClassIdx size_class_idx, size_t usable_size, bool slow_path)
{
    ALLOCATOR_ASSERT(thread_state != nullptr && thread_cache != nullptr);
    bool is_sample_promoted = config::profiling && size_class_idx < SIZE_CLASS_NUM_BINS;
    if (ALLOCATOR_UNLIKELY(is_sample_promoted))
    {
        arenaDeallocatePromoted(thread_state, ptr, thread_cache, slow_path);
    }
    else
    {
        if (size_class_idx < threadCacheNumBinsGet(thread_cache->thread_cache_slow)
            && !threadCacheBinDisabled(size_class_idx, &thread_cache->bins[size_class_idx], thread_cache->thread_cache_slow))
        {
            threadCacheDeallocateLarge(*thread_state, thread_cache, ptr, size_class_idx, slow_path);
        }
        else
        {
            Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
            if (largeDeallocateSafetyChecks(extent, ptr, usable_size))
            {
                /// See the comment in isfree.
                return;
            }
            largeDeallocate(thread_state, extent);
        }
    }
}

/// Only in debug builds: detects double frees of small regions. Returns true if the deallocation must be skipped.
/// jemalloc: arena_tcache_dalloc_small_safety_check
ALLOCATOR_ALWAYS_INLINE bool arenaThreadCacheDeallocateSmallSafetyCheck(ThreadState * thread_state, void * ptr)
{
    if constexpr (!config::debug)
    {
        return false;
    }
    else
    {
        Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
        SizeClassIdx bin_idx = extent->sizeClassIdx();
        DivisionInfo division_info = arena_bin_idx_division_info[bin_idx];
        /// Calls the internal function `slabRegionIdxImpl` because the safety check does not require a lock.
        size_t region_idx = Bin::slabRegionIdxImpl(division_info, bin_idx, extent, ptr);
        SlabData * slab_data = extent->slabData();
        const BinInfo & bin_info = bin_infos[bin_idx];
        ALLOCATOR_ASSERT(extent->numFree() < bin_info.num_regions);
        if (ALLOCATOR_UNLIKELY(!bitmapGet(slab_data->bitmap, bin_info.bitmap_info, region_idx)))
        {
            safetyCheckFail(
                "Invalid deallocation detected: the pointer being freed (%p) not currently active, possibly caused by "
                "double free bugs.\n",
                ptr);
            return true;
        }
        return false;
    }
}

/// jemalloc: arena_dalloc
ALLOCATOR_ALWAYS_INLINE void
arenaDeallocate(ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, AllocContext * caller_alloc_context, bool slow_path)
{
    ALLOCATOR_ASSERT(thread_state != nullptr || thread_cache == nullptr);
    ALLOCATOR_ASSERT(ptr != nullptr);

    if (ALLOCATOR_UNLIKELY(thread_cache == nullptr))
    {
        arenaDeallocateNoThreadCache(thread_state, ptr);
        return;
    }

    AllocContext alloc_context;
    if (caller_alloc_context != nullptr)
    {
        alloc_context = *caller_alloc_context;
    }
    else
    {
        __builtin_assume(thread_state != nullptr);
        arena_extent_map_global.allocContextLookup(thread_state, ptr, &alloc_context);
    }

    if constexpr (config::debug)
    {
        Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
        ALLOCATOR_ASSERT(alloc_context.size_class_idx == extent->sizeClassIdx());
        ALLOCATOR_ASSERT(alloc_context.size_class_idx < SIZE_CLASS_NUM_SIZES);
        ALLOCATOR_ASSERT(alloc_context.slab == extent->slab());
        ALLOCATOR_ASSERT(alloc_context.usableSizeGet() == extent->usableSize());
    }

    if (ALLOCATOR_LIKELY(alloc_context.slab))
    {
        /// Small allocation.
        if (arenaThreadCacheDeallocateSmallSafetyCheck(thread_state, ptr))
            return;
        threadCacheDeallocateSmall(*thread_state, thread_cache, ptr, alloc_context.size_class_idx, slow_path);
    }
    else
    {
        arenaDeallocateLarge(thread_state, ptr, thread_cache, alloc_context.size_class_idx, alloc_context.usableSizeGet(), slow_path);
    }
}

/// jemalloc: arena_sdalloc_no_tcache
inline void arenaSizedDeallocateNoThreadCache(ThreadState * thread_state, void * ptr, size_t size)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size <= SIZE_CLASS_LARGE_MAX_CLASS);

    AllocContext alloc_context;
    if (!config::profiling || !options.profiling)
    {
        /// There is no risk of being confused by a promoted sampled object, so base szind and slab on the given size.
        SizeClassIdx size_class_idx = size_classes::sizeToIndex(size);
        alloc_context.init(size_class_idx, (size_class_idx < SIZE_CLASS_NUM_BINS), size);
    }

    if ((config::profiling && options.profiling) || config::debug)
    {
        arena_extent_map_global.allocContextLookup(thread_state, ptr, &alloc_context);

        ALLOCATOR_ASSERT(alloc_context.size_class_idx == size_classes::sizeToIndex(size));
        ALLOCATOR_ASSERT(
            (config::profiling && options.profiling) || alloc_context.slab == (alloc_context.size_class_idx < SIZE_CLASS_NUM_BINS));

        if constexpr (config::debug)
        {
            Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
            ALLOCATOR_ASSERT(alloc_context.size_class_idx == extent->sizeClassIdx());
            ALLOCATOR_ASSERT(alloc_context.slab == extent->slab());
        }
    }

    if (ALLOCATOR_LIKELY(alloc_context.slab))
    {
        /// Small allocation.
        arenaDeallocateSmall(thread_state, ptr);
    }
    else
    {
        arenaDeallocateLargeNoThreadCache(thread_state, ptr, alloc_context.size_class_idx, alloc_context.usableSizeGet());
    }
}

/// jemalloc: arena_sdalloc
ALLOCATOR_ALWAYS_INLINE void arenaSizedDeallocate(
    ThreadState * thread_state, void * ptr, size_t size, ThreadCache * thread_cache, AllocContext * caller_alloc_context, bool slow_path)
{
    ALLOCATOR_ASSERT(thread_state != nullptr || thread_cache == nullptr);
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size <= SIZE_CLASS_LARGE_MAX_CLASS);

    if (ALLOCATOR_UNLIKELY(thread_cache == nullptr))
    {
        arenaSizedDeallocateNoThreadCache(thread_state, ptr, size);
        return;
    }

    AllocContext alloc_context;
    if (config::profiling && options.profiling)
    {
        if (caller_alloc_context == nullptr)
        {
            /// Uncommon case and should be a static check.
            arena_extent_map_global.allocContextLookup(thread_state, ptr, &alloc_context);
            ALLOCATOR_ASSERT(alloc_context.size_class_idx == size_classes::sizeToIndex(size));
            ALLOCATOR_ASSERT(alloc_context.usableSizeGet() == size);
        }
        else
        {
            alloc_context = *caller_alloc_context;
        }
    }
    else
    {
        /// There is no risk of being confused by a promoted sampled object, so base szind and slab on the given size.
        alloc_context.size_class_idx = size_classes::sizeToIndex(size);
        alloc_context.slab = (alloc_context.size_class_idx < SIZE_CLASS_NUM_BINS);
    }

    if constexpr (config::debug)
    {
        Extent * extent = arena_extent_map_global.extentLookup(thread_state, ptr);
        ALLOCATOR_ASSERT(alloc_context.size_class_idx == extent->sizeClassIdx());
        ALLOCATOR_ASSERT(alloc_context.slab == extent->slab());
        alloc_context.init(alloc_context.size_class_idx, alloc_context.slab, size_classes::sizeToUsableSize(size));
        ALLOCATOR_ASSERT(alloc_context.usableSizeGet() == extent->usableSize());
    }

    if (ALLOCATOR_LIKELY(alloc_context.slab))
    {
        /// Small allocation.
        if (arenaThreadCacheDeallocateSmallSafetyCheck(thread_state, ptr))
            return;
        threadCacheDeallocateSmall(*thread_state, thread_cache, ptr, alloc_context.size_class_idx, slow_path);
    }
    else
    {
        arenaDeallocateLarge(
            thread_state, ptr, thread_cache, alloc_context.size_class_idx, size_classes::sizeToUsableSize(size), slow_path);
    }
}

}
