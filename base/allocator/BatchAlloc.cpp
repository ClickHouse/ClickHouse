/// `experimental.batch_alloc` (jemalloc: `batch_alloc` in `src/jemalloc.c`). In the core library (not API.cpp)
/// because the mallctl tree references it.

#include <allocator/InternalMalloc.h>

namespace jemalloc
{

namespace
{

/// jemalloc: prof_sampled
[[maybe_unused]] bool profilingSampled(ThreadState & thread_state, const void * ptr)
{
    ProfilingInfo profiling_info;
    profilingInfoGet(thread_state, ptr, nullptr, &profiling_info);
    return profilingThreadContextIsValid(profiling_info.alloc_thread_context);
}

/// jemalloc: batch_alloc_prof_sample_assert
void batchAllocProfilingSampleAssert(
    [[maybe_unused]] ThreadState & thread_state, [[maybe_unused]] size_t batch, [[maybe_unused]] size_t usable_size)
{
    ALLOCATOR_ASSERT(config::profiling && options.profiling);
    if constexpr (config::debug)
    {
        bool profiling_sample_event = threadEventProfilingSampleEventLookahead(thread_state, batch * usable_size);
        ALLOCATOR_ASSERT(!profiling_sample_event);
        size_t surplus;
        profiling_sample_event = threadEventProfilingSampleEventLookaheadSurplus(thread_state, (batch + 1) * usable_size, &surplus);
        ALLOCATOR_ASSERT(profiling_sample_event);
        ALLOCATOR_ASSERT(surplus < usable_size);
    }
}

}

/// jemalloc: batch_alloc
size_t batchAlloc(void ** ptrs, size_t num, size_t size, int flags)
{
    ThreadState & thread_state = ThreadState::fetch();

    size_t filled = 0;

    if (ALLOCATOR_UNLIKELY(thread_state.reentrancyLevel() > 0))
        return filled;

    size_t alignment = alignmentFromFlags(flags);
    size_t usable_size;
    if (alignedUsableSizeGet(size, alignment, &usable_size, nullptr, false))
        return filled;
    SizeClassIdx idx = size_classes::sizeToIndex(usable_size);
    bool zero = zeroGet(zeroFromFlags(flags), /* slow */ true);

    /// The cache bin and arena will be lazily initialized; it's hard to know in advance whether each of them needs
    /// to be initialized.
    CacheBin * bin = nullptr;
    Arena * arena = nullptr;

    size_t num_regions = 0;
    if (ALLOCATOR_LIKELY(idx < SIZE_CLASS_NUM_BINS))
    {
        num_regions = bin_infos[idx].num_regions;
        ALLOCATOR_ASSERT(num_regions > 0);
    }

    while (filled < num)
    {
        size_t batch = num - filled;
        size_t surplus = SIZE_MAX; /// Dead store.
        bool profiling_sample_event = config::profiling && options.profiling && profilingActiveGetUnlocked()
            && threadEventProfilingSampleEventLookaheadSurplus(thread_state, batch * usable_size, &surplus);

        if (profiling_sample_event)
        {
            /// Adjust so that the batch does not trigger prof sampling.
            batch -= surplus / usable_size + 1;
            batchAllocProfilingSampleAssert(thread_state, batch, usable_size);
        }

        size_t progress = 0;

        if (ALLOCATOR_LIKELY(idx < SIZE_CLASS_NUM_BINS) && batch >= num_regions)
        {
            if (arena == nullptr)
            {
                unsigned arena_idx = arenaIdxFromFlags(flags);
                if (arenaGetFromIdx(thread_state, arena_idx, &arena))
                    return filled;
                if (arena == nullptr)
                    arena = arenaChoose(thread_state, nullptr);
                if (ALLOCATOR_UNLIKELY(arena == nullptr))
                    return filled;
            }
            size_t arena_batch = batch - batch % num_regions;
            size_t n = arenaFillSmallFresh(&thread_state, arena, idx, ptrs + filled, arena_batch, zero);
            progress += n;
            filled += n;
        }

        unsigned thread_cache_idx = threadCacheIdxFromFlags(flags);
        ThreadCache * thread_cache = threadCacheGetFromIdx(thread_state, thread_cache_idx, /* slow */ true, /* is_alloc */ true);
        if (ALLOCATOR_LIKELY(
                thread_cache != nullptr && idx < threadCacheNumBinsGet(thread_cache->thread_cache_slow)
                && !threadCacheBinDisabled(idx, &thread_cache->bins[idx], thread_cache->thread_cache_slow))
            && progress < batch)
        {
            if (bin == nullptr)
                bin = &thread_cache->bins[idx];
            /// If we don't have a tcache bin, we don't want to immediately give up, because there's the possibility
            /// that the user explicitly requested to bypass the tcache, or that the user explicitly turned off the
            /// tcache; in such cases, we go through the slow path, i.e. the `mallocx` call at the end of the while
            /// loop.
            if (bin != nullptr)
            {
                size_t bin_batch = batch - progress;
                /// `n` can be less than `bin_batch`, meaning that the cache bin does not have enough memory. In such
                /// cases, we rely on the slow path, i.e. the `mallocx` call at the end of the while loop, to fill in
                /// the cache, and in the next iteration of the while loop, the tcache will contain a lot of memory,
                /// and we can harvest them here. Compared to the alternative approach where we directly go to the
                /// arena bins here, the overhead of our current approach should usually be minimal, since we never
                /// try to fetch more memory than what a slab contains via the tcache. An additional benefit is that
                /// the tcache will not be empty for the next allocation request.
                size_t n = bin->allocBatch(bin_batch, ptrs + filled);
                if constexpr (config::stats)
                    bin->thread_cache_stats.num_requests += n;
                if (zero)
                {
                    for (size_t i = 0; i < n; ++i)
                        memset(ptrs[filled + i], 0, usable_size);
                }
                if (config::profiling && options.profiling && ALLOCATOR_UNLIKELY(idx >= SIZE_CLASS_NUM_BINS))
                {
                    for (size_t i = 0; i < n; ++i)
                        profilingThreadContextResetSampled(thread_state, ptrs[filled + i]);
                }
                progress += n;
                filled += n;
            }
        }

        /// For thread events other than prof sampling, trigger them as if there's a single allocation of size
        /// (n * usize). This is fine because:
        /// (a) these events do not alter the allocation itself, and
        /// (b) it's possible that some event would have been triggered multiple times, instead of only once, if the
        ///     allocations were handled individually, but it would do no harm (or even be beneficial) to coalesce
        ///     the triggerings.
        threadAllocationEvent(thread_state, progress * usable_size);

        if (progress < batch || profiling_sample_event)
        {
            void * p = allocateWithFlags(size, flags);
            if (p == nullptr)
            {
                /// OOM
                break;
            }
            if (progress == batch)
                ALLOCATOR_ASSERT(profilingSampled(thread_state, p));
            ptrs[filled++] = p;
        }
    }

    return filled;
}

}
