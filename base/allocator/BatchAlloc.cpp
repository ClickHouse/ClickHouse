/// `experimental.batch_alloc` (jemalloc: `batch_alloc` in `src/jemalloc.c`). In the core library (not Api.cpp)
/// because the mallctl tree references it.

#include <allocator/Imalloc.h>

namespace jemalloc
{

namespace
{

/// jemalloc: prof_sampled
[[maybe_unused]] bool profSampled(ThreadState & tsd, const void * ptr)
{
    ProfInfo prof_info;
    profInfoGet(tsd, ptr, nullptr, &prof_info);
    return profTctxIsValid(prof_info.alloc_tctx);
}

/// jemalloc: batch_alloc_prof_sample_assert
void batchAllocProfSampleAssert([[maybe_unused]] ThreadState & tsd, [[maybe_unused]] size_t batch, [[maybe_unused]] size_t usize)
{
    JE_ASSERT(config::prof && opt.prof);
    if constexpr (config::debug)
    {
        bool prof_sample_event = teProfSampleEventLookahead(tsd, batch * usize);
        JE_ASSERT(!prof_sample_event);
        size_t surplus;
        prof_sample_event = teProfSampleEventLookaheadSurplus(tsd, (batch + 1) * usize, &surplus);
        JE_ASSERT(prof_sample_event);
        JE_ASSERT(surplus < usize);
    }
}

}

/// jemalloc: batch_alloc
size_t batchAlloc(void ** ptrs, size_t num, size_t size, int flags)
{
    ThreadState & tsd = ThreadState::fetch();

    size_t filled = 0;

    if (JE_UNLIKELY(tsd.reentrancyLevel() > 0))
        return filled;

    size_t alignment = mallocxAlignGet(flags);
    size_t usize;
    if (alignedUsizeGet(size, alignment, &usize, nullptr, false))
        return filled;
    szind_t ind = sz::sizeToIndex(usize);
    bool zero = zeroGet(mallocxZeroGet(flags), /* slow */ true);

    /// The cache bin and arena will be lazily initialized; it's hard to know in advance whether each of them needs
    /// to be initialized.
    CacheBin * bin = nullptr;
    Arena * arena = nullptr;

    size_t nregs = 0;
    if (JE_LIKELY(ind < SC_NBINS))
    {
        nregs = bin_infos[ind].nregs;
        JE_ASSERT(nregs > 0);
    }

    while (filled < num)
    {
        size_t batch = num - filled;
        size_t surplus = SIZE_MAX; /// Dead store.
        bool prof_sample_event = config::prof && opt.prof && profActiveGetUnlocked()
            && teProfSampleEventLookaheadSurplus(tsd, batch * usize, &surplus);

        if (prof_sample_event)
        {
            /// Adjust so that the batch does not trigger prof sampling.
            batch -= surplus / usize + 1;
            batchAllocProfSampleAssert(tsd, batch, usize);
        }

        size_t progress = 0;

        if (JE_LIKELY(ind < SC_NBINS) && batch >= nregs)
        {
            if (arena == nullptr)
            {
                unsigned arena_ind = mallocxArenaIndGet(flags);
                if (arenaGetFromInd(tsd, arena_ind, &arena))
                    return filled;
                if (arena == nullptr)
                    arena = arenaChoose(tsd, nullptr);
                if (JE_UNLIKELY(arena == nullptr))
                    return filled;
            }
            size_t arena_batch = batch - batch % nregs;
            size_t n = arenaFillSmallFresh(&tsd, arena, ind, ptrs + filled, arena_batch, zero);
            progress += n;
            filled += n;
        }

        unsigned tcache_ind = mallocxTcacheIndGet(flags);
        ThreadCache * tcache = tcacheGetFromInd(tsd, tcache_ind, /* slow */ true, /* is_alloc */ true);
        if (JE_LIKELY(
                tcache != nullptr && ind < tcacheNbinsGet(tcache->tcache_slow)
                && !tcacheBinDisabled(ind, &tcache->bins[ind], tcache->tcache_slow))
            && progress < batch)
        {
            if (bin == nullptr)
                bin = &tcache->bins[ind];
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
                    bin->tstats.nrequests += n;
                if (zero)
                {
                    for (size_t i = 0; i < n; ++i)
                        memset(ptrs[filled + i], 0, usize);
                }
                if (config::prof && opt.prof && JE_UNLIKELY(ind >= SC_NBINS))
                {
                    for (size_t i = 0; i < n; ++i)
                        profTctxResetSampled(tsd, ptrs[filled + i]);
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
        threadAllocEvent(tsd, progress * usize);

        if (progress < batch || prof_sample_event)
        {
            void * p = mallocx(size, flags);
            if (p == nullptr)
            {
                /// OOM
                break;
            }
            if (progress == batch)
                JE_ASSERT(profSampled(tsd, p));
            ptrs[filled++] = p;
        }
    }

    return filled;
}

}
