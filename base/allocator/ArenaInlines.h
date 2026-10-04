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
JE_ALWAYS_INLINE void arenaProfInfoGet(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx, ProfInfo * prof_info, bool reset_recent)
{
    static_assert(config::prof);
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(prof_info != nullptr);

    Extent * edata = nullptr;
    bool is_slab;

    /// Static check.
    if (alloc_ctx == nullptr)
    {
        edata = arena_emap_global.edataLookup(&tsd, ptr);
        is_slab = edata->slab();
    }
    else if (JE_UNLIKELY(!(is_slab = alloc_ctx->slab)))
    {
        edata = arena_emap_global.edataLookup(&tsd, ptr);
    }

    if (JE_UNLIKELY(!is_slab))
    {
        /// edata must have been initialized at this point.
        JE_ASSERT(edata != nullptr);
        size_t usize = (alloc_ctx == nullptr) ? edata->usize() : alloc_ctx->usizeGet();
        if (reset_recent && largeDallocSafetyChecks(edata, ptr, usize))
        {
            prof_info->alloc_tctx = PROF_TCTX_SENTINEL;
            return;
        }
        largeProfInfoGet(tsd, edata, prof_info, reset_recent);
    }
    else
    {
        /// No need to set other fields in prof_info; they will never be accessed if alloc_tctx == PROF_TCTX_SENTINEL.
        prof_info->alloc_tctx = PROF_TCTX_SENTINEL;
    }
}

/// jemalloc: arena_prof_tctx_reset
JE_ALWAYS_INLINE void arenaProfTctxReset(ThreadState & tsd, const void * ptr, AllocContext * alloc_ctx)
{
    static_assert(config::prof);
    JE_ASSERT(ptr != nullptr);

    /// Static check.
    if (alloc_ctx == nullptr)
    {
        Extent * edata = arena_emap_global.edataLookup(&tsd, ptr);
        if (JE_UNLIKELY(!edata->slab()))
            largeProfTctxReset(edata);
    }
    else
    {
        if (JE_UNLIKELY(!alloc_ctx->slab))
        {
            Extent * edata = arena_emap_global.edataLookup(&tsd, ptr);
            largeProfTctxReset(edata);
        }
    }
}

/// jemalloc: arena_prof_tctx_reset_sampled
JE_ALWAYS_INLINE void arenaProfTctxResetSampled(ThreadState & tsd, const void * ptr)
{
    static_assert(config::prof);
    JE_ASSERT(ptr != nullptr);

    Extent * edata = arena_emap_global.edataLookup(&tsd, ptr);
    JE_ASSERT(!edata->slab());

    largeProfTctxReset(edata);
}

/// jemalloc: arena_prof_info_set
JE_ALWAYS_INLINE void arenaProfInfoSet(ThreadState & /*tsd*/, Extent * edata, ProfThreadContext * tctx, size_t size)
{
    static_assert(config::prof);
    JE_ASSERT(!edata->slab());
    largeProfInfoSet(edata, tctx, size);
}

/// jemalloc: arena_malloc
JE_ALWAYS_INLINE void * arenaMalloc(
    ThreadState * tsdn, Arena * arena, size_t size, szind_t ind, bool zero, bool slab, ThreadCache * tcache, bool slow_path)
{
    JE_ASSERT(tsdn != nullptr || tcache == nullptr);

    if (JE_LIKELY(tcache != nullptr))
    {
        if (JE_LIKELY(slab))
        {
            JE_ASSERT(sz::canUseSlab(size));
            return tcacheAllocSmall(*tsdn, arena, tcache, size, ind, zero, slow_path);
        }
        else if (JE_LIKELY(
                     ind < tcacheNbinsGet(tcache->tcache_slow) && !tcacheBinDisabled(ind, &tcache->bins[ind], tcache->tcache_slow)))
        {
            return tcacheAllocLarge(*tsdn, arena, tcache, size, ind, zero, slow_path);
        }
        /// (size > tcache_max) case falls through.
    }

    return arenaMallocHard(tsdn, arena, size, ind, zero, slab);
}

/// jemalloc: arena_aalloc
JE_ALWAYS_INLINE Arena * arenaAalloc(ThreadState * tsdn, const void * ptr)
{
    Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
    unsigned arena_ind = edata->arenaInd();
    return arenas[arena_ind].load(std::memory_order_relaxed);
}

/// jemalloc: arena_salloc
JE_ALWAYS_INLINE size_t arenaSalloc(ThreadState * tsdn, const void * ptr)
{
    JE_ASSERT(ptr != nullptr);
    AllocContext alloc_ctx;
    arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);
    JE_ASSERT(alloc_ctx.szind != SC_NSIZES);

    return alloc_ctx.usizeGet();
}

/// Return 0 if ptr is not within an extent managed by jemalloc. This function has two extra costs relative to
/// `isalloc`:
/// - The rtree calls cannot claim to be dependent lookups, which induces rtree lookup load dependencies.
/// - The lookup may fail, so there is an extra branch to check for failure.
/// jemalloc: arena_vsalloc
JE_ALWAYS_INLINE size_t arenaVsalloc(ThreadState * tsdn, const void * ptr)
{
    FullAllocContext full_alloc_ctx;
    bool missing = arena_emap_global.fullAllocCtxTryLookup(tsdn, ptr, &full_alloc_ctx);
    if (missing)
        return 0;

    if (full_alloc_ctx.edata == nullptr)
        return 0;
    JE_ASSERT(full_alloc_ctx.edata->state() == extent_state_active);
    /// Only slab members should be looked up via interior pointers.
    JE_ASSERT(full_alloc_ctx.edata->addr() == ptr || full_alloc_ctx.edata->slab());

    JE_ASSERT(full_alloc_ctx.szind != SC_NSIZES);

    return full_alloc_ctx.edata->usize();
}

/// `szind` is still needed in this function mainly because `szind < SC_NBINS` determines not only if this is a small
/// alloc, but also if `szind` is valid (an inactive extent would have `szind == SC_NSIZES`).
/// jemalloc: arena_dalloc_large_no_tcache
inline void arenaDallocLargeNoTcache(ThreadState * tsdn, void * ptr, szind_t szind, size_t usize)
{
    if (config::prof && JE_UNLIKELY(szind < SC_NBINS))
    {
        arenaDallocPromoted(tsdn, ptr, nullptr, true);
    }
    else
    {
        Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
        if (largeDallocSafetyChecks(edata, ptr, usize))
        {
            /// See the comment in isfree.
            return;
        }
        largeDalloc(tsdn, edata);
    }
}

/// jemalloc: arena_dalloc_no_tcache
inline void arenaDallocNoTcache(ThreadState * tsdn, void * ptr)
{
    JE_ASSERT(ptr != nullptr);

    AllocContext alloc_ctx;
    arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);

    if constexpr (config::debug)
    {
        Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
        JE_ASSERT(alloc_ctx.szind == edata->szind());
        JE_ASSERT(alloc_ctx.szind < SC_NSIZES);
        JE_ASSERT(alloc_ctx.slab == edata->slab());
        JE_ASSERT(alloc_ctx.usizeGet() == edata->usize());
    }

    if (JE_LIKELY(alloc_ctx.slab))
    {
        /// Small allocation.
        arenaDallocSmall(tsdn, ptr);
    }
    else
    {
        arenaDallocLargeNoTcache(tsdn, ptr, alloc_ctx.szind, alloc_ctx.usizeGet());
    }
}

/// jemalloc: arena_dalloc_large
JE_ALWAYS_INLINE void arenaDallocLarge(ThreadState * tsdn, void * ptr, ThreadCache * tcache, szind_t szind, size_t usize, bool slow_path)
{
    JE_ASSERT(tsdn != nullptr && tcache != nullptr);
    bool is_sample_promoted = config::prof && szind < SC_NBINS;
    if (JE_UNLIKELY(is_sample_promoted))
    {
        arenaDallocPromoted(tsdn, ptr, tcache, slow_path);
    }
    else
    {
        if (szind < tcacheNbinsGet(tcache->tcache_slow) && !tcacheBinDisabled(szind, &tcache->bins[szind], tcache->tcache_slow))
        {
            tcacheDallocLarge(*tsdn, tcache, ptr, szind, slow_path);
        }
        else
        {
            Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
            if (largeDallocSafetyChecks(edata, ptr, usize))
            {
                /// See the comment in isfree.
                return;
            }
            largeDalloc(tsdn, edata);
        }
    }
}

/// Only in debug builds: detects double frees of small regions. Returns true if the deallocation must be skipped.
/// jemalloc: arena_tcache_dalloc_small_safety_check
JE_ALWAYS_INLINE bool arenaTcacheDallocSmallSafetyCheck(ThreadState * tsdn, void * ptr)
{
    if constexpr (!config::debug)
    {
        return false;
    }
    else
    {
        Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
        szind_t binind = edata->szind();
        DivInfo div_info = arena_binind_div_info[binind];
        /// Calls the internal function `slabRegindImpl` because the safety check does not require a lock.
        size_t regind = Bin::slabRegindImpl(div_info, binind, edata, ptr);
        SlabData * slab_data = edata->slabData();
        const BinInfo & bin_info = bin_infos[binind];
        JE_ASSERT(edata->nfree() < bin_info.nregs);
        if (JE_UNLIKELY(!bitmapGet(slab_data->bitmap, bin_info.bitmap_info, regind)))
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
JE_ALWAYS_INLINE void arenaDalloc(ThreadState * tsdn, void * ptr, ThreadCache * tcache, AllocContext * caller_alloc_ctx, bool slow_path)
{
    JE_ASSERT(tsdn != nullptr || tcache == nullptr);
    JE_ASSERT(ptr != nullptr);

    if (JE_UNLIKELY(tcache == nullptr))
    {
        arenaDallocNoTcache(tsdn, ptr);
        return;
    }

    AllocContext alloc_ctx;
    if (caller_alloc_ctx != nullptr)
    {
        alloc_ctx = *caller_alloc_ctx;
    }
    else
    {
        __builtin_assume(tsdn != nullptr);
        arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);
    }

    if constexpr (config::debug)
    {
        Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
        JE_ASSERT(alloc_ctx.szind == edata->szind());
        JE_ASSERT(alloc_ctx.szind < SC_NSIZES);
        JE_ASSERT(alloc_ctx.slab == edata->slab());
        JE_ASSERT(alloc_ctx.usizeGet() == edata->usize());
    }

    if (JE_LIKELY(alloc_ctx.slab))
    {
        /// Small allocation.
        if (arenaTcacheDallocSmallSafetyCheck(tsdn, ptr))
            return;
        tcacheDallocSmall(*tsdn, tcache, ptr, alloc_ctx.szind, slow_path);
    }
    else
    {
        arenaDallocLarge(tsdn, ptr, tcache, alloc_ctx.szind, alloc_ctx.usizeGet(), slow_path);
    }
}

/// jemalloc: arena_sdalloc_no_tcache
inline void arenaSdallocNoTcache(ThreadState * tsdn, void * ptr, size_t size)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size <= SC_LARGE_MAXCLASS);

    AllocContext alloc_ctx;
    if (!config::prof || !opt.prof)
    {
        /// There is no risk of being confused by a promoted sampled object, so base szind and slab on the given size.
        szind_t szind = sz::sizeToIndex(size);
        alloc_ctx.init(szind, (szind < SC_NBINS), size);
    }

    if ((config::prof && opt.prof) || config::debug)
    {
        arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);

        JE_ASSERT(alloc_ctx.szind == sz::sizeToIndex(size));
        JE_ASSERT((config::prof && opt.prof) || alloc_ctx.slab == (alloc_ctx.szind < SC_NBINS));

        if constexpr (config::debug)
        {
            Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
            JE_ASSERT(alloc_ctx.szind == edata->szind());
            JE_ASSERT(alloc_ctx.slab == edata->slab());
        }
    }

    if (JE_LIKELY(alloc_ctx.slab))
    {
        /// Small allocation.
        arenaDallocSmall(tsdn, ptr);
    }
    else
    {
        arenaDallocLargeNoTcache(tsdn, ptr, alloc_ctx.szind, alloc_ctx.usizeGet());
    }
}

/// jemalloc: arena_sdalloc
JE_ALWAYS_INLINE void arenaSdalloc(
    ThreadState * tsdn, void * ptr, size_t size, ThreadCache * tcache, AllocContext * caller_alloc_ctx, bool slow_path)
{
    JE_ASSERT(tsdn != nullptr || tcache == nullptr);
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size <= SC_LARGE_MAXCLASS);

    if (JE_UNLIKELY(tcache == nullptr))
    {
        arenaSdallocNoTcache(tsdn, ptr, size);
        return;
    }

    AllocContext alloc_ctx;
    if (config::prof && opt.prof)
    {
        if (caller_alloc_ctx == nullptr)
        {
            /// Uncommon case and should be a static check.
            arena_emap_global.allocCtxLookup(tsdn, ptr, &alloc_ctx);
            JE_ASSERT(alloc_ctx.szind == sz::sizeToIndex(size));
            JE_ASSERT(alloc_ctx.usizeGet() == size);
        }
        else
        {
            alloc_ctx = *caller_alloc_ctx;
        }
    }
    else
    {
        /// There is no risk of being confused by a promoted sampled object, so base szind and slab on the given size.
        alloc_ctx.szind = sz::sizeToIndex(size);
        alloc_ctx.slab = (alloc_ctx.szind < SC_NBINS);
    }

    if constexpr (config::debug)
    {
        Extent * edata = arena_emap_global.edataLookup(tsdn, ptr);
        JE_ASSERT(alloc_ctx.szind == edata->szind());
        JE_ASSERT(alloc_ctx.slab == edata->slab());
        alloc_ctx.init(alloc_ctx.szind, alloc_ctx.slab, sz::s2u(size));
        JE_ASSERT(alloc_ctx.usizeGet() == edata->usize());
    }

    if (JE_LIKELY(alloc_ctx.slab))
    {
        /// Small allocation.
        if (arenaTcacheDallocSmallSafetyCheck(tsdn, ptr))
            return;
        tcacheDallocSmall(*tsdn, tcache, ptr, alloc_ctx.szind, slow_path);
    }
    else
    {
        arenaDallocLarge(tsdn, ptr, tcache, alloc_ctx.szind, sz::s2u(size), slow_path);
    }
}

}
