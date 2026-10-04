#pragma once

/// The internal allocation front-end: the `i*alloc*` functions, the tcache selection by index, the `malloc` and
/// `free` fast paths, and the initialization state.
/// jemalloc: `jemalloc_internal_inlines_c.h` (and `malloc_initialized`, `malloc_slow`, the junk callbacks and the
/// `TCACHE_IND_*` / `ARENA_IND_*` sentinels). The arena selection of `jemalloc_internal_inlines_a.h` /
/// `jemalloc_internal_inlines_b.h` is in Arenas.h, the tcache accessors in ThreadCache.h.
///
/// Translating the names of the `i` functions:
///   Abbreviations used in the first part of the function name (before alloc/dalloc) describe what that function
///   accomplishes:
///     a: arena (query)
///     s: size (query, or sized deallocation)
///     p: aligned (allocates)
///     vs: size (query, without knowing that the pointer is into the heap)
///     r: rallocx implementation
///     x: xallocx implementation
///   Abbreviations used in the second part of the function name (after alloc/dalloc) describe the arguments it takes:
///     z: whether to return zeroed memory
///     t: accepts a `ThreadCache *` parameter
///     m: accepts an `Arena *` parameter
///
/// The experimental hooks (`hook_invoke_*`, `hook_ralloc_args_t`) are dropped.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/CacheBin.h>
#include <allocator/Common.h>
#include <allocator/ExtentMap.h>
#include <allocator/Options.h>
#include <allocator/ProfHooks.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace jemalloc
{

/// --- Initialization state (jemalloc.c, jemalloc_internal_types.h) ---------------------------------------------------

/// The numeric values are jemalloc's: `malloc_initialized` compares with 0.
/// jemalloc: malloc_init_t
enum MallocInitState : uint8_t
{
    malloc_init_initialized = 0,
    malloc_init_recursible = 1,
    malloc_init_a0_initialized = 2,
    malloc_init_uninitialized = 3,
};

/// A plain (non-atomic) variable, like in jemalloc. Defined in Init.cpp. jemalloc: malloc_init_state
extern constinit MallocInitState malloc_init_state;

/// jemalloc: malloc_initialized
JE_ALWAYS_INLINE bool mallocInitialized()
{
    return malloc_init_state == malloc_init_initialized;
}

/// Initializes the allocator (Init.cpp). Returns true on error. jemalloc: malloc_init_hard
bool mallocInitHard();

/// jemalloc: malloc_init
JE_ALWAYS_INLINE bool mallocInit()
{
    if (JE_UNLIKELY(!mallocInitialized()) && mallocInitHard())
        return true;
    return false;
}

/// Whether the calling thread is the one initializing the allocator (for assertions; Init.cpp).
/// jemalloc: IS_INITIALIZER
bool mallocIsInitializer();

/// `stats.zero_reallocs`: the number of `realloc(ptr, 0)` calls (relaxed). Defined in Init.cpp.
/// jemalloc: zero_realloc_count
extern constinit std::atomic<size_t> zero_realloc_count;

/// --- Junk filling (jemalloc.c) ---------------------------------------------------------------------------------------

/// The documented values of the junk fill debugging facilities. jemalloc: junk_alloc_byte, junk_free_byte
inline constexpr uint8_t junk_alloc_byte = 0xa5;
inline constexpr uint8_t junk_free_byte = 0x5a;

/// jemalloc: junk_alloc_callback = default_junk_alloc (`JET_MUTABLE` is `const` outside of tests)
JE_ALWAYS_INLINE void junkAllocCallback(void * ptr, size_t usize)
{
    memset(ptr, junk_alloc_byte, usize);
}

/// jemalloc: junk_free_callback = default_junk_free
JE_ALWAYS_INLINE void junkFreeCallback(void * ptr, size_t usize)
{
    memset(ptr, junk_free_byte, usize);
}

/// --- Sentinels of the tcache / arena indices (jemalloc_internal_inlines_c.h) ------------------------------------

/// These correspond to the macros in jemalloc_macros.h (the representations need not be related).
/// jemalloc: TCACHE_IND_NONE, TCACHE_IND_AUTOMATIC, ARENA_IND_AUTOMATIC
inline constexpr unsigned TCACHE_IND_NONE = unsigned(-1);
inline constexpr unsigned TCACHE_IND_AUTOMATIC = unsigned(-2);
inline constexpr unsigned ARENA_IND_AUTOMATIC = unsigned(-1);

/// jemalloc: mallocx_tcache_get
JE_ALWAYS_INLINE unsigned mallocxTcacheIndGet(int flags)
{
    if (JE_LIKELY((flags & MALLOCX_TCACHE_MASK) == 0))
        return TCACHE_IND_AUTOMATIC;
    else if ((flags & MALLOCX_TCACHE_MASK) == MALLOCX_TCACHE_NONE_FLAG)
        return TCACHE_IND_NONE;
    else
        return mallocxTcacheGet(flags);
}

/// jemalloc: mallocx_arena_get
JE_ALWAYS_INLINE unsigned mallocxArenaIndGet(int flags)
{
    if (JE_UNLIKELY((flags & MALLOCX_ARENA_MASK) != 0))
        return mallocxArenaGet(flags);
    else
        return ARENA_IND_AUTOMATIC;
}

/// --- The `i` functions (jemalloc_internal_inlines_c.h) --------------------------------------------------------------

/// jemalloc: iaalloc
JE_ALWAYS_INLINE Arena * iaalloc(ThreadState * tsdn, const void * ptr)
{
    JE_ASSERT(ptr != nullptr);
    return arenaAalloc(tsdn, ptr);
}

/// jemalloc: isalloc
JE_ALWAYS_INLINE size_t isalloc(ThreadState * tsdn, const void * ptr)
{
    JE_ASSERT(ptr != nullptr);
    return arenaSalloc(tsdn, ptr);
}

/// jemalloc: iallocztm_explicit_slab
JE_ALWAYS_INLINE void * iallocztmExplicitSlab(
    ThreadState * tsdn, size_t size, szind_t ind, bool zero, bool slab, ThreadCache * tcache, bool is_internal, Arena * arena,
    bool slow_path)
{
    JE_ASSERT(!slab || sz::canUseSlab(size)); /// slab && large is illegal
    JE_ASSERT(!is_internal || tcache == nullptr);
    JE_ASSERT(!is_internal || arena == nullptr || arenaIsAuto(arena));

    void * ret = arenaMalloc(tsdn, arena, size, ind, zero, slab, tcache, slow_path);
    if (config::stats && is_internal && JE_LIKELY(ret != nullptr))
        arenaInternalAdd(iaalloc(tsdn, ret), isalloc(tsdn, ret));
    return ret;
}

/// jemalloc: iallocztm
JE_ALWAYS_INLINE void * iallocztm(
    ThreadState * tsdn, size_t size, szind_t ind, bool zero, ThreadCache * tcache, bool is_internal, Arena * arena, bool slow_path)
{
    bool slab = sz::canUseSlab(size);
    return iallocztmExplicitSlab(tsdn, size, ind, zero, slab, tcache, is_internal, arena, slow_path);
}

/// jemalloc: ialloc
JE_ALWAYS_INLINE void * ialloc(ThreadState & tsd, size_t size, szind_t ind, bool zero, bool slow_path)
{
    return iallocztm(&tsd, size, ind, zero, tcacheGet(tsd), false, nullptr, slow_path);
}

/// jemalloc: ipallocztm_explicit_slab
JE_ALWAYS_INLINE void * ipallocztmExplicitSlab(
    ThreadState * tsdn, size_t usize, size_t alignment, bool zero, bool slab, ThreadCache * tcache, bool is_internal, Arena * arena)
{
    JE_ASSERT(!slab || sz::canUseSlab(usize)); /// slab && large is illegal
    JE_ASSERT(usize != 0);
    JE_ASSERT(usize == sz::sa2u(usize, alignment));
    JE_ASSERT(!is_internal || tcache == nullptr);
    JE_ASSERT(!is_internal || arena == nullptr || arenaIsAuto(arena));

    void * ret = arenaPalloc(tsdn, arena, usize, alignment, zero, slab, tcache);
    JE_ASSERT(alignmentAddrToBase(ret, alignment) == ret);
    if (config::stats && is_internal && JE_LIKELY(ret != nullptr))
        arenaInternalAdd(iaalloc(tsdn, ret), isalloc(tsdn, ret));
    return ret;
}

/// jemalloc: ipallocztm
JE_ALWAYS_INLINE void * ipallocztm(
    ThreadState * tsdn, size_t usize, size_t alignment, bool zero, ThreadCache * tcache, bool is_internal, Arena * arena)
{
    return ipallocztmExplicitSlab(tsdn, usize, alignment, zero, sz::canUseSlab(usize), tcache, is_internal, arena);
}

/// jemalloc: ipalloct
JE_ALWAYS_INLINE void * ipalloct(ThreadState * tsdn, size_t usize, size_t alignment, bool zero, ThreadCache * tcache, Arena * arena)
{
    return ipallocztm(tsdn, usize, alignment, zero, tcache, false, arena);
}

/// jemalloc: ipalloct_explicit_slab
JE_ALWAYS_INLINE void * ipalloctExplicitSlab(
    ThreadState * tsdn, size_t usize, size_t alignment, bool zero, bool slab, ThreadCache * tcache, Arena * arena)
{
    return ipallocztmExplicitSlab(tsdn, usize, alignment, zero, slab, tcache, false, arena);
}

/// jemalloc: ipalloc
JE_ALWAYS_INLINE void * ipalloc(ThreadState & tsd, size_t usize, size_t alignment, bool zero)
{
    return ipallocztm(&tsd, usize, alignment, zero, tcacheGet(tsd), false, nullptr);
}

/// jemalloc: ivsalloc
JE_ALWAYS_INLINE size_t ivsalloc(ThreadState * tsdn, const void * ptr)
{
    return arenaVsalloc(tsdn, ptr);
}

/// jemalloc: idalloctm
JE_ALWAYS_INLINE void idalloctm(
    ThreadState * tsdn, void * ptr, ThreadCache * tcache, AllocContext * alloc_ctx, bool is_internal, bool slow_path)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(!is_internal || tcache == nullptr);
    JE_ASSERT(!is_internal || arenaIsAuto(iaalloc(tsdn, ptr)));
    if (config::stats && is_internal)
        arenaInternalSub(iaalloc(tsdn, ptr), isalloc(tsdn, ptr));
    if (!is_internal && tsdn != nullptr && tsdn->reentrancyLevel() != 0)
        JE_ASSERT(tcache == nullptr);
    arenaDalloc(tsdn, ptr, tcache, alloc_ctx, slow_path);
}

/// jemalloc: idalloc
JE_ALWAYS_INLINE void idalloc(ThreadState & tsd, void * ptr)
{
    idalloctm(&tsd, ptr, tcacheGet(tsd), nullptr, false, true);
}

/// jemalloc: isdalloct
JE_ALWAYS_INLINE void isdalloct(ThreadState * tsdn, void * ptr, size_t size, ThreadCache * tcache, AllocContext * alloc_ctx, bool slow_path)
{
    arenaSdalloc(tsdn, ptr, size, tcache, alloc_ctx, slow_path);
}

/// jemalloc: iralloct_realign
JE_ALWAYS_INLINE void * iralloctRealign(
    ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t alignment, bool zero, bool slab, ThreadCache * tcache,
    Arena * arena)
{
    size_t usize = sz::sa2u(size, alignment);
    if (JE_UNLIKELY(usize == 0 || usize > SC_LARGE_MAXCLASS))
        return nullptr;
    void * p = ipalloctExplicitSlab(tsdn, usize, alignment, zero, slab, tcache, arena);
    if (p == nullptr)
        return nullptr;
    /// Copy at most size bytes (not size+extra), since the caller has no expectation that the extra bytes will be
    /// reliably preserved.
    size_t copysize = (size < oldsize) ? size : oldsize;
    memcpy(p, ptr, copysize);
    isdalloct(tsdn, ptr, oldsize, tcache, nullptr, true);
    return p;
}

/// jemalloc: iralloct_explicit_slab
JE_ALWAYS_INLINE void * iralloctExplicitSlab(
    ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t alignment, bool zero, bool slab, ThreadCache * tcache,
    Arena * arena)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size != 0);

    if (alignment != 0 && (reinterpret_cast<uintptr_t>(ptr) & (uintptr_t(alignment) - 1)) != 0)
    {
        /// Existing object alignment is inadequate; allocate new space and copy.
        return iralloctRealign(tsdn, ptr, oldsize, size, alignment, zero, slab, tcache, arena);
    }

    return arenaRalloc(tsdn, arena, ptr, oldsize, size, alignment, zero, slab, tcache);
}

/// jemalloc: iralloct
JE_ALWAYS_INLINE void * iralloct(
    ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t alignment, size_t usize, bool zero, ThreadCache * tcache,
    Arena * arena)
{
    bool slab = sz::canUseSlab(usize);
    return iralloctExplicitSlab(tsdn, ptr, oldsize, size, alignment, zero, slab, tcache, arena);
}

/// jemalloc: iralloc
JE_ALWAYS_INLINE void * iralloc(ThreadState & tsd, void * ptr, size_t oldsize, size_t size, size_t alignment, size_t usize, bool zero)
{
    return iralloct(&tsd, ptr, oldsize, size, alignment, usize, zero, tcacheGet(tsd), nullptr);
}

/// Returns true if the allocation could not be resized in place (`*newsize` is the resulting usable size).
/// jemalloc: ixalloc
JE_ALWAYS_INLINE bool ixalloc(
    ThreadState * tsdn, void * ptr, size_t oldsize, size_t size, size_t extra, size_t alignment, bool zero, size_t * newsize)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size != 0);

    if (alignment != 0 && (reinterpret_cast<uintptr_t>(ptr) & (uintptr_t(alignment) - 1)) != 0)
    {
        /// Existing object alignment is inadequate.
        *newsize = oldsize;
        return true;
    }

    return arenaRallocNoMove(tsdn, ptr, oldsize, size, extra, zero, newsize);
}

/// --- Fast paths (jemalloc_internal_inlines_c.h) ---------------------------------------------------------------------

/// jemalloc: fastpath_success_finish
JE_ALWAYS_INLINE void fastpathSuccessFinish(ThreadState * tsd, uint64_t allocated_after, CacheBin * bin, void * /*ret*/)
{
    tsd->thread_allocated = allocated_after;
    if constexpr (config::stats)
        ++bin->tstats.nrequests;
}

/// The `malloc` fast path. Assumes `size <= SC_LOOKUP_MAXCLASS` and that the tcache bin is not empty; otherwise (and
/// for the uninitialized / slow / event-triggering cases, which are all folded into one threshold comparison) it
/// tail-calls `fallback_alloc`, which has the signature of `malloc`, so that no call frame is set up in the common
/// case.
/// jemalloc: imalloc_fastpath
template <void * (*fallback_alloc)(size_t)>
JE_ALWAYS_INLINE void * imallocFastpath(size_t size)
{
    if (Tsd::get_allocates && JE_UNLIKELY(!mallocInitialized()))
        return fallback_alloc(size);

    ThreadState * tsd = Tsd::get(false);
    if (JE_UNLIKELY((size > SC_LOOKUP_MAXCLASS) || tsd == nullptr))
        return fallback_alloc(size);

    /// The code below till the branch checking the next_event threshold may execute before `mallocInit`, in which case
    /// the threshold is 0 to trigger slow path and initialization. Note that when uninitialized, only the fast-path
    /// variants of the sz / tsd facilities may be called.
    szind_t ind;
    /// The `thread_allocated` counter in tsd serves as a general purpose accumulator for bytes of allocation to
    /// trigger different types of events. usize is always needed to advance thread_allocated, though it's not always
    /// needed in the core allocation logic.
    size_t usize;
    sz::sizeToIndexUsizeFastpath(size, &ind, &usize);
    /// Fast path relies on size being a bin.
    JE_ASSERT(ind < SC_NBINS);
    static_assert(SC_LOOKUP_MAXCLASS < SC_SMALL_MAXCLASS);

    uint64_t allocated;
    uint64_t threshold;
    teMallocFastpathCtx(*tsd, allocated, threshold);
    uint64_t allocated_after = allocated + usize;
    /// The ind and usize might be uninitialized (or partially) before `mallocInit`. The assertions check for: 1) full
    /// correctness (usize & ind) when initialized; and 2) guaranteed slow-path (threshold == 0) when !initialized.
    if (!mallocInitialized())
    {
        JE_ASSERT(threshold == 0);
    }
    else
    {
        JE_ASSERT(ind == sz::sizeToIndex(size));
        JE_ASSERT(usize > 0 && usize == sz::indexToSize(ind));
    }
    /// Check for events and tsd non-nominal (fast_threshold will be set to 0) in a single branch.
    if (JE_UNLIKELY(allocated_after >= threshold))
        return fallback_alloc(size);
    JE_ASSERT(tsd->fast());

    ThreadCache * tcache = tsd->tcacheGet();
    JE_ASSERT(tcache == tcacheGet(*tsd));
    CacheBin * bin = &tcache->bins[ind];

    /// We split up the code this way so that redundant low-water computation doesn't happen on the (more common) case
    /// in which we don't touch the low water mark. The compiler won't do this duplication on its own.
    bool tcache_success;
    void * ret = bin->allocEasy(tcache_success);
    if (tcache_success)
    {
        fastpathSuccessFinish(tsd, allocated_after, bin, ret);
        return ret;
    }
    ret = bin->alloc(tcache_success);
    if (tcache_success)
    {
        fastpathSuccessFinish(tsd, allocated_after, bin, ret);
        return ret;
    }

    return fallback_alloc(size);
}

/// jemalloc: tcache_get_from_ind
JE_ALWAYS_INLINE ThreadCache * tcacheGetFromInd(ThreadState & tsd, unsigned tcache_ind, bool slow, bool is_alloc)
{
    ThreadCache * tcache;
    if (tcache_ind == TCACHE_IND_AUTOMATIC)
    {
        if (JE_LIKELY(!slow))
        {
            /// Getting tcache ptr unconditionally.
            tcache = tsd.tcacheGet();
            JE_ASSERT(tcache == tcacheGet(tsd));
        }
        else if (is_alloc || JE_LIKELY(tsd.reentrancyLevel() == 0))
        {
            tcache = tcacheGet(tsd);
        }
        else
        {
            tcache = nullptr;
        }
    }
    else
    {
        /// Should not specify tcache on deallocation path when being reentrant.
        JE_ASSERT(is_alloc || tsd.reentrancyLevel() == 0 || tsd.stateNocleanup());
        if (tcache_ind == TCACHE_IND_NONE)
            tcache = nullptr;
        else
            tcache = tcachesGet(tsd, tcache_ind);
    }
    return tcache;
}

/// Only with `config_opt_size_checks` (never enabled in ClickHouse). Returns true on a detected mismatch.
/// jemalloc: maybe_check_alloc_ctx
JE_ALWAYS_INLINE bool maybeCheckAllocCtx(ThreadState & tsd, void * ptr, AllocContext * alloc_ctx)
{
    if constexpr (config::opt_size_checks)
    {
        AllocContext dbg_ctx;
        arena_emap_global.allocCtxLookup(&tsd, ptr, &dbg_ctx);
        if (alloc_ctx->szind != dbg_ctx.szind)
        {
            safetyCheckFailSizedDealloc(
                /* current_dealloc */ true, ptr, /* true_size */ dbg_ctx.usizeGet(), /* input_size */ alloc_ctx->usizeGet());
            return true;
        }
        if (alloc_ctx->slab != dbg_ctx.slab)
        {
            safetyCheckFail("Internal heap corruption detected: mismatch in slab bit");
            return true;
        }
    }
    else
    {
        (void)tsd;
        (void)ptr;
        (void)alloc_ctx;
    }
    return false;
}

/// The free fast path does not handle two uncommon cases: 1) sampled profiled objects and 2) sampled junk & stash for
/// use-after-free detection. Both have special alignments which are used to escape the fast path. `prof_sample` is
/// page-aligned, which covers the UAF check when both are enabled. At most one runtime branch.
/// jemalloc: free_fastpath_nonfast_aligned
JE_ALWAYS_INLINE bool freeFastpathNonfastAligned(void * ptr, bool check_prof)
{
    if constexpr (config::debug)
    {
        if (cacheBinNonfastAligned(ptr))
            JE_ASSERT(profSampleAligned(ptr));
    }

    if (config::prof && check_prof)
    {
        /// When prof is enabled, the prof_sample alignment is enough.
        return profSampleAligned(ptr);
    }

    if constexpr (config::uaf_detection)
        return cacheBinNonfastAligned(ptr);

    return false;
}

/// Returns whether or not the free attempt was successful.
/// jemalloc: free_fastpath
JE_ALWAYS_INLINE bool freeFastpath(void * ptr, size_t size, bool size_hint)
{
    ThreadState * tsd = Tsd::get(false);
    /// The branch gets optimized away unless the TSD implementation allocates.
    if (JE_UNLIKELY(tsd == nullptr))
        return false;
    /// The `tsd_fast` / initialized checks are folded into the branch testing (deallocated_after >= threshold) later in
    /// this function. The threshold will be set to 0 when !tsd_fast.
    JE_ASSERT(tsd->fast() || tsd->thread_deallocated_next_event_fast == 0);

    AllocContext alloc_ctx{0, 0, false};
    size_t usize;
    if (!size_hint)
    {
        bool err = arena_emap_global.allocCtxTryLookupFast(*tsd, ptr, &alloc_ctx);

        /// Note: profiled objects will have alloc_ctx.slab set.
        if (JE_UNLIKELY(err || !alloc_ctx.slab || freeFastpathNonfastAligned(ptr, /* check_prof */ false)))
            return false;
        JE_ASSERT(alloc_ctx.szind != SC_NSIZES);
        usize = sz::indexToSize(alloc_ctx.szind);
    }
    else
    {
        /// Check for both sizes that are too large, and for sampled / special aligned objects. The alignment check will
        /// also check for null ptr.
        if (JE_UNLIKELY(size > SC_LOOKUP_MAXCLASS || freeFastpathNonfastAligned(ptr, /* check_prof */ true)))
            return false;
        sz::sizeToIndexUsizeFastpath(size, &alloc_ctx.szind, &usize);
        /// Max lookup class must be small.
        JE_ASSERT(alloc_ctx.szind < SC_NBINS);
        /// This is a dead store, except when opt size checking is on.
        alloc_ctx.slab = true;
    }
    /// Currently the fast path only handles small sizes. The branch on SC_LOOKUP_MAXCLASS makes sure of it. This lets
    /// us avoid checking the tcache szind upper limit (i.e. tcache_max) as well.
    JE_ASSERT(alloc_ctx.slab);

    uint64_t deallocated;
    uint64_t threshold;
    teFreeFastpathCtx(*tsd, deallocated, threshold);

    uint64_t deallocated_after = deallocated + usize;
    /// Check for events and tsd non-nominal (fast_threshold will be set to 0) in a single branch. Note that this
    /// handles the uninitialized case as well (TSD init will be triggered on the non-fastpath). Therefore anything
    /// that depends on a functional TSD (e.g. the alloc_ctx sanity check below) needs to be after this branch.
    if (JE_UNLIKELY(deallocated_after >= threshold))
        return false;
    JE_ASSERT(tsd->fast());
    bool fail = maybeCheckAllocCtx(*tsd, ptr, &alloc_ctx);
    if (fail)
    {
        /// See the comment in isfree.
        return true;
    }

    ThreadCache * tcache = tcacheGetFromInd(*tsd, TCACHE_IND_AUTOMATIC, /* slow */ false, /* is_alloc */ false);
    CacheBin * bin = &tcache->bins[alloc_ctx.szind];

    /// If junking were enabled, this is where we would do it. It's not though, since we ensured above that we're on
    /// the fast path.
    JE_ASSERT(!opt.junk_free);

    if (!bin->dallocEasy(ptr))
        return false;

    tsd->thread_deallocated = deallocated_after;

    return true;
}

/// The slow paths of `malloc`, `free`, `sdallocx` (noinline, defined in Api.cpp).
/// jemalloc: malloc_default, free_default, sdallocx_default
JE_NOINLINE void * mallocDefault(size_t size);
JE_NOINLINE void freeDefault(void * ptr);
JE_NOINLINE void sdallocxDefault(void * ptr, size_t size, int flags);

/// jemalloc: je_sdallocx_noflags
JE_ALWAYS_INLINE void sdallocxNoflags(void * ptr, size_t size)
{
    if (!freeFastpath(ptr, size, true))
        sdallocxDefault(ptr, size, 0);
}

/// jemalloc: je_sdallocx_impl
JE_ALWAYS_INLINE void sdallocxImpl(void * ptr, size_t size, int flags)
{
    if (flags != 0 || !freeFastpath(ptr, size, true))
        sdallocxDefault(ptr, size, flags);
}

/// jemalloc: je_free_impl
JE_ALWAYS_INLINE void freeImpl(void * ptr)
{
    if (!freeFastpath(ptr, 0, false))
        freeDefault(ptr);
}

}
