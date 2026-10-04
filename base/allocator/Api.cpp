/// The exported C API (jemalloc: the "malloc(3)-compatible functions" and "non-standard functions" of
/// `src/jemalloc.c`): `je_malloc`, `je_free`, ..., `je_mallctl*`, `je_malloc_stats_print`.
///
/// Only the `je_*` functions (and the FreeBSD fork hooks in Fork.cpp) have default visibility; everything else is in
/// `namespace jemalloc` with hidden visibility. `je_malloc_message` is defined in Format.cpp, `je_malloc_conf` and
/// `je_malloc_conf_2_conf_harder` in Conf.cpp.
///
/// Dropped: the experimental hooks (`hook_invoke_*`), `UTRACE` (not configured), `smallocx`,
/// `je_memalign` / `je_valloc` / `je_pvalloc` (not configured: ClickHouse implements them itself). `opt.xmalloc` can
/// never be enabled (`JEMALLOC_XMALLOC` is not configured, the option is rejected), but its messages are kept.
///
/// The allocation machinery (`imalloc`) is in Imalloc.h; `batch_alloc` is in BatchAlloc.cpp.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/Common.h>
#include <allocator/Ctl.h>
#include <allocator/ExtentMap.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Imalloc.h>
#include <allocator/Init.h>
#include <allocator/Options.h>
#include <allocator/ProfHooks.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/Stats.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdlib>
#include <cstring>

/// The public declarations (`jemalloc/jemalloc.h` without `jemalloc_typedefs.h`, which has no include guard and is
/// already included by ExtentHooks.h, and without the renaming header).
extern "C"
{
#include <jemalloc/jemalloc_defs.h>
#include <jemalloc/jemalloc_macros.h>
#include <jemalloc/jemalloc_protos.h>
}

namespace jemalloc
{

namespace
{

/// jemalloc: ifree
JE_ALWAYS_INLINE void ifree(ThreadState & tsd, void * ptr, ThreadCache * tcache, bool slow_path)
{
    if (!slow_path)
        tsd.assertFast();
    if (tsd.reentrancyLevel() != 0)
        JE_ASSERT(slow_path);

    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());

    AllocContext alloc_ctx;
    arena_emap_global.allocCtxLookup(&tsd, ptr, &alloc_ctx);
    JE_ASSERT(alloc_ctx.szind != SC_NSIZES);

    size_t usize = alloc_ctx.usizeGet();
    if (config::prof && opt.prof)
        profFree(tsd, ptr, usize, &alloc_ctx);

    if (JE_LIKELY(!slow_path))
    {
        idalloctm(&tsd, ptr, tcache, &alloc_ctx, false, false);
    }
    else
    {
        if (config::fill && slow_path && opt.junk_free)
            junkFreeCallback(ptr, usize);
        idalloctm(&tsd, ptr, tcache, &alloc_ctx, false, true);
    }
    threadDallocEvent(tsd, usize);
}

/// jemalloc: isfree
JE_ALWAYS_INLINE void isfree(ThreadState & tsd, void * ptr, size_t usize, ThreadCache * tcache, bool slow_path)
{
    if (!slow_path)
        tsd.assertFast();
    if (tsd.reentrancyLevel() != 0)
        JE_ASSERT(slow_path);

    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());

    AllocContext alloc_ctx;
    szind_t szind = sz::sizeToIndex(usize);
    if constexpr (!config::prof)
    {
        alloc_ctx.init(szind, (szind < SC_NBINS), usize);
    }
    else
    {
        if (JE_LIKELY(!profSampleAligned(ptr)))
        {
            /// When the ptr is not page aligned, it was not sampled. usize can be trusted to determine szind and slab.
            alloc_ctx.init(szind, (szind < SC_NBINS), usize);
        }
        else if (opt.prof)
        {
            /// Small sampled allocs promoted can still get correct usize here. Check comments in `Extent::usize`.
            arena_emap_global.allocCtxLookup(&tsd, ptr, &alloc_ctx);

            if constexpr (config::opt_safety_checks)
            {
                /// Small alloc may have !slab (sampled).
                size_t true_size = alloc_ctx.usizeGet();
                if (JE_UNLIKELY(alloc_ctx.szind != sz::sizeToIndex(usize)))
                    safetyCheckFailSizedDealloc(/* current_dealloc */ true, ptr, /* true_size */ true_size, /* input_size */ usize);
            }
        }
        else
        {
            alloc_ctx.init(szind, (szind < SC_NBINS), usize);
        }
    }
    bool fail = maybeCheckAllocCtx(tsd, ptr, &alloc_ctx);
    if (fail)
    {
        /// This is a heap corruption bug. In real life we'll crash; for the unit test we just want to avoid breaking
        /// anything too badly to get a test result out. Let's leak instead of trying to free.
        return;
    }

    if (config::prof && opt.prof)
        profFree(tsd, ptr, usize, &alloc_ctx);
    if (JE_LIKELY(!slow_path))
    {
        isdalloct(&tsd, ptr, usize, tcache, &alloc_ctx, false);
    }
    else
    {
        if (config::fill && slow_path && opt.junk_free)
            junkFreeCallback(ptr, usize);
        isdalloct(&tsd, ptr, usize, tcache, &alloc_ctx, true);
    }
    threadDallocEvent(tsd, usize);
}

/// jemalloc: irallocx_prof_sample
void * irallocxProfSample(
    ThreadState * tsdn,
    void * old_ptr,
    size_t old_usize,
    size_t usize,
    size_t alignment,
    bool zero,
    ThreadCache * tcache,
    Arena * arena,
    ProfThreadContext * tctx)
{
    void * p;

    if (tctx == nullptr)
        return nullptr;

    alignment = profSampleAlign(usize, alignment);
    /// If the allocation is small enough that it would normally be allocated on a slab, we need to take additional
    /// steps to ensure that it gets its own extent instead.
    if (sz::canUseSlab(usize))
    {
        size_t bumped_usize = sz::sa2u(usize, alignment);
        p = iralloctExplicitSlab(tsdn, old_ptr, old_usize, bumped_usize, alignment, zero, /* slab */ false, tcache, arena);
        if (p == nullptr)
            return nullptr;
        arenaProfPromote(tsdn, p, usize, bumped_usize);
    }
    else
    {
        p = iralloctExplicitSlab(tsdn, old_ptr, old_usize, usize, alignment, zero, /* slab */ false, tcache, arena);
    }
    JE_ASSERT(profSampleAligned(p));

    return p;
}

/// jemalloc: irallocx_prof
JE_ALWAYS_INLINE void * irallocxProf(
    ThreadState & tsd,
    void * old_ptr,
    size_t old_usize,
    size_t size,
    size_t alignment,
    size_t usize,
    bool zero,
    ThreadCache * tcache,
    Arena * arena,
    AllocContext * alloc_ctx)
{
    ProfInfo old_prof_info;
    profInfoGetAndResetRecent(tsd, old_ptr, alloc_ctx, &old_prof_info);
    bool prof_active = profActiveGetUnlocked();
    bool sample_event = teProfSampleEventLookahead(tsd, usize);
    ProfThreadContext * tctx = profAllocPrep(tsd, prof_active, sample_event);
    void * p;
    if (JE_UNLIKELY(tctx != PROF_TCTX_SENTINEL))
        p = irallocxProfSample(&tsd, old_ptr, old_usize, usize, alignment, zero, tcache, arena, tctx);
    else
        p = iralloct(&tsd, old_ptr, old_usize, size, alignment, usize, zero, tcache, arena);
    if (JE_UNLIKELY(p == nullptr))
    {
        profAllocRollback(tsd, tctx);
        return nullptr;
    }
    JE_ASSERT(usize == isalloc(&tsd, p));
    profRealloc(tsd, p, size, usize, tctx, prof_active, old_ptr, old_usize, &old_prof_info, sample_event);

    return p;
}

/// jemalloc: do_rallocx
void * doRallocx(void * ptr, size_t size, int flags, bool is_realloc)
{
    void * p;
    size_t usize;
    size_t old_usize;
    size_t alignment = mallocxAlignGet(flags);
    Arena * arena;

    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size != 0);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());
    ThreadState & tsd = ThreadState::fetch();

    bool zero = zeroGet(mallocxZeroGet(flags), /* slow */ true);

    unsigned arena_ind = mallocxArenaIndGet(flags);
    ThreadCache * tcache;
    AllocContext alloc_ctx;
    if (arenaGetFromInd(tsd, arena_ind, &arena))
        goto label_oom;

    tcache = tcacheGetFromInd(tsd, mallocxTcacheIndGet(flags), /* slow */ true, /* is_alloc */ true);

    arena_emap_global.allocCtxLookup(&tsd, ptr, &alloc_ctx);
    JE_ASSERT(alloc_ctx.szind != SC_NSIZES);
    old_usize = alloc_ctx.usizeGet();
    JE_ASSERT(old_usize == isalloc(&tsd, ptr));
    if (alignedUsizeGet(size, alignment, &usize, nullptr, false))
        goto label_oom;

    if (config::prof && opt.prof)
    {
        p = irallocxProf(tsd, ptr, old_usize, size, alignment, usize, zero, tcache, arena, &alloc_ctx);
        if (JE_UNLIKELY(p == nullptr))
            goto label_oom;
    }
    else
    {
        p = iralloct(&tsd, ptr, old_usize, size, alignment, usize, zero, tcache, arena);
        if (JE_UNLIKELY(p == nullptr))
            goto label_oom;
        JE_ASSERT(usize == isalloc(&tsd, p));
    }
    JE_ASSERT(alignment == 0 || (reinterpret_cast<uintptr_t>(p) & (alignment - 1)) == 0);
    threadAllocEvent(tsd, usize);
    threadDallocEvent(tsd, old_usize);

    if (config::fill && JE_UNLIKELY(opt.junk_alloc) && usize > old_usize && !zero)
    {
        size_t excess_len = usize - old_usize;
        void * excess_start = static_cast<char *>(p) + old_usize;
        junkAllocCallback(excess_start, excess_len);
    }

    return p;
label_oom:
    if (is_realloc)
        errno = ENOMEM;
    if (config_xmalloc && JE_UNLIKELY(opt.xmalloc))
    {
        writeMessage("<jemalloc>: Error in rallocx(): out of memory\n");
        abort();
    }

    return nullptr;
}

/// jemalloc: do_realloc_nonnull_zero
void * doReallocNonnullZero(void * ptr)
{
    if constexpr (config::stats)
        zero_realloc_count.fetch_add(1, std::memory_order_relaxed);
    if (opt.zero_realloc_action == ZeroReallocAction::Alloc)
    {
        /// The user might have gotten an alloc setting while expecting a free setting. If that's the case, we at least
        /// try to reduce the harm, and turn off the tcache while allocating, so that we'll get a true first fit.
        return doRallocx(ptr, 1, MALLOCX_TCACHE_NONE_FLAG, true);
    }
    else if (opt.zero_realloc_action == ZeroReallocAction::Free)
    {
        ThreadState & tsd = ThreadState::fetch();

        ThreadCache * tcache = tcacheGetFromInd(tsd, TCACHE_IND_AUTOMATIC, /* slow */ true, /* is_alloc */ false);
        ifree(tsd, ptr, tcache, true);

        return nullptr;
    }
    else
    {
        safetyCheckFail("Called realloc(non-null-ptr, 0) with zero_realloc:abort set\n");
        /// In real code, this will never run; the safety check failure will call abort. In the unit test, we just
        /// want to bail out without corrupting internal state that the test needs to finish.
        return nullptr;
    }
}

/// jemalloc: ixallocx_helper
JE_ALWAYS_INLINE size_t
ixallocxHelper(ThreadState * tsdn, void * ptr, size_t old_usize, size_t size, size_t extra, size_t alignment, bool zero)
{
    size_t newsize;

    if (ixalloc(tsdn, ptr, old_usize, size, extra, alignment, zero, &newsize))
        return old_usize;

    return newsize;
}

/// jemalloc: ixallocx_prof_sample
size_t ixallocxProfSample(
    ThreadState * tsdn, void * ptr, size_t old_usize, size_t size, size_t extra, size_t alignment, bool zero, ProfThreadContext * tctx)
{
    /// Sampled allocation needs to be page aligned.
    if (tctx == nullptr || !profSampleAligned(ptr))
        return old_usize;

    return ixallocxHelper(tsdn, ptr, old_usize, size, extra, alignment, zero);
}

/// jemalloc: ixallocx_prof
JE_ALWAYS_INLINE size_t ixallocxProf(
    ThreadState & tsd, void * ptr, size_t old_usize, size_t size, size_t extra, size_t alignment, bool zero, AllocContext * alloc_ctx)
{
    /// `old_prof_info` is only used for asserting that the profiling info isn't changed by the `ixalloc` call.
    ProfInfo old_prof_info;
    profInfoGet(tsd, ptr, alloc_ctx, &old_prof_info);

    /// usize isn't knowable before `ixalloc` returns when extra is non-zero. Therefore, compute its maximum possible
    /// value and use that in `profAllocPrep` to decide whether to capture a backtrace. `profRealloc` will use the
    /// actual usize to decide whether to sample.
    size_t usize_max;
    if (alignedUsizeGet(size + extra, alignment, &usize_max, nullptr, false))
    {
        /// usize_max is out of range, and chances are that allocation will fail, but use the maximum possible value
        /// and carry on with `profAllocPrep`, just in case allocation succeeds.
        usize_max = SC_LARGE_MAXCLASS;
    }
    bool prof_active = profActiveGetUnlocked();
    bool sample_event = teProfSampleEventLookahead(tsd, usize_max);
    ProfThreadContext * tctx = profAllocPrep(tsd, prof_active, sample_event);

    size_t usize;
    if (JE_UNLIKELY(tctx != PROF_TCTX_SENTINEL))
        usize = ixallocxProfSample(&tsd, ptr, old_usize, size, extra, alignment, zero, tctx);
    else
        usize = ixallocxHelper(&tsd, ptr, old_usize, size, extra, alignment, zero);

    /// At this point we can still safely get the original profiling information associated with the ptr, because
    /// (a) the `Extent` object associated with the ptr still lives and (b) the profiling info fields are not touched.
    /// "(a)" is asserted in the outer `je_xallocx` function, and "(b)" is indirectly verified below by checking that
    /// the alloc_tctx field is unchanged.
    ProfInfo prof_info;
    if (usize == old_usize)
    {
        profInfoGet(tsd, ptr, alloc_ctx, &prof_info);
        profAllocRollback(tsd, tctx);
    }
    else
    {
        /// Need to retrieve the new alloc_ctx since the modification to the extent has already been done.
        AllocContext new_alloc_ctx;
        arena_emap_global.allocCtxLookup(&tsd, ptr, &new_alloc_ctx);
        profInfoGetAndResetRecent(tsd, ptr, &new_alloc_ctx, &prof_info);
        JE_ASSERT(usize <= usize_max);
        sample_event = teProfSampleEventLookahead(tsd, usize);
        profRealloc(tsd, ptr, size, usize, tctx, prof_active, ptr, old_usize, &prof_info, sample_event);
    }

    JE_ASSERT(old_prof_info.alloc_tctx == prof_info.alloc_tctx);
    return usize;
}

/// jemalloc: inallocx
JE_ALWAYS_INLINE size_t inallocx(ThreadState * /*tsdn*/, size_t size, int flags)
{
    size_t usize;
    /// In case of out of range, let the user see it rather than fail.
    alignedUsizeGet(size, mallocxAlignGet(flags), &usize, nullptr, false);
    return usize;
}

/// jemalloc: je_malloc_usable_size_impl
JE_ALWAYS_INLINE size_t mallocUsableSizeImpl(const void * ptr)
{
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState * tsdn = ThreadState::tsdnFetch();

    size_t ret;
    if (JE_UNLIKELY(ptr == nullptr))
    {
        ret = 0;
    }
    else
    {
        /// jemalloc uses `ivsalloc` with `config_debug` (never in ClickHouse) or `force_ivsalloc` (never set).
        ret = isalloc(tsdn, ptr);
    }

    return ret;
}

}

/// This variant has logging hook on exit but not on entry. It's called only by `je_malloc`, which tail-calls it.
/// jemalloc: malloc_default
JE_NOINLINE void * mallocDefault(size_t size)
{
    void * ret;
    StaticOpts sopts;
    DynamicOpts dopts;

    sopts.null_out_result_on_error = true;
    sopts.set_errno_on_error = true;
    sopts.oom_string = "<jemalloc>: Error in malloc(): out of memory\n";

    dopts.result = &ret;
    dopts.num_items = 1;
    dopts.item_size = size;

    imalloc(sopts, dopts);

    return ret;
}

/// jemalloc: free_default
JE_NOINLINE void freeDefault(void * ptr)
{
    if (JE_LIKELY(ptr != nullptr))
    {
        /// We avoid setting up tsd fully (e.g. tcache, arena binding) based on only free() calls -- other activities
        /// trigger the minimal to full transition. This is because free() may happen during thread shutdown after tls
        /// deallocation: if a thread never had any malloc activities until then, a fully-setup tsd won't be destructed
        /// properly.
        ThreadState & tsd = ThreadState::fetchMin();

        if (JE_LIKELY(tsd.fast()))
        {
            ThreadCache * tcache = tcacheGetFromInd(tsd, TCACHE_IND_AUTOMATIC, /* slow */ false, /* is_alloc */ false);
            ifree(tsd, ptr, tcache, /* slow */ false);
        }
        else
        {
            ThreadCache * tcache = tcacheGetFromInd(tsd, TCACHE_IND_AUTOMATIC, /* slow */ true, /* is_alloc */ false);
            ifree(tsd, ptr, tcache, /* slow */ true);
        }
    }
}

/// jemalloc: sdallocx_default
JE_NOINLINE void sdallocxDefault(void * ptr, size_t size, int flags)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState & tsd = ThreadState::fetchMin();
    bool fast = tsd.fast();
    size_t usize = inallocx(&tsd, size, flags);

    unsigned tcache_ind = mallocxTcacheIndGet(flags);
    ThreadCache * tcache = tcacheGetFromInd(tsd, tcache_ind, !fast, /* is_alloc */ false);

    if (JE_LIKELY(fast))
    {
        tsd.assertFast();
        isfree(tsd, ptr, usize, tcache, false);
    }
    else
    {
        isfree(tsd, ptr, usize, tcache, true);
    }
}

/// The constructor is here (not in Init.cpp) so that the unit tests, which link only the internals, do not
/// initialize the global allocator state.
namespace
{

/// If an application creates a thread before doing any allocation in the main thread, then calls fork(2) in the main
/// thread followed by memory allocation in the child process, a race can occur that results in deadlock within the
/// child: the main thread may have forked while the created thread had partially initialized the allocator.
/// Ordinarily jemalloc prevents fork/malloc races via the functions it registers during initialization using
/// `pthread_atfork`, but of course that does no good if the allocator isn't fully initialized at fork time. This
/// library constructor is a partial solution to this problem.
/// jemalloc: jemalloc_constructor
__attribute__((constructor)) void jemallocConstructor()
{
    mallocInit();
}

}

}

using namespace jemalloc;

/// --- malloc(3)-compatible functions ---------------------------------------------------------------------------------

/// jemalloc: je_malloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_malloc(size_t size) JEMALLOC_CXX_THROW
{
    return imallocFastpath<&mallocDefault>(size);
}

/// jemalloc: je_posix_memalign
JEMALLOC_EXPORT int JEMALLOC_SYS_NOTHROW je_posix_memalign(void ** memptr, size_t alignment, size_t size) JEMALLOC_CXX_THROW
{
    StaticOpts sopts;
    DynamicOpts dopts;

    sopts.bump_empty_aligned_alloc = true;
    sopts.min_alignment = sizeof(void *);
    sopts.oom_string = "<jemalloc>: Error allocating aligned memory: out of memory\n";
    sopts.invalid_alignment_string = "<jemalloc>: Error allocating aligned memory: invalid alignment\n";

    dopts.result = memptr;
    dopts.num_items = 1;
    dopts.item_size = size;
    dopts.alignment = alignment;

    return imalloc(sopts, dopts);
}

/// jemalloc: je_aligned_alloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_aligned_alloc(size_t alignment, size_t size) JEMALLOC_CXX_THROW
{
    void * ret;

    StaticOpts sopts;
    DynamicOpts dopts;

    sopts.bump_empty_aligned_alloc = true;
    sopts.null_out_result_on_error = true;
    sopts.set_errno_on_error = true;
    sopts.min_alignment = 1;
    sopts.oom_string = "<jemalloc>: Error allocating aligned memory: out of memory\n";
    sopts.invalid_alignment_string = "<jemalloc>: Error allocating aligned memory: invalid alignment\n";

    dopts.result = &ret;
    dopts.num_items = 1;
    dopts.item_size = size;
    dopts.alignment = alignment;

    imalloc(sopts, dopts);

    return ret;
}

/// jemalloc: je_calloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_calloc(size_t num, size_t size) JEMALLOC_CXX_THROW
{
    void * ret;
    StaticOpts sopts;
    DynamicOpts dopts;

    sopts.may_overflow = true;
    sopts.null_out_result_on_error = true;
    sopts.set_errno_on_error = true;
    sopts.oom_string = "<jemalloc>: Error in calloc(): out of memory\n";

    dopts.result = &ret;
    dopts.num_items = num;
    dopts.item_size = size;
    dopts.zero = true;

    imalloc(sopts, dopts);

    return ret;
}

/// jemalloc: je_free
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW je_free(void * ptr) JEMALLOC_CXX_THROW
{
    freeImpl(ptr);
}

/// Not declared in the public header (unused by ClickHouse), but exported like in jemalloc.
/// jemalloc: je_free_sized
extern "C" JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_free_sized(void * ptr, size_t size)
{
    sdallocxNoflags(ptr, size);
}

/// jemalloc: je_free_aligned_sized
extern "C" JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_free_aligned_sized(void * ptr, size_t alignment, size_t size)
{
    return je_sdallocx(ptr, size, /* flags */ MALLOCX_ALIGN(alignment));
}

/// --- Non-standard functions -----------------------------------------------------------------------------------------

/// jemalloc: je_mallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW * je_mallocx(size_t size, int flags)
{
    return mallocx(size, flags);
}

/// jemalloc: je_rallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW * je_rallocx(void * ptr, size_t size, int flags)
{
    return doRallocx(ptr, size, flags, false);
}

/// jemalloc: je_realloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_realloc(void * ptr, size_t size) JEMALLOC_CXX_THROW
{
    if (JE_LIKELY(ptr != nullptr && size != 0))
    {
        return doRallocx(ptr, size, 0, true);
    }
    else if (ptr != nullptr && size == 0)
    {
        return doReallocNonnullZero(ptr);
    }
    else
    {
        /// realloc(NULL, size) is equivalent to malloc(size).
        void * ret;

        StaticOpts sopts;
        DynamicOpts dopts;

        sopts.null_out_result_on_error = true;
        sopts.set_errno_on_error = true;
        sopts.oom_string = "<jemalloc>: Error in realloc(): out of memory\n";

        dopts.result = &ret;
        dopts.num_items = 1;
        dopts.item_size = size;

        imalloc(sopts, dopts);
        return ret;
    }
}

/// jemalloc: je_xallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_xallocx(void * ptr, size_t size, size_t extra, int flags)
{
    size_t usize;
    size_t old_usize;
    size_t alignment = mallocxAlignGet(flags);
    bool zero = zeroGet(mallocxZeroGet(flags), /* slow */ true);

    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(size != 0);
    JE_ASSERT(SIZE_MAX - size >= extra);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());
    ThreadState & tsd = ThreadState::fetch();

    /// `old_edata` is only for verifying that xallocx() keeps the `Extent` object associated with the ptr (though the
    /// content of the object can be changed).
    [[maybe_unused]] Extent * old_edata = config::debug ? arena_emap_global.edataLookup(&tsd, ptr) : nullptr;

    AllocContext alloc_ctx;
    arena_emap_global.allocCtxLookup(&tsd, ptr, &alloc_ctx);
    JE_ASSERT(alloc_ctx.szind != SC_NSIZES);
    old_usize = alloc_ctx.usizeGet();
    JE_ASSERT(old_usize == isalloc(&tsd, ptr));
    /// The API explicitly absolves itself of protecting against (size + extra) numerical overflow, but we may need to
    /// clamp extra to avoid exceeding SC_LARGE_MAXCLASS.
    ///
    /// Ordinarily, size limit checking is handled deeper down, but here we have to check as part of (size + extra)
    /// clamping, since we need the clamped value in the above helper functions.
    if (JE_UNLIKELY(size > SC_LARGE_MAXCLASS))
    {
        usize = old_usize;
        goto label_not_resized;
    }
    if (JE_UNLIKELY(SC_LARGE_MAXCLASS - size < extra))
        extra = SC_LARGE_MAXCLASS - size;

    if (config::prof && opt.prof)
        usize = ixallocxProf(tsd, ptr, old_usize, size, extra, alignment, zero, &alloc_ctx);
    else
        usize = ixallocxHelper(&tsd, ptr, old_usize, size, extra, alignment, zero);

    /// xallocx() should keep using the same `Extent` object (though its content can be changed).
    JE_ASSERT(!config::debug || arena_emap_global.edataLookup(&tsd, ptr) == old_edata);

    if (JE_UNLIKELY(usize == old_usize))
        goto label_not_resized;
    threadAllocEvent(tsd, usize);
    threadDallocEvent(tsd, old_usize);

    if (config::fill && JE_UNLIKELY(opt.junk_alloc) && usize > old_usize && !zero)
    {
        size_t excess_len = usize - old_usize;
        void * excess_start = static_cast<char *>(ptr) + old_usize;
        junkAllocCallback(excess_start, excess_len);
    }
label_not_resized:
    return usize;
}

/// jemalloc: je_sallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_sallocx(const void * ptr, int /*flags*/)
{
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());
    JE_ASSERT(ptr != nullptr);

    ThreadState * tsdn = ThreadState::tsdnFetch();

    /// jemalloc uses `ivsalloc` with `config_debug` (never in ClickHouse) or `force_ivsalloc` (never set).
    return isalloc(tsdn, ptr);
}

/// jemalloc: je_dallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_dallocx(void * ptr, int flags)
{
    JE_ASSERT(ptr != nullptr);
    JE_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState & tsd = ThreadState::fetchMin();
    bool fast = tsd.fast();

    unsigned tcache_ind = mallocxTcacheIndGet(flags);
    ThreadCache * tcache = tcacheGetFromInd(tsd, tcache_ind, !fast, /* is_alloc */ false);

    if (JE_LIKELY(fast))
    {
        tsd.assertFast();
        ifree(tsd, ptr, tcache, false);
    }
    else
    {
        ifree(tsd, ptr, tcache, true);
    }
}

/// jemalloc: je_sdallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_sdallocx(void * ptr, size_t size, int flags)
{
    sdallocxImpl(ptr, size, flags);
}

/// jemalloc: je_nallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_nallocx(size_t size, int flags)
{
    JE_ASSERT(size != 0);

    if (JE_UNLIKELY(mallocInit()))
        return 0;

    ThreadState * tsdn = ThreadState::tsdnFetch();

    size_t usize = inallocx(tsdn, size, flags);
    if (JE_UNLIKELY(usize > SC_LARGE_MAXCLASS))
        return 0;

    return usize;
}

/// jemalloc: je_mallctl
JEMALLOC_EXPORT int JEMALLOC_NOTHROW je_mallctl(const char * name, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & tsd = ThreadState::fetch();
    return ctlByName(tsd, name, oldp, oldlenp, newp, newlen);
}

/// jemalloc: je_mallctlnametomib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW je_mallctlnametomib(const char * name, size_t * mibp, size_t * miblenp)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & tsd = ThreadState::fetch();
    return ctlNameToMib(tsd, name, mibp, miblenp);
}

/// jemalloc: je_mallctlbymib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW
je_mallctlbymib(const size_t * mib, size_t miblen, void * oldp, size_t * oldlenp, void * newp, size_t newlen)
{
    if (JE_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & tsd = ThreadState::fetch();
    return ctlByMib(tsd, mib, miblen, oldp, oldlenp, newp, newlen);
}

/// NB: does not initialize the allocator (like jemalloc).
/// jemalloc: je_malloc_stats_print
JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_malloc_stats_print(void (*write_cb)(void *, const char *), void * cbopaque, const char * opts)
{
    mallocStatsPrint(write_cb, cbopaque, opts);
}

/// jemalloc: je_malloc_usable_size
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_malloc_usable_size(JEMALLOC_USABLE_SIZE_CONST void * ptr) JEMALLOC_CXX_THROW
{
    return mallocUsableSizeImpl(ptr);
}

#if defined(__APPLE__)
static_assert(config::have_malloc_size);
/// Not declared by the public headers: `JEMALLOC_HAVE_MALLOC_SIZE` is an internal define in jemalloc.
extern "C" JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_malloc_size(const void * ptr);

/// jemalloc: je_malloc_size
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_malloc_size(const void * ptr)
{
    return mallocUsableSizeImpl(ptr);
}
#endif
