#pragma once

/// The allocation front-end of the "malloc(3)-compatible functions" and "non-standard functions" (jemalloc: the
/// `imalloc` machinery of `src/jemalloc.c`). Header-inline so that `je_malloc`, `je_mallocx`, ... in Api.cpp compile
/// to the same code as before, while `batchAlloc` (in the core library, used by `experimental.batch_alloc`) can call
/// `mallocx` like jemalloc's `batch_alloc` calls `je_mallocx`.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/Common.h>
#include <allocator/ExtentMap.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Init.h>
#include <allocator/Options.h>
#include <allocator/ProfHooks.h>
#include <allocator/Sanitizer.h>
#include <allocator/SizeClasses.h>
#include <allocator/ThreadCache.h>
#include <allocator/ThreadEvent.h>
#include <allocator/ThreadState.h>

#include <cerrno>
#include <cstdint>
#include <cstdlib>

namespace jemalloc
{

/// `JEMALLOC_XMALLOC` is not configured. jemalloc: config_xmalloc
constexpr bool config_xmalloc = false;

/// Settings determined by the documented behavior of the allocation functions.
/// jemalloc: static_opts_t, static_opts_init
struct StaticOpts
{
    /// Whether or not allocation size may overflow.
    bool may_overflow = false;
    /// Whether or not allocations (with alignment) of size 0 should be treated as size 1.
    bool bump_empty_aligned_alloc = false;
    /// Whether to assert that allocations are not of size 0 (after any bumping).
    bool assert_nonempty_alloc = false;
    /// Whether or not to modify the 'result' argument to malloc in case of error.
    bool null_out_result_on_error = false;
    /// Whether to set errno when we encounter an error condition.
    bool set_errno_on_error = false;
    /// The minimum valid alignment for functions requesting aligned storage.
    size_t min_alignment = 0;
    /// The error string to use if we oom.
    const char * oom_string = "";
    /// The error string to use if the passed-in alignment is invalid.
    const char * invalid_alignment_string = "";
    /// False if we're configured to skip some time-consuming operations. This isn't really a malloc "behavior", but
    /// it acts as a useful summary of several other static (or at least, static after program initialization)
    /// options.
    bool slow = false;
    /// Return size.
    bool usize = false;
};

/// jemalloc: dynamic_opts_t, dynamic_opts_init
struct DynamicOpts
{
    void ** result = nullptr;
    size_t usize = 0;
    size_t num_items = 0;
    size_t item_size = 0;
    size_t alignment = 0;
    bool zero = false;
    unsigned tcache_ind = TCACHE_IND_AUTOMATIC;
    unsigned arena_ind = ARENA_IND_AUTOMATIC;
};

/// `ind` is optional and is only checked and filled if `alignment == 0`. Returns true if the result is out of range.
/// jemalloc: aligned_usize_get
JE_ALWAYS_INLINE bool alignedUsizeGet(size_t size, size_t alignment, size_t * usize, szind_t * ind, bool bump_empty_aligned_alloc)
{
    JE_ASSERT(usize != nullptr);
    if (alignment == 0)
    {
        if (ind != nullptr)
        {
            *ind = sz::sizeToIndex(size);
            if (JE_UNLIKELY(*ind >= SC_NSIZES))
                return true;
            *usize = sz::largeSizeClassesDisabled() ? sz::s2u(size) : sz::indexToSize(*ind);
            JE_ASSERT(*usize > 0 && *usize <= SC_LARGE_MAXCLASS);
            return false;
        }
        *usize = sz::s2u(size);
    }
    else
    {
        if (bump_empty_aligned_alloc && JE_UNLIKELY(size == 0))
            size = 1;
        *usize = sz::sa2u(size, alignment);
    }
    if (JE_UNLIKELY(*usize == 0 || *usize > SC_LARGE_MAXCLASS))
        return true;
    return false;
}

/// jemalloc: zero_get
JE_ALWAYS_INLINE bool zeroGet(bool guarantee, bool slow)
{
    if (config::fill && slow && JE_UNLIKELY(opt.zero))
        return true;
    return guarantee;
}

/// Returns true if a manual arena is specified and `arenaGet` OOMs.
/// jemalloc: arena_get_from_ind
JE_ALWAYS_INLINE bool arenaGetFromInd(ThreadState & tsd, unsigned arena_ind, Arena ** arena_p)
{
    if (arena_ind == ARENA_IND_AUTOMATIC)
    {
        /// In case of automatic arena management, we defer arena computation until as late as we can, hoping to fill
        /// the allocation out of the tcache.
        *arena_p = nullptr;
    }
    else
    {
        *arena_p = arenaGet(&tsd, arena_ind, true);
        if (JE_UNLIKELY(*arena_p == nullptr) && arena_ind >= narenas_auto)
            return true;
    }
    return false;
}

/// `ind` is ignored if `dopts.alignment > 0`.
/// jemalloc: imalloc_no_sample
JE_ALWAYS_INLINE void *
imallocNoSample(StaticOpts & sopts, DynamicOpts & dopts, ThreadState & tsd, size_t size, size_t usize, szind_t ind, bool slab)
{
    /// Fill in the tcache.
    ThreadCache * tcache = tcacheGetFromInd(tsd, dopts.tcache_ind, sopts.slow, /* is_alloc */ true);

    /// Fill in the arena.
    Arena * arena;
    if (arenaGetFromInd(tsd, dopts.arena_ind, &arena))
        return nullptr;

    if (JE_UNLIKELY(dopts.alignment != 0))
        return ipalloctExplicitSlab(&tsd, usize, dopts.alignment, dopts.zero, slab, tcache, arena);

    return iallocztmExplicitSlab(&tsd, size, ind, dopts.zero, slab, tcache, false, arena, sopts.slow);
}

/// jemalloc: imalloc_sample
JE_ALWAYS_INLINE void * imallocSample(StaticOpts & sopts, DynamicOpts & dopts, ThreadState & tsd, size_t usize, szind_t ind)
{
    void * ret;

    dopts.alignment = profSampleAlign(usize, dopts.alignment);
    /// If the allocation is small enough that it would normally be allocated on a slab, we need to take additional
    /// steps to ensure that it gets its own extent instead.
    if (sz::canUseSlab(usize))
    {
        JE_ASSERT((dopts.alignment & PROF_SAMPLE_ALIGNMENT_MASK) == 0);
        size_t bumped_usize = sz::sa2u(usize, dopts.alignment);
        szind_t bumped_ind = sz::sizeToIndex(bumped_usize);
        dopts.tcache_ind = TCACHE_IND_NONE;
        ret = imallocNoSample(sopts, dopts, tsd, bumped_usize, bumped_usize, bumped_ind, /* slab */ false);
        if (JE_UNLIKELY(ret == nullptr))
            return nullptr;
        arenaProfPromote(&tsd, ret, usize, bumped_usize);
    }
    else
    {
        ret = imallocNoSample(sopts, dopts, tsd, usize, usize, ind, /* slab */ false);
    }
    JE_ASSERT(profSampleAligned(ret));

    return ret;
}

/// Returns true if the allocation will overflow, and false otherwise. Sets `*size` to the product either way.
/// jemalloc: compute_size_with_overflow
JE_ALWAYS_INLINE bool computeSizeWithOverflow(bool may_overflow, DynamicOpts & dopts, size_t * size)
{
    /// This function is just num_items * item_size, except that we may have to check for overflow.

    if (!may_overflow)
    {
        JE_ASSERT(dopts.num_items == 1);
        *size = dopts.item_size;
        return false;
    }

    /// A size_t with its high-half bits all set to 1.
    constexpr size_t high_bits = SIZE_MAX << (sizeof(size_t) * 8 / 2);

    *size = dopts.item_size * dopts.num_items;

    if (JE_UNLIKELY(*size == 0))
        return (dopts.num_items != 0 && dopts.item_size != 0);

    /// We got a non-zero size, but we don't know if we overflowed to get there. To avoid having to do a divide, we'll
    /// be clever and note that if both A and B can be represented in N/2 bits, then their product can be represented
    /// in N bits (without the possibility of overflow).
    if (JE_LIKELY((high_bits & (dopts.num_items | dopts.item_size)) == 0))
        return false;
    if (JE_LIKELY(*size / dopts.item_size == dopts.num_items))
        return false;
    return true;
}

/// Returns the errno-style error code of the allocation.
/// jemalloc: imalloc_body
JE_ALWAYS_INLINE int imallocBody(StaticOpts & sopts, DynamicOpts & dopts, ThreadState & tsd)
{
    /// Where the actual allocation memory will live.
    void * allocation = nullptr;
    /// Filled in by `computeSizeWithOverflow` below.
    size_t size = 0;
    /// The zero initialization for ind is actually a dead store, in that its value is reset before any branch on its
    /// value is taken.
    szind_t ind = 0;
    /// usize will always be properly initialized.
    size_t usize;

    /// Reentrancy is only checked on slow path.
    int8_t reentrancy_level;

    /// Compute the amount of memory the user wants.
    if (JE_UNLIKELY(computeSizeWithOverflow(sopts.may_overflow, dopts, &size)))
        goto label_oom;

    if (JE_UNLIKELY(dopts.alignment < sopts.min_alignment || (dopts.alignment & (dopts.alignment - 1)) != 0))
        goto label_invalid_alignment;

    /// This is the beginning of the "core" algorithm.
    dopts.zero = zeroGet(dopts.zero, sopts.slow);
    if (alignedUsizeGet(size, dopts.alignment, &usize, &ind, sopts.bump_empty_aligned_alloc))
        goto label_oom;
    dopts.usize = usize;
    /// Validate the user input.
    if (sopts.assert_nonempty_alloc)
        JE_ASSERT(size != 0);

    /// If we need to handle reentrancy, we can do it out of a known-initialized arena (i.e. arena 0).
    reentrancy_level = tsd.reentrancyLevel();
    if (sopts.slow && JE_UNLIKELY(reentrancy_level > 0))
    {
        /// We should never specify particular arenas or tcaches from within our internal allocations.
        JE_ASSERT(dopts.tcache_ind == TCACHE_IND_AUTOMATIC || dopts.tcache_ind == TCACHE_IND_NONE);
        JE_ASSERT(dopts.arena_ind == ARENA_IND_AUTOMATIC);
        dopts.tcache_ind = TCACHE_IND_NONE;
        /// We know that arena 0 has already been initialized.
        dopts.arena_ind = 0;
    }

    /// If `dopts.alignment > 0`, then ind is still 0, but usize was computed in the previous if statement. Down the
    /// positive alignment path, `imallocNoSample` and `imallocSample` will ignore ind.

    /// If profiling is on, get our profiling context.
    if (config::prof && opt.prof)
    {
        bool prof_active = profActiveGetUnlocked();
        bool sample_event = teProfSampleEventLookahead(tsd, usize);
        ProfThreadContext * tctx = profAllocPrep(tsd, prof_active, sample_event);

        AllocContext alloc_ctx;
        if (JE_LIKELY(tctx == PROF_TCTX_SENTINEL))
        {
            alloc_ctx.slab = sz::canUseSlab(usize);
            allocation = imallocNoSample(sopts, dopts, tsd, usize, usize, ind, alloc_ctx.slab);
        }
        else if (tctx != nullptr)
        {
            allocation = imallocSample(sopts, dopts, tsd, usize, ind);
            alloc_ctx.slab = false;
        }
        else
        {
            allocation = nullptr;
        }

        if (JE_UNLIKELY(allocation == nullptr))
        {
            profAllocRollback(tsd, tctx);
            goto label_oom;
        }
        profMalloc(tsd, allocation, size, usize, &alloc_ctx, tctx);
    }
    else
    {
        JE_ASSERT(!opt.prof);
        allocation = imallocNoSample(sopts, dopts, tsd, size, usize, ind, sz::canUseSlab(usize));
        if (JE_UNLIKELY(allocation == nullptr))
            goto label_oom;
    }

    /// Allocation has been done at this point. We still have some post-allocation work to do though.

    threadAllocEvent(tsd, usize);

    JE_ASSERT(dopts.alignment == 0 || (reinterpret_cast<uintptr_t>(allocation) & (dopts.alignment - 1)) == 0);

    JE_ASSERT(usize == isalloc(&tsd, allocation));

    if (config::fill && sopts.slow && !dopts.zero && JE_UNLIKELY(opt.junk_alloc))
        junkAllocCallback(allocation, usize);

    /// Success!
    *dopts.result = allocation;
    return 0;

label_oom:
    if (JE_UNLIKELY(sopts.slow) && config_xmalloc && JE_UNLIKELY(opt.xmalloc))
    {
        writeMessage(sopts.oom_string);
        abort();
    }

    if (sopts.set_errno_on_error)
        errno = ENOMEM;

    if (sopts.null_out_result_on_error)
        *dopts.result = nullptr;

    return ENOMEM;

    /// This label is only jumped to by one goto; we move it out of line anyways to avoid obscuring the non-error paths,
    /// and for symmetry with the oom case.
label_invalid_alignment:
    if (config_xmalloc && JE_UNLIKELY(opt.xmalloc))
    {
        writeMessage(sopts.invalid_alignment_string);
        abort();
    }

    if (sopts.set_errno_on_error)
        errno = EINVAL;

    if (sopts.null_out_result_on_error)
        *dopts.result = nullptr;

    return EINVAL;
}

/// jemalloc: imalloc_init_check
JE_ALWAYS_INLINE bool imallocInitCheck(StaticOpts & sopts, DynamicOpts & dopts)
{
    if (JE_UNLIKELY(!mallocInitialized()) && JE_UNLIKELY(mallocInit()))
    {
        if (config_xmalloc && JE_UNLIKELY(opt.xmalloc))
        {
            writeMessage(sopts.oom_string);
            abort();
        }
        errno = ENOMEM;
        *dopts.result = nullptr;

        return false;
    }

    return true;
}

/// Returns the errno-style error code of the allocation.
/// jemalloc: imalloc
JE_ALWAYS_INLINE int imalloc(StaticOpts & sopts, DynamicOpts & dopts)
{
    if (Tsd::get_allocates && !imallocInitCheck(sopts, dopts))
        return ENOMEM;

    /// We always need the tsd. Let's grab it right away.
    ThreadState & tsd = ThreadState::fetch();
    if (JE_LIKELY(tsd.fast()))
    {
        /// Fast and common path.
        tsd.assertFast();
        sopts.slow = false;
        return imallocBody(sopts, dopts, tsd);
    }
    else
    {
        if (!Tsd::get_allocates && !imallocInitCheck(sopts, dopts))
            return ENOMEM;

        sopts.slow = true;
        return imallocBody(sopts, dopts, tsd);
    }
}

/// The body of `je_mallocx`. jemalloc: je_mallocx
JE_ALWAYS_INLINE void * mallocx(size_t size, int flags)
{
    void * ret;
    StaticOpts sopts;
    DynamicOpts dopts;

    sopts.assert_nonempty_alloc = true;
    sopts.null_out_result_on_error = true;
    sopts.oom_string = "<jemalloc>: Error in mallocx(): out of memory\n";

    dopts.result = &ret;
    dopts.num_items = 1;
    dopts.item_size = size;
    if (JE_UNLIKELY(flags != 0))
    {
        dopts.alignment = mallocxAlignGet(flags);
        dopts.zero = mallocxZeroGet(flags);
        dopts.tcache_ind = mallocxTcacheIndGet(flags);
        dopts.arena_ind = mallocxArenaIndGet(flags);
    }

    imalloc(sopts, dopts);

    return ret;
}

/// Allocates up to `num` objects of `size` with `flags` (as `mallocx`) into `ptrs`; returns the number allocated.
/// Defined in BatchAlloc.cpp. jemalloc: batch_alloc
size_t batchAlloc(void ** ptrs, size_t num, size_t size, int flags);

}
