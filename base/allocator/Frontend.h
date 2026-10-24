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
#include <allocator/ProfilingHooks.h>
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
ALLOCATOR_ALWAYS_INLINE bool mallocInitialized()
{
    return malloc_init_state == malloc_init_initialized;
}

/// Initializes the allocator (Init.cpp). Returns true on error. jemalloc: malloc_init_hard
bool mallocInitHard();

/// jemalloc: malloc_init
ALLOCATOR_ALWAYS_INLINE bool mallocInit()
{
    if (ALLOCATOR_UNLIKELY(!mallocInitialized()) && mallocInitHard())
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
ALLOCATOR_ALWAYS_INLINE void junkAllocCallback(void * ptr, size_t usable_size)
{
    memset(ptr, junk_alloc_byte, usable_size);
}

/// jemalloc: junk_free_callback = default_junk_free
ALLOCATOR_ALWAYS_INLINE void junkFreeCallback(void * ptr, size_t usable_size)
{
    memset(ptr, junk_free_byte, usable_size);
}

/// --- Sentinels of the tcache / arena indices (jemalloc_internal_inlines_c.h) ------------------------------------

/// These correspond to the macros in jemalloc_macros.h (the representations need not be related).
/// jemalloc: TCACHE_IND_NONE, TCACHE_IND_AUTOMATIC, ARENA_IND_AUTOMATIC
inline constexpr unsigned THREAD_CACHE_IDX_NONE = unsigned(-1);
inline constexpr unsigned THREAD_CACHE_IDX_AUTOMATIC = unsigned(-2);
inline constexpr unsigned ARENA_IDX_AUTOMATIC = unsigned(-1);

/// jemalloc: mallocx_tcache_get
ALLOCATOR_ALWAYS_INLINE unsigned threadCacheIdxFromFlags(int flags)
{
    if (ALLOCATOR_LIKELY((flags & MALLOCX_THREAD_CACHE_MASK) == 0))
        return THREAD_CACHE_IDX_AUTOMATIC;
    else if ((flags & MALLOCX_THREAD_CACHE_MASK) == MALLOCX_THREAD_CACHE_NONE_FLAG)
        return THREAD_CACHE_IDX_NONE;
    else
        return threadCacheFromFlags(flags);
}

/// jemalloc: mallocx_arena_get
ALLOCATOR_ALWAYS_INLINE unsigned arenaIdxFromFlags(int flags)
{
    if (ALLOCATOR_UNLIKELY((flags & MALLOCX_ARENA_MASK) != 0))
        return arenaFromFlags(flags);
    else
        return ARENA_IDX_AUTOMATIC;
}

/// --- The `i` functions (jemalloc_internal_inlines_c.h) --------------------------------------------------------------

/// jemalloc: iaalloc
ALLOCATOR_ALWAYS_INLINE Arena * allocationArena(ThreadState * thread_state, const void * ptr)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    return arenaOfPointer(thread_state, ptr);
}

/// jemalloc: isalloc
ALLOCATOR_ALWAYS_INLINE size_t allocationSize(ThreadState * thread_state, const void * ptr)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    return arenaAllocationSize(thread_state, ptr);
}

/// jemalloc: iallocztm_explicit_slab
ALLOCATOR_ALWAYS_INLINE void * internalAllocateFullExplicitSlab(
    ThreadState * thread_state,
    size_t size,
    SizeClassIdx idx,
    bool zero,
    bool slab,
    ThreadCache * thread_cache,
    bool is_internal,
    Arena * arena,
    bool slow_path)
{
    ALLOCATOR_ASSERT(!slab || size_classes::canUseSlab(size)); /// slab && large is illegal
    ALLOCATOR_ASSERT(!is_internal || thread_cache == nullptr);
    ALLOCATOR_ASSERT(!is_internal || arena == nullptr || arenaIsAuto(arena));

    void * result = arenaMalloc(thread_state, arena, size, idx, zero, slab, thread_cache, slow_path);
    if (config::stats && is_internal && ALLOCATOR_LIKELY(result != nullptr))
        arenaInternalAdd(allocationArena(thread_state, result), allocationSize(thread_state, result));
    return result;
}

/// jemalloc: iallocztm
ALLOCATOR_ALWAYS_INLINE void * internalAllocateFull(
    ThreadState * thread_state,
    size_t size,
    SizeClassIdx idx,
    bool zero,
    ThreadCache * thread_cache,
    bool is_internal,
    Arena * arena,
    bool slow_path)
{
    bool slab = size_classes::canUseSlab(size);
    return internalAllocateFullExplicitSlab(thread_state, size, idx, zero, slab, thread_cache, is_internal, arena, slow_path);
}

/// jemalloc: ialloc
ALLOCATOR_ALWAYS_INLINE void * internalAllocate(ThreadState & thread_state, size_t size, SizeClassIdx idx, bool zero, bool slow_path)
{
    return internalAllocateFull(&thread_state, size, idx, zero, threadCacheGet(thread_state), false, nullptr, slow_path);
}

/// jemalloc: ipallocztm_explicit_slab
ALLOCATOR_ALWAYS_INLINE void * internalAllocateAlignedFullExplicitSlab(
    ThreadState * thread_state,
    size_t usable_size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * thread_cache,
    bool is_internal,
    Arena * arena)
{
    ALLOCATOR_ASSERT(!slab || size_classes::canUseSlab(usable_size)); /// slab && large is illegal
    ALLOCATOR_ASSERT(usable_size != 0);
    ALLOCATOR_ASSERT(usable_size == size_classes::alignedSizeToUsableSize(usable_size, alignment));
    ALLOCATOR_ASSERT(!is_internal || thread_cache == nullptr);
    ALLOCATOR_ASSERT(!is_internal || arena == nullptr || arenaIsAuto(arena));

    void * result = arenaAllocateAligned(thread_state, arena, usable_size, alignment, zero, slab, thread_cache);
    ALLOCATOR_ASSERT(alignmentAddrToBase(result, alignment) == result);
    if (config::stats && is_internal && ALLOCATOR_LIKELY(result != nullptr))
        arenaInternalAdd(allocationArena(thread_state, result), allocationSize(thread_state, result));
    return result;
}

/// jemalloc: ipallocztm
ALLOCATOR_ALWAYS_INLINE void * internalAllocateAlignedFull(
    ThreadState * thread_state,
    size_t usable_size,
    size_t alignment,
    bool zero,
    ThreadCache * thread_cache,
    bool is_internal,
    Arena * arena)
{
    return internalAllocateAlignedFullExplicitSlab(
        thread_state, usable_size, alignment, zero, size_classes::canUseSlab(usable_size), thread_cache, is_internal, arena);
}

/// jemalloc: ipalloct
ALLOCATOR_ALWAYS_INLINE void * internalAllocateAlignedWithCache(
    ThreadState * thread_state, size_t usable_size, size_t alignment, bool zero, ThreadCache * thread_cache, Arena * arena)
{
    return internalAllocateAlignedFull(thread_state, usable_size, alignment, zero, thread_cache, false, arena);
}

/// jemalloc: ipalloct_explicit_slab
ALLOCATOR_ALWAYS_INLINE void * internalAllocateAlignedWithCacheExplicitSlab(
    ThreadState * thread_state, size_t usable_size, size_t alignment, bool zero, bool slab, ThreadCache * thread_cache, Arena * arena)
{
    return internalAllocateAlignedFullExplicitSlab(thread_state, usable_size, alignment, zero, slab, thread_cache, false, arena);
}

/// jemalloc: ipalloc
ALLOCATOR_ALWAYS_INLINE void * internalAllocateAligned(ThreadState & thread_state, size_t usable_size, size_t alignment, bool zero)
{
    return internalAllocateAlignedFull(&thread_state, usable_size, alignment, zero, threadCacheGet(thread_state), false, nullptr);
}

/// jemalloc: ivsalloc
ALLOCATOR_ALWAYS_INLINE size_t allocationSizeIfOwned(ThreadState * thread_state, const void * ptr)
{
    return arenaAllocationSizeIfOwned(thread_state, ptr);
}

/// jemalloc: idalloctm
ALLOCATOR_ALWAYS_INLINE void internalDeallocateFull(
    ThreadState * thread_state, void * ptr, ThreadCache * thread_cache, AllocContext * alloc_context, bool is_internal, bool slow_path)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(!is_internal || thread_cache == nullptr);
    ALLOCATOR_ASSERT(!is_internal || arenaIsAuto(allocationArena(thread_state, ptr)));
    if (config::stats && is_internal)
        arenaInternalSub(allocationArena(thread_state, ptr), allocationSize(thread_state, ptr));
    if (!is_internal && thread_state != nullptr && thread_state->reentrancyLevel() != 0)
        ALLOCATOR_ASSERT(thread_cache == nullptr);
    arenaDeallocate(thread_state, ptr, thread_cache, alloc_context, slow_path);
}

/// jemalloc: idalloc
ALLOCATOR_ALWAYS_INLINE void internalDeallocate(ThreadState & thread_state, void * ptr)
{
    internalDeallocateFull(&thread_state, ptr, threadCacheGet(thread_state), nullptr, false, true);
}

/// jemalloc: isdalloct
ALLOCATOR_ALWAYS_INLINE void internalSizedDeallocate(
    ThreadState * thread_state, void * ptr, size_t size, ThreadCache * thread_cache, AllocContext * alloc_context, bool slow_path)
{
    arenaSizedDeallocate(thread_state, ptr, size, thread_cache, alloc_context, slow_path);
}

/// jemalloc: iralloct_realign
ALLOCATOR_ALWAYS_INLINE void * internalReallocateRealign(
    ThreadState * thread_state,
    void * ptr,
    size_t old_size,
    size_t size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * thread_cache,
    Arena * arena)
{
    size_t usable_size = size_classes::alignedSizeToUsableSize(size, alignment);
    if (ALLOCATOR_UNLIKELY(usable_size == 0 || usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return nullptr;
    void * p = internalAllocateAlignedWithCacheExplicitSlab(thread_state, usable_size, alignment, zero, slab, thread_cache, arena);
    if (p == nullptr)
        return nullptr;
    /// Copy at most size bytes (not size+extra), since the caller has no expectation that the extra bytes will be
    /// reliably preserved.
    size_t copy_size = (size < old_size) ? size : old_size;
    memcpy(p, ptr, copy_size);
    internalSizedDeallocate(thread_state, ptr, old_size, thread_cache, nullptr, true);
    return p;
}

/// jemalloc: iralloct_explicit_slab
ALLOCATOR_ALWAYS_INLINE void * internalReallocateExplicitSlab(
    ThreadState * thread_state,
    void * ptr,
    size_t old_size,
    size_t size,
    size_t alignment,
    bool zero,
    bool slab,
    ThreadCache * thread_cache,
    Arena * arena)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size != 0);

    if (alignment != 0 && (reinterpret_cast<uintptr_t>(ptr) & (uintptr_t(alignment) - 1)) != 0)
    {
        /// Existing object alignment is inadequate; allocate new space and copy.
        return internalReallocateRealign(thread_state, ptr, old_size, size, alignment, zero, slab, thread_cache, arena);
    }

    return arenaReallocate(thread_state, arena, ptr, old_size, size, alignment, zero, slab, thread_cache);
}

/// jemalloc: iralloct
ALLOCATOR_ALWAYS_INLINE void * internalReallocateWithCache(
    ThreadState * thread_state,
    void * ptr,
    size_t old_size,
    size_t size,
    size_t alignment,
    size_t usable_size,
    bool zero,
    ThreadCache * thread_cache,
    Arena * arena)
{
    bool slab = size_classes::canUseSlab(usable_size);
    return internalReallocateExplicitSlab(thread_state, ptr, old_size, size, alignment, zero, slab, thread_cache, arena);
}

/// jemalloc: iralloc
ALLOCATOR_ALWAYS_INLINE void *
internalReallocate(ThreadState & thread_state, void * ptr, size_t old_size, size_t size, size_t alignment, size_t usable_size, bool zero)
{
    return internalReallocateWithCache(
        &thread_state, ptr, old_size, size, alignment, usable_size, zero, threadCacheGet(thread_state), nullptr);
}

/// Returns true if the allocation could not be resized in place (`*new_size` is the resulting usable size).
/// jemalloc: ixalloc
ALLOCATOR_ALWAYS_INLINE bool internalExpandInPlace(
    ThreadState * thread_state, void * ptr, size_t old_size, size_t size, size_t extra, size_t alignment, bool zero, size_t * new_size)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size != 0);

    if (alignment != 0 && (reinterpret_cast<uintptr_t>(ptr) & (uintptr_t(alignment) - 1)) != 0)
    {
        /// Existing object alignment is inadequate.
        *new_size = old_size;
        return true;
    }

    return arenaReallocateNoMove(thread_state, ptr, old_size, size, extra, zero, new_size);
}

/// --- Fast paths (jemalloc_internal_inlines_c.h) ---------------------------------------------------------------------

/// jemalloc: fastpath_success_finish
ALLOCATOR_ALWAYS_INLINE void fastPathSuccessFinish(ThreadState * thread_state, uint64_t allocated_after, CacheBin * bin, void * /*ret*/)
{
    thread_state->thread_allocated = allocated_after;
    if constexpr (config::stats)
        ++bin->thread_cache_stats.num_requests;
}

/// The `malloc` fast path. Assumes `size <= SIZE_CLASS_LOOKUP_MAX_CLASS` and that the tcache bin is not empty; otherwise (and
/// for the uninitialized / slow / event-triggering cases, which are all folded into one threshold comparison) it
/// tail-calls `fallback_alloc`, which has the signature of `malloc`, so that no call frame is set up in the common
/// case.
/// jemalloc: imalloc_fastpath
template <void * (*fallback_alloc)(size_t)>
ALLOCATOR_ALWAYS_INLINE void * allocateFastPath(size_t size)
{
    if (ThreadStateStorage::get_allocates && ALLOCATOR_UNLIKELY(!mallocInitialized()))
        return fallback_alloc(size);

    ThreadState * thread_state = ThreadStateStorage::get(false);
    if (ALLOCATOR_UNLIKELY((size > SIZE_CLASS_LOOKUP_MAX_CLASS) || thread_state == nullptr))
        return fallback_alloc(size);

    /// The code below till the branch checking the next_event threshold may execute before `mallocInit`, in which case
    /// the threshold is 0 to trigger slow path and initialization. Note that when uninitialized, only the fast-path
    /// variants of the sz / tsd facilities may be called.
    SizeClassIdx idx;
    /// The `thread_allocated` counter in tsd serves as a general purpose accumulator for bytes of allocation to
    /// trigger different types of events. usize is always needed to advance thread_allocated, though it's not always
    /// needed in the core allocation logic.
    size_t usable_size;
    size_classes::sizeToIndexUsableSizeFastPath(size, &idx, &usable_size);
    /// Fast path relies on size being a bin.
    ALLOCATOR_ASSERT(idx < SIZE_CLASS_NUM_BINS);
    static_assert(SIZE_CLASS_LOOKUP_MAX_CLASS < SIZE_CLASS_SMALL_MAX_CLASS);

    uint64_t allocated;
    uint64_t threshold;
    threadEventMallocFastPathContext(*thread_state, allocated, threshold);
    uint64_t allocated_after = allocated + usable_size;
    /// The ind and usize might be uninitialized (or partially) before `mallocInit`. The assertions check for: 1) full
    /// correctness (usize & ind) when initialized; and 2) guaranteed slow-path (threshold == 0) when !initialized.
    if (!mallocInitialized())
    {
        ALLOCATOR_ASSERT(threshold == 0);
    }
    else
    {
        ALLOCATOR_ASSERT(idx == size_classes::sizeToIndex(size));
        ALLOCATOR_ASSERT(usable_size > 0 && usable_size == size_classes::indexToSize(idx));
    }
    /// Check for events and tsd non-nominal (fast_threshold will be set to 0) in a single branch.
    if (ALLOCATOR_UNLIKELY(allocated_after >= threshold))
        return fallback_alloc(size);
    ALLOCATOR_ASSERT(thread_state->fast());

    ThreadCache * thread_cache = thread_state->threadCacheGet();
    ALLOCATOR_ASSERT(thread_cache == threadCacheGet(*thread_state));
    CacheBin * bin = &thread_cache->bins[idx];

    /// We split up the code this way so that redundant low-water computation doesn't happen on the (more common) case
    /// in which we don't touch the low water mark. The compiler won't do this duplication on its own.
    bool thread_cache_success;
    void * result = bin->allocEasy(thread_cache_success);
    if (thread_cache_success)
    {
        fastPathSuccessFinish(thread_state, allocated_after, bin, result);
        return result;
    }
    result = bin->alloc(thread_cache_success);
    if (thread_cache_success)
    {
        fastPathSuccessFinish(thread_state, allocated_after, bin, result);
        return result;
    }

    return fallback_alloc(size);
}

/// jemalloc: tcache_get_from_ind
ALLOCATOR_ALWAYS_INLINE ThreadCache * threadCacheGetFromIdx(ThreadState & thread_state, unsigned thread_cache_idx, bool slow, bool is_alloc)
{
    ThreadCache * thread_cache;
    if (thread_cache_idx == THREAD_CACHE_IDX_AUTOMATIC)
    {
        if (ALLOCATOR_LIKELY(!slow))
        {
            /// Getting tcache ptr unconditionally.
            thread_cache = thread_state.threadCacheGet();
            ALLOCATOR_ASSERT(thread_cache == threadCacheGet(thread_state));
        }
        else if (is_alloc || ALLOCATOR_LIKELY(thread_state.reentrancyLevel() == 0))
        {
            thread_cache = threadCacheGet(thread_state);
        }
        else
        {
            thread_cache = nullptr;
        }
    }
    else
    {
        /// Should not specify tcache on deallocation path when being reentrant.
        ALLOCATOR_ASSERT(is_alloc || thread_state.reentrancyLevel() == 0 || thread_state.stateNoCleanup());
        if (thread_cache_idx == THREAD_CACHE_IDX_NONE)
            thread_cache = nullptr;
        else
            thread_cache = explicitThreadCachesGet(thread_state, thread_cache_idx);
    }
    return thread_cache;
}

/// Only with `config_opt_size_checks` (never enabled in ClickHouse). Returns true on a detected mismatch.
/// jemalloc: maybe_check_alloc_ctx
ALLOCATOR_ALWAYS_INLINE bool maybeCheckAllocContext(ThreadState & thread_state, void * ptr, AllocContext * alloc_context)
{
    if constexpr (config::option_size_checks)
    {
        AllocContext debug_context;
        arena_extent_map_global.allocContextLookup(&thread_state, ptr, &debug_context);
        if (alloc_context->size_class_idx != debug_context.size_class_idx)
        {
            safetyCheckFailSizedDealloc(
                /* current_dealloc */ true,
                ptr,
                /* true_size */ debug_context.usableSizeGet(),
                /* input_size */ alloc_context->usableSizeGet());
            return true;
        }
        if (alloc_context->slab != debug_context.slab)
        {
            safetyCheckFail("Internal heap corruption detected: mismatch in slab bit");
            return true;
        }
    }
    else
    {
        (void)thread_state;
        (void)ptr;
        (void)alloc_context;
    }
    return false;
}

/// The free fast path does not handle two uncommon cases: 1) sampled profiled objects and 2) sampled junk & stash for
/// use-after-free detection. Both have special alignments which are used to escape the fast path. `prof_sample` is
/// page-aligned, which covers the UAF check when both are enabled. At most one runtime branch.
/// jemalloc: free_fastpath_nonfast_aligned
ALLOCATOR_ALWAYS_INLINE bool freeFastPathNonFastAligned(void * ptr, bool check_profiling)
{
    if constexpr (config::debug)
    {
        if (cacheBinNonFastAligned(ptr))
            ALLOCATOR_ASSERT(profilingSampleAligned(ptr));
    }

    if (config::profiling && check_profiling)
    {
        /// When prof is enabled, the prof_sample alignment is enough.
        return profilingSampleAligned(ptr);
    }

    if constexpr (config::use_after_free_detection)
        return cacheBinNonFastAligned(ptr);

    return false;
}

/// Returns whether or not the free attempt was successful.
/// jemalloc: free_fastpath
ALLOCATOR_ALWAYS_INLINE bool freeFastPath(void * ptr, size_t size, bool size_hint)
{
    ThreadState * thread_state = ThreadStateStorage::get(false);
    /// The branch gets optimized away unless the TSD implementation allocates.
    if (ALLOCATOR_UNLIKELY(thread_state == nullptr))
        return false;
    /// The `tsd_fast` / initialized checks are folded into the branch testing (deallocated_after >= threshold) later in
    /// this function. The threshold will be set to 0 when !tsd_fast.
    ALLOCATOR_ASSERT(
        thread_state->fast()
        || std::atomic_ref<uint64_t>(thread_state->thread_deallocated_next_event_fast).load(std::memory_order_relaxed) == 0);

    AllocContext alloc_context{0, 0, false};
    size_t usable_size;
    if (!size_hint)
    {
        bool error = arena_extent_map_global.allocContextTryLookupFast(*thread_state, ptr, &alloc_context);

        /// Note: profiled objects will have alloc_ctx.slab set.
        if (ALLOCATOR_UNLIKELY(error || !alloc_context.slab || freeFastPathNonFastAligned(ptr, /* check_prof */ false)))
            return false;
        ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);
        usable_size = size_classes::indexToSize(alloc_context.size_class_idx);
    }
    else
    {
        /// Check for both sizes that are too large, and for sampled / special aligned objects. The alignment check will
        /// also check for null ptr.
        if (ALLOCATOR_UNLIKELY(size > SIZE_CLASS_LOOKUP_MAX_CLASS || freeFastPathNonFastAligned(ptr, /* check_prof */ true)))
            return false;
        size_classes::sizeToIndexUsableSizeFastPath(size, &alloc_context.size_class_idx, &usable_size);
        /// Max lookup class must be small.
        ALLOCATOR_ASSERT(alloc_context.size_class_idx < SIZE_CLASS_NUM_BINS);
        /// This is a dead store, except when opt size checking is on.
        alloc_context.slab = true;
    }
    /// Currently the fast path only handles small sizes. The branch on SIZE_CLASS_LOOKUP_MAX_CLASS makes sure of it. This lets
    /// us avoid checking the tcache szind upper limit (i.e. tcache_max) as well.
    ALLOCATOR_ASSERT(alloc_context.slab);

    uint64_t deallocated;
    uint64_t threshold;
    threadEventFreeFastPathContext(*thread_state, deallocated, threshold);

    uint64_t deallocated_after = deallocated + usable_size;
    /// Check for events and tsd non-nominal (fast_threshold will be set to 0) in a single branch. Note that this
    /// handles the uninitialized case as well (TSD init will be triggered on the non-fastpath). Therefore anything
    /// that depends on a functional TSD (e.g. the alloc_ctx sanity check below) needs to be after this branch.
    if (ALLOCATOR_UNLIKELY(deallocated_after >= threshold))
        return false;
    ALLOCATOR_ASSERT(thread_state->fast());
    bool fail = maybeCheckAllocContext(*thread_state, ptr, &alloc_context);
    if (fail)
    {
        /// See the comment in isfree.
        return true;
    }

    ThreadCache * thread_cache = threadCacheGetFromIdx(*thread_state, THREAD_CACHE_IDX_AUTOMATIC, /* slow */ false, /* is_alloc */ false);
    CacheBin * bin = &thread_cache->bins[alloc_context.size_class_idx];

    /// If junking were enabled, this is where we would do it. It's not though, since we ensured above that we're on
    /// the fast path.
    ALLOCATOR_ASSERT(!options.junk_free);

    if (!bin->deallocateEasy(ptr))
        return false;

    thread_state->thread_deallocated = deallocated_after;

    return true;
}

/// The slow paths of `malloc`, `free`, `sdallocx` (noinline, defined in API.cpp).
/// jemalloc: malloc_default, free_default, sdallocx_default
ALLOCATOR_NOINLINE void * mallocDefault(size_t size);
ALLOCATOR_NOINLINE void freeDefault(void * ptr);
ALLOCATOR_NOINLINE void sizedDeallocateWithFlagsDefault(void * ptr, size_t size, int flags);

/// jemalloc: je_sdallocx_noflags
ALLOCATOR_ALWAYS_INLINE void sizedDeallocateNoFlags(void * ptr, size_t size)
{
    if (!freeFastPath(ptr, size, true))
        sizedDeallocateWithFlagsDefault(ptr, size, 0);
}

/// jemalloc: je_sdallocx_impl
ALLOCATOR_ALWAYS_INLINE void sizedDeallocateWithFlagsImpl(void * ptr, size_t size, int flags)
{
    if (flags != 0 || !freeFastPath(ptr, size, true))
        sizedDeallocateWithFlagsDefault(ptr, size, flags);
}

/// jemalloc: je_free_impl
ALLOCATOR_ALWAYS_INLINE void freeImpl(void * ptr)
{
    if (!freeFastPath(ptr, 0, false))
        freeDefault(ptr);
}

}
