/// The exported C API (jemalloc: the "malloc(3)-compatible functions" and "non-standard functions" of
/// `src/jemalloc.c`): `je_malloc`, `je_free`, ..., `je_mallctl*`, `je_malloc_stats_print`.
///
/// Only the `je_*` functions (and the FreeBSD fork hooks in Fork.cpp) have default visibility; everything else is in
/// `namespace jemalloc` with hidden visibility. `je_malloc_message` is defined in Format.cpp, `je_malloc_conf` and
/// `je_malloc_conf_2_conf_harder` in MallocConf.cpp.
///
/// Dropped: the experimental hooks (`hook_invoke_*`), `UTRACE` (not configured), `smallocx`,
/// `je_memalign` / `je_valloc` / `je_pvalloc` (not configured: ClickHouse implements them itself). `opt.xmalloc` can
/// never be enabled (`JEMALLOC_XMALLOC` is not configured, the option is rejected), but its messages are kept.
///
/// The allocation machinery (`allocateWithOptions`) is in InternalMalloc.h; `batch_alloc` is in BatchAlloc.cpp.

#include <allocator/Arena.h>
#include <allocator/ArenaInlines.h>
#include <allocator/Arenas.h>
#include <allocator/Common.h>
#include <allocator/ExtentMap.h>
#include <allocator/Format.h>
#include <allocator/Frontend.h>
#include <allocator/Init.h>
#include <allocator/InternalMalloc.h>
#include <allocator/Mallctl.h>
#include <allocator/Options.h>
#include <allocator/ProfilingHooks.h>
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
extern "C" {
#include <jemalloc/jemalloc_defs.h>
#include <jemalloc/jemalloc_macros.h>
#include <jemalloc/jemalloc_protos.h>
}

namespace jemalloc
{

namespace
{

/// jemalloc: ifree
ALLOCATOR_ALWAYS_INLINE void internalFree(ThreadState & thread_state, void * ptr, ThreadCache * thread_cache, bool slow_path)
{
    if (!slow_path)
        thread_state.assertFast();
    if (thread_state.reentrancyLevel() != 0)
        ALLOCATOR_ASSERT(slow_path);

    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());

    AllocContext alloc_context;
    arena_extent_map_global.allocContextLookup(&thread_state, ptr, &alloc_context);
    ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);

    size_t usable_size = alloc_context.usableSizeGet();
    if (config::profiling && options.profiling)
        profilingFree(thread_state, ptr, usable_size, &alloc_context);

    if (ALLOCATOR_LIKELY(!slow_path))
    {
        internalDeallocateFull(&thread_state, ptr, thread_cache, &alloc_context, false, false);
    }
    else
    {
        if (config::fill && slow_path && options.junk_free)
            junkFreeCallback(ptr, usable_size);
        internalDeallocateFull(&thread_state, ptr, thread_cache, &alloc_context, false, true);
    }
    threadDeallocationEvent(thread_state, usable_size);
}

/// jemalloc: isfree
ALLOCATOR_ALWAYS_INLINE void
internalSizedFree(ThreadState & thread_state, void * ptr, size_t usable_size, ThreadCache * thread_cache, bool slow_path)
{
    if (!slow_path)
        thread_state.assertFast();
    if (thread_state.reentrancyLevel() != 0)
        ALLOCATOR_ASSERT(slow_path);

    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());

    AllocContext alloc_context;
    SizeClassIdx size_class_idx = size_classes::sizeToIndex(usable_size);
    if constexpr (!config::profiling)
    {
        alloc_context.init(size_class_idx, (size_class_idx < SIZE_CLASS_NUM_BINS), usable_size);
    }
    else
    {
        if (ALLOCATOR_LIKELY(!profilingSampleAligned(ptr)))
        {
            /// When the ptr is not page aligned, it was not sampled. usize can be trusted to determine szind and slab.
            alloc_context.init(size_class_idx, (size_class_idx < SIZE_CLASS_NUM_BINS), usable_size);
        }
        else if (options.profiling)
        {
            /// Small sampled allocs promoted can still get correct usize here. Check comments in `Extent::usable_size`.
            arena_extent_map_global.allocContextLookup(&thread_state, ptr, &alloc_context);

            if constexpr (config::option_safety_checks)
            {
                /// Small alloc may have !slab (sampled).
                size_t true_size = alloc_context.usableSizeGet();
                if (ALLOCATOR_UNLIKELY(alloc_context.size_class_idx != size_classes::sizeToIndex(usable_size)))
                    safetyCheckFailSizedDealloc(/* current_dealloc */ true, ptr, /* true_size */ true_size, /* input_size */ usable_size);
            }
        }
        else
        {
            alloc_context.init(size_class_idx, (size_class_idx < SIZE_CLASS_NUM_BINS), usable_size);
        }
    }
    bool fail = maybeCheckAllocContext(thread_state, ptr, &alloc_context);
    if (fail)
    {
        /// This is a heap corruption bug. In real life we'll crash; for the unit test we just want to avoid breaking
        /// anything too badly to get a test result out. Let's leak instead of trying to free.
        return;
    }

    if (config::profiling && options.profiling)
        profilingFree(thread_state, ptr, usable_size, &alloc_context);
    if (ALLOCATOR_LIKELY(!slow_path))
    {
        internalSizedDeallocate(&thread_state, ptr, usable_size, thread_cache, &alloc_context, false);
    }
    else
    {
        if (config::fill && slow_path && options.junk_free)
            junkFreeCallback(ptr, usable_size);
        internalSizedDeallocate(&thread_state, ptr, usable_size, thread_cache, &alloc_context, true);
    }
    threadDeallocationEvent(thread_state, usable_size);
}

/// jemalloc: irallocx_prof_sample
void * reallocateProfilingSample(
    ThreadState * thread_state,
    void * old_ptr,
    size_t old_usable_size,
    size_t usable_size,
    size_t alignment,
    bool zero,
    ThreadCache * thread_cache,
    Arena * arena,
    ProfilingThreadContext * thread_context)
{
    void * p;

    if (thread_context == nullptr)
        return nullptr;

    alignment = profilingSampleAlign(usable_size, alignment);
    /// If the allocation is small enough that it would normally be allocated on a slab, we need to take additional
    /// steps to ensure that it gets its own extent instead.
    if (size_classes::canUseSlab(usable_size))
    {
        size_t bumped_usable_size = size_classes::alignedSizeToUsableSize(usable_size, alignment);
        p = internalReallocateExplicitSlab(
            thread_state, old_ptr, old_usable_size, bumped_usable_size, alignment, zero, /* slab */ false, thread_cache, arena);
        if (p == nullptr)
            return nullptr;
        arenaProfilingPromote(thread_state, p, usable_size, bumped_usable_size);
    }
    else
    {
        p = internalReallocateExplicitSlab(
            thread_state, old_ptr, old_usable_size, usable_size, alignment, zero, /* slab */ false, thread_cache, arena);
    }
    ALLOCATOR_ASSERT(profilingSampleAligned(p));

    return p;
}

/// jemalloc: irallocx_prof
ALLOCATOR_ALWAYS_INLINE void * reallocateProfiling(
    ThreadState & thread_state,
    void * old_ptr,
    size_t old_usable_size,
    size_t size,
    size_t alignment,
    size_t usable_size,
    bool zero,
    ThreadCache * thread_cache,
    Arena * arena,
    AllocContext * alloc_context)
{
    ProfilingInfo old_profiling_info;
    profilingInfoGetAndResetRecent(thread_state, old_ptr, alloc_context, &old_profiling_info);
    bool profiling_active = profilingActiveGetUnlocked();
    bool sample_event = threadEventProfilingSampleEventLookahead(thread_state, usable_size);
    ProfilingThreadContext * thread_context = profilingAllocPrepare(thread_state, profiling_active, sample_event);
    void * p;
    if (ALLOCATOR_UNLIKELY(thread_context != PROFILING_THREAD_CONTEXT_SENTINEL))
        p = reallocateProfilingSample(
            &thread_state, old_ptr, old_usable_size, usable_size, alignment, zero, thread_cache, arena, thread_context);
    else
        p = internalReallocateWithCache(&thread_state, old_ptr, old_usable_size, size, alignment, usable_size, zero, thread_cache, arena);
    if (ALLOCATOR_UNLIKELY(p == nullptr))
    {
        profilingAllocRollback(thread_state, thread_context);
        return nullptr;
    }
    ALLOCATOR_ASSERT(usable_size == allocationSize(&thread_state, p));
    profilingRealloc(
        thread_state, p, size, usable_size, thread_context, profiling_active, old_ptr, old_usable_size, &old_profiling_info, sample_event);

    return p;
}

/// jemalloc: do_rallocx
void * doReallocateWithFlags(void * ptr, size_t size, int flags, bool is_realloc)
{
    void * p;
    size_t usable_size;
    size_t old_usable_size;
    size_t alignment = alignmentFromFlags(flags);
    Arena * arena;

    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());
    ThreadState & thread_state = ThreadState::fetch();

    bool zero = zeroGet(zeroFromFlags(flags), /* slow */ true);

    unsigned arena_idx = arenaIdxFromFlags(flags);
    ThreadCache * thread_cache;
    AllocContext alloc_context;
    if (arenaGetFromIdx(thread_state, arena_idx, &arena))
        goto label_oom;

    thread_cache = threadCacheGetFromIdx(thread_state, threadCacheIdxFromFlags(flags), /* slow */ true, /* is_alloc */ true);

    arena_extent_map_global.allocContextLookup(&thread_state, ptr, &alloc_context);
    ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);
    old_usable_size = alloc_context.usableSizeGet();
    ALLOCATOR_ASSERT(old_usable_size == allocationSize(&thread_state, ptr));
    if (alignedUsableSizeGet(size, alignment, &usable_size, nullptr, false))
        goto label_oom;

    if (config::profiling && options.profiling)
    {
        p = reallocateProfiling(
            thread_state, ptr, old_usable_size, size, alignment, usable_size, zero, thread_cache, arena, &alloc_context);
        if (ALLOCATOR_UNLIKELY(p == nullptr))
            goto label_oom;
    }
    else
    {
        p = internalReallocateWithCache(&thread_state, ptr, old_usable_size, size, alignment, usable_size, zero, thread_cache, arena);
        if (ALLOCATOR_UNLIKELY(p == nullptr))
            goto label_oom;
        ALLOCATOR_ASSERT(usable_size == allocationSize(&thread_state, p));
    }
    ALLOCATOR_ASSERT(alignment == 0 || (reinterpret_cast<uintptr_t>(p) & (alignment - 1)) == 0);
    threadAllocationEvent(thread_state, usable_size);
    threadDeallocationEvent(thread_state, old_usable_size);

    if (config::fill && ALLOCATOR_UNLIKELY(options.junk_alloc) && usable_size > old_usable_size && !zero)
    {
        size_t excess_len = usable_size - old_usable_size;
        void * excess_start = static_cast<char *>(p) + old_usable_size;
        junkAllocCallback(excess_start, excess_len);
    }

    return p;
label_oom:
    if (is_realloc)
        errno = ENOMEM;
    if (config_abort_on_out_of_memory && ALLOCATOR_UNLIKELY(options.abort_on_out_of_memory))
    {
        writeMessage("<jemalloc>: Error in rallocx(): out of memory\n");
        abort();
    }

    return nullptr;
}

/// jemalloc: do_realloc_nonnull_zero
void * doReallocNonNullZero(void * ptr)
{
    if constexpr (config::stats)
        zero_realloc_count.fetch_add(1, std::memory_order_relaxed);
    if (options.zero_realloc_action == ZeroReallocAction::Alloc)
    {
        /// The user might have gotten an alloc setting while expecting a free setting. If that's the case, we at least
        /// try to reduce the harm, and turn off the tcache while allocating, so that we'll get a true first fit.
        return doReallocateWithFlags(ptr, 1, MALLOCX_THREAD_CACHE_NONE_FLAG, true);
    }
    else if (options.zero_realloc_action == ZeroReallocAction::Free)
    {
        ThreadState & thread_state = ThreadState::fetch();

        ThreadCache * thread_cache = threadCacheGetFromIdx(thread_state, THREAD_CACHE_IDX_AUTOMATIC, /* slow */ true, /* is_alloc */ false);
        internalFree(thread_state, ptr, thread_cache, true);

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
ALLOCATOR_ALWAYS_INLINE size_t
expandInPlaceHelper(ThreadState * thread_state, void * ptr, size_t old_usable_size, size_t size, size_t extra, size_t alignment, bool zero)
{
    size_t new_size;

    if (internalExpandInPlace(thread_state, ptr, old_usable_size, size, extra, alignment, zero, &new_size))
        return old_usable_size;

    return new_size;
}

/// jemalloc: ixallocx_prof_sample
size_t expandInPlaceProfilingSample(
    ThreadState * thread_state,
    void * ptr,
    size_t old_usable_size,
    size_t size,
    size_t extra,
    size_t alignment,
    bool zero,
    ProfilingThreadContext * thread_context)
{
    /// Sampled allocation needs to be page aligned.
    if (thread_context == nullptr || !profilingSampleAligned(ptr))
        return old_usable_size;

    return expandInPlaceHelper(thread_state, ptr, old_usable_size, size, extra, alignment, zero);
}

/// jemalloc: ixallocx_prof
ALLOCATOR_ALWAYS_INLINE size_t expandInPlaceProfiling(
    ThreadState & thread_state,
    void * ptr,
    size_t old_usable_size,
    size_t size,
    size_t extra,
    size_t alignment,
    bool zero,
    AllocContext * alloc_context)
{
    /// `old_profiling_info` is only used for asserting that the profiling info isn't changed by the `internalExpandInPlace` call.
    ProfilingInfo old_profiling_info;
    profilingInfoGet(thread_state, ptr, alloc_context, &old_profiling_info);

    /// usize isn't knowable before `internalExpandInPlace` returns when extra is non-zero. Therefore, compute its maximum possible
    /// value and use that in `profilingAllocPrepare` to decide whether to capture a backtrace. `profilingRealloc` will use the
    /// actual usize to decide whether to sample.
    size_t usable_size_max;
    if (alignedUsableSizeGet(size + extra, alignment, &usable_size_max, nullptr, false))
    {
        /// usize_max is out of range, and chances are that allocation will fail, but use the maximum possible value
        /// and carry on with `profilingAllocPrepare`, just in case allocation succeeds.
        usable_size_max = SIZE_CLASS_LARGE_MAX_CLASS;
    }
    bool profiling_active = profilingActiveGetUnlocked();
    bool sample_event = threadEventProfilingSampleEventLookahead(thread_state, usable_size_max);
    ProfilingThreadContext * thread_context = profilingAllocPrepare(thread_state, profiling_active, sample_event);

    size_t usable_size;
    if (ALLOCATOR_UNLIKELY(thread_context != PROFILING_THREAD_CONTEXT_SENTINEL))
        usable_size = expandInPlaceProfilingSample(&thread_state, ptr, old_usable_size, size, extra, alignment, zero, thread_context);
    else
        usable_size = expandInPlaceHelper(&thread_state, ptr, old_usable_size, size, extra, alignment, zero);

    /// At this point we can still safely get the original profiling information associated with the ptr, because
    /// (a) the `Extent` object associated with the ptr still lives and (b) the profiling info fields are not touched.
    /// "(a)" is asserted in the outer `je_xallocx` function, and "(b)" is indirectly verified below by checking that
    /// the alloc_tctx field is unchanged.
    ProfilingInfo profiling_info;
    if (usable_size == old_usable_size)
    {
        profilingInfoGet(thread_state, ptr, alloc_context, &profiling_info);
        profilingAllocRollback(thread_state, thread_context);
    }
    else
    {
        /// Need to retrieve the new alloc_ctx since the modification to the extent has already been done.
        AllocContext new_alloc_context;
        arena_extent_map_global.allocContextLookup(&thread_state, ptr, &new_alloc_context);
        profilingInfoGetAndResetRecent(thread_state, ptr, &new_alloc_context, &profiling_info);
        ALLOCATOR_ASSERT(usable_size <= usable_size_max);
        sample_event = threadEventProfilingSampleEventLookahead(thread_state, usable_size);
        profilingRealloc(
            thread_state, ptr, size, usable_size, thread_context, profiling_active, ptr, old_usable_size, &profiling_info, sample_event);
    }

    ALLOCATOR_ASSERT(old_profiling_info.alloc_thread_context == profiling_info.alloc_thread_context);
    return usable_size;
}

/// jemalloc: inallocx
ALLOCATOR_ALWAYS_INLINE size_t usableSizeForRequest(ThreadState * /*tsdn*/, size_t size, int flags)
{
    size_t usable_size;
    /// In case of out of range, let the user see it rather than fail.
    alignedUsableSizeGet(size, alignmentFromFlags(flags), &usable_size, nullptr, false);
    return usable_size;
}

/// jemalloc: je_malloc_usable_size_impl
ALLOCATOR_ALWAYS_INLINE size_t mallocUsableSizeImpl(const void * ptr)
{
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState * thread_state = ThreadState::threadStateFetch();

    size_t result;
    if (ALLOCATOR_UNLIKELY(ptr == nullptr))
    {
        result = 0;
    }
    else
    {
        /// jemalloc uses `ivsalloc` with `config_debug` (never in ClickHouse) or `force_ivsalloc` (never set).
        result = allocationSize(thread_state, ptr);
    }

    return result;
}

}

/// This variant has logging hook on exit but not on entry. It's called only by `je_malloc`, which tail-calls it.
/// jemalloc: malloc_default
ALLOCATOR_NOINLINE void * mallocDefault(size_t size)
{
    void * result;
    StaticOptions static_options;
    DynamicOptions dynamic_options;

    static_options.null_out_result_on_error = true;
    static_options.set_errno_on_error = true;
    static_options.oom_string = "<jemalloc>: Error in malloc(): out of memory\n";

    dynamic_options.result = &result;
    dynamic_options.num_items = 1;
    dynamic_options.item_size = size;

    allocateWithOptions(static_options, dynamic_options);

    return result;
}

/// jemalloc: free_default
ALLOCATOR_NOINLINE void freeDefault(void * ptr)
{
    if (ALLOCATOR_LIKELY(ptr != nullptr))
    {
        /// We avoid setting up tsd fully (e.g. tcache, arena binding) based on only free() calls -- other activities
        /// trigger the minimal to full transition. This is because free() may happen during thread shutdown after tls
        /// deallocation: if a thread never had any malloc activities until then, a fully-setup tsd won't be destructed
        /// properly.
        ThreadState & thread_state = ThreadState::fetchMin();

        if (ALLOCATOR_LIKELY(thread_state.fast()))
        {
            ThreadCache * thread_cache
                = threadCacheGetFromIdx(thread_state, THREAD_CACHE_IDX_AUTOMATIC, /* slow */ false, /* is_alloc */ false);
            internalFree(thread_state, ptr, thread_cache, /* slow */ false);
        }
        else
        {
            ThreadCache * thread_cache
                = threadCacheGetFromIdx(thread_state, THREAD_CACHE_IDX_AUTOMATIC, /* slow */ true, /* is_alloc */ false);
            internalFree(thread_state, ptr, thread_cache, /* slow */ true);
        }
    }
}

/// jemalloc: sdallocx_default
ALLOCATOR_NOINLINE void sizedDeallocateWithFlagsDefault(void * ptr, size_t size, int flags)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState & thread_state = ThreadState::fetchMin();
    bool fast = thread_state.fast();
    size_t usable_size = usableSizeForRequest(&thread_state, size, flags);

    unsigned thread_cache_idx = threadCacheIdxFromFlags(flags);
    ThreadCache * thread_cache = threadCacheGetFromIdx(thread_state, thread_cache_idx, !fast, /* is_alloc */ false);

    if (ALLOCATOR_LIKELY(fast))
    {
        thread_state.assertFast();
        internalSizedFree(thread_state, ptr, usable_size, thread_cache, false);
    }
    else
    {
        internalSizedFree(thread_state, ptr, usable_size, thread_cache, true);
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
    return allocateFastPath<&mallocDefault>(size);
}

/// jemalloc: je_posix_memalign
JEMALLOC_EXPORT int JEMALLOC_SYS_NOTHROW je_posix_memalign(void ** memory_ptr, size_t alignment, size_t size) JEMALLOC_CXX_THROW
{
    StaticOptions static_options;
    DynamicOptions dynamic_options;

    static_options.bump_empty_aligned_alloc = true;
    static_options.min_alignment = sizeof(void *);
    static_options.oom_string = "<jemalloc>: Error allocating aligned memory: out of memory\n";
    static_options.invalid_alignment_string = "<jemalloc>: Error allocating aligned memory: invalid alignment\n";

    dynamic_options.result = memory_ptr;
    dynamic_options.num_items = 1;
    dynamic_options.item_size = size;
    dynamic_options.alignment = alignment;

    return allocateWithOptions(static_options, dynamic_options);
}

/// jemalloc: je_aligned_alloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_aligned_alloc(size_t alignment, size_t size) JEMALLOC_CXX_THROW
{
    void * result;

    StaticOptions static_options;
    DynamicOptions dynamic_options;

    static_options.bump_empty_aligned_alloc = true;
    static_options.null_out_result_on_error = true;
    static_options.set_errno_on_error = true;
    static_options.min_alignment = 1;
    static_options.oom_string = "<jemalloc>: Error allocating aligned memory: out of memory\n";
    static_options.invalid_alignment_string = "<jemalloc>: Error allocating aligned memory: invalid alignment\n";

    dynamic_options.result = &result;
    dynamic_options.num_items = 1;
    dynamic_options.item_size = size;
    dynamic_options.alignment = alignment;

    allocateWithOptions(static_options, dynamic_options);

    return result;
}

/// jemalloc: je_calloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_calloc(size_t num, size_t size) JEMALLOC_CXX_THROW
{
    void * result;
    StaticOptions static_options;
    DynamicOptions dynamic_options;

    static_options.may_overflow = true;
    static_options.null_out_result_on_error = true;
    static_options.set_errno_on_error = true;
    static_options.oom_string = "<jemalloc>: Error in calloc(): out of memory\n";

    dynamic_options.result = &result;
    dynamic_options.num_items = num;
    dynamic_options.item_size = size;
    dynamic_options.zero = true;

    allocateWithOptions(static_options, dynamic_options);

    return result;
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
    sizedDeallocateNoFlags(ptr, size);
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
    return allocateWithFlags(size, flags);
}

/// jemalloc: je_rallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW * je_rallocx(void * ptr, size_t size, int flags)
{
    return doReallocateWithFlags(ptr, size, flags, false);
}

/// jemalloc: je_realloc
JEMALLOC_EXPORT void JEMALLOC_SYS_NOTHROW * je_realloc(void * ptr, size_t size) JEMALLOC_CXX_THROW
{
    if (ALLOCATOR_LIKELY(ptr != nullptr && size != 0))
    {
        return doReallocateWithFlags(ptr, size, 0, true);
    }
    else if (ptr != nullptr && size == 0)
    {
        return doReallocNonNullZero(ptr);
    }
    else
    {
        /// realloc(NULL, size) is equivalent to malloc(size).
        void * result;

        StaticOptions static_options;
        DynamicOptions dynamic_options;

        static_options.null_out_result_on_error = true;
        static_options.set_errno_on_error = true;
        static_options.oom_string = "<jemalloc>: Error in realloc(): out of memory\n";

        dynamic_options.result = &result;
        dynamic_options.num_items = 1;
        dynamic_options.item_size = size;

        allocateWithOptions(static_options, dynamic_options);
        return result;
    }
}

/// jemalloc: je_xallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_xallocx(void * ptr, size_t size, size_t extra, int flags)
{
    size_t usable_size;
    size_t old_usable_size;
    size_t alignment = alignmentFromFlags(flags);
    bool zero = zeroGet(zeroFromFlags(flags), /* slow */ true);

    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(SIZE_MAX - size >= extra);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());
    ThreadState & thread_state = ThreadState::fetch();

    /// `old_extent` is only for verifying that xallocx() keeps the `Extent` object associated with the ptr (though the
    /// content of the object can be changed).
    [[maybe_unused]] Extent * old_extent = config::debug ? arena_extent_map_global.extentLookup(&thread_state, ptr) : nullptr;

    AllocContext alloc_context;
    arena_extent_map_global.allocContextLookup(&thread_state, ptr, &alloc_context);
    ALLOCATOR_ASSERT(alloc_context.size_class_idx != SIZE_CLASS_NUM_SIZES);
    old_usable_size = alloc_context.usableSizeGet();
    ALLOCATOR_ASSERT(old_usable_size == allocationSize(&thread_state, ptr));
    /// The API explicitly absolves itself of protecting against (size + extra) numerical overflow, but we may need to
    /// clamp extra to avoid exceeding SIZE_CLASS_LARGE_MAX_CLASS.
    ///
    /// Ordinarily, size limit checking is handled deeper down, but here we have to check as part of (size + extra)
    /// clamping, since we need the clamped value in the above helper functions.
    if (ALLOCATOR_UNLIKELY(size > SIZE_CLASS_LARGE_MAX_CLASS))
    {
        usable_size = old_usable_size;
        goto label_not_resized;
    }
    if (ALLOCATOR_UNLIKELY(SIZE_CLASS_LARGE_MAX_CLASS - size < extra))
        extra = SIZE_CLASS_LARGE_MAX_CLASS - size;

    if (config::profiling && options.profiling)
        usable_size = expandInPlaceProfiling(thread_state, ptr, old_usable_size, size, extra, alignment, zero, &alloc_context);
    else
        usable_size = expandInPlaceHelper(&thread_state, ptr, old_usable_size, size, extra, alignment, zero);

    /// xallocx() should keep using the same `Extent` object (though its content can be changed).
    ALLOCATOR_ASSERT(!config::debug || arena_extent_map_global.extentLookup(&thread_state, ptr) == old_extent);

    if (ALLOCATOR_UNLIKELY(usable_size == old_usable_size))
        goto label_not_resized;
    threadAllocationEvent(thread_state, usable_size);
    threadDeallocationEvent(thread_state, old_usable_size);

    if (config::fill && ALLOCATOR_UNLIKELY(options.junk_alloc) && usable_size > old_usable_size && !zero)
    {
        size_t excess_len = usable_size - old_usable_size;
        void * excess_start = static_cast<char *>(ptr) + old_usable_size;
        junkAllocCallback(excess_start, excess_len);
    }
label_not_resized:
    return usable_size;
}

/// jemalloc: je_sallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_sallocx(const void * ptr, int /*flags*/)
{
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());
    ALLOCATOR_ASSERT(ptr != nullptr);

    ThreadState * thread_state = ThreadState::threadStateFetch();

    /// jemalloc uses `ivsalloc` with `config_debug` (never in ClickHouse) or `force_ivsalloc` (never set).
    return allocationSize(thread_state, ptr);
}

/// jemalloc: je_dallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_dallocx(void * ptr, int flags)
{
    ALLOCATOR_ASSERT(ptr != nullptr);
    ALLOCATOR_ASSERT(mallocInitialized() || mallocIsInitializer());

    ThreadState & thread_state = ThreadState::fetchMin();
    bool fast = thread_state.fast();

    unsigned thread_cache_idx = threadCacheIdxFromFlags(flags);
    ThreadCache * thread_cache = threadCacheGetFromIdx(thread_state, thread_cache_idx, !fast, /* is_alloc */ false);

    if (ALLOCATOR_LIKELY(fast))
    {
        thread_state.assertFast();
        internalFree(thread_state, ptr, thread_cache, false);
    }
    else
    {
        internalFree(thread_state, ptr, thread_cache, true);
    }
}

/// jemalloc: je_sdallocx
JEMALLOC_EXPORT void JEMALLOC_NOTHROW je_sdallocx(void * ptr, size_t size, int flags)
{
    sizedDeallocateWithFlagsImpl(ptr, size, flags);
}

/// jemalloc: je_nallocx
JEMALLOC_EXPORT size_t JEMALLOC_NOTHROW je_nallocx(size_t size, int flags)
{
    ALLOCATOR_ASSERT(size != 0);

    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return 0;

    ThreadState * thread_state = ThreadState::threadStateFetch();

    size_t usable_size = usableSizeForRequest(thread_state, size, flags);
    if (ALLOCATOR_UNLIKELY(usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return 0;

    return usable_size;
}

/// jemalloc: je_mallctl
JEMALLOC_EXPORT int JEMALLOC_NOTHROW
je_mallctl(const char * name, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & thread_state = ThreadState::fetch();
    return mallctlByName(thread_state, name, old_value, old_length_ptr, new_value, new_length);
}

/// jemalloc: je_mallctlnametomib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW je_mallctl_name_to_numeric_path(const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & thread_state = ThreadState::fetch();
    return mallctlNameToNumericPath(thread_state, name, numeric_path_ptr, numeric_path_length_ptr);
}

/// jemalloc: je_mallctlbymib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW
je_mallctl_by_numeric_path(const size_t * numeric_path, size_t numeric_path_length, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    if (ALLOCATOR_UNLIKELY(mallocInit()))
        return EAGAIN;

    ThreadState & thread_state = ThreadState::fetch();
    return mallctlByNumericPath(thread_state, numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// The standard jemalloc names of the two functions above, kept for compatibility with code written for jemalloc.
/// jemalloc: je_mallctlnametomib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW je_mallctlnametomib(const char * name, size_t * numeric_path_ptr, size_t * numeric_path_length_ptr)
{
    return je_mallctl_name_to_numeric_path(name, numeric_path_ptr, numeric_path_length_ptr);
}

/// jemalloc: je_mallctlbymib
JEMALLOC_EXPORT int JEMALLOC_NOTHROW
je_mallctlbymib(const size_t * numeric_path, size_t numeric_path_length, void * old_value, size_t * old_length_ptr, void * new_value, size_t new_length)
{
    return je_mallctl_by_numeric_path(numeric_path, numeric_path_length, old_value, old_length_ptr, new_value, new_length);
}

/// NB: does not initialize the allocator itself (like jemalloc), but the printer reads everything through `mallctl`,
/// which does: the mandatory `epoch` refresh (`statsMallctl`, jemalloc: `je_mallctl`) initializes it on first use,
/// exactly as jemalloc's `stats_print` does.
/// jemalloc: je_malloc_stats_print
JEMALLOC_EXPORT void JEMALLOC_NOTHROW
je_malloc_stats_print(void (*write_callback)(void *, const char *), void * callback_argument, const char * options_string)
{
    mallocStatsPrint(write_callback, callback_argument, options_string);
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
