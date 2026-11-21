#pragma once

/// The allocation front-end of the "malloc(3)-compatible functions" and "non-standard functions" (jemalloc: the
/// `allocateWithOptions` machinery of `src/jemalloc.c`). Header-inline so that `je_malloc`, `je_mallocx`, ... in API.cpp compile
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
#include <allocator/ProfilingHooks.h>
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
constexpr bool config_abort_on_out_of_memory = false;

/// Settings determined by the documented behavior of the allocation functions.
/// jemalloc: static_opts_t, static_opts_init
struct StaticOptions
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
    bool usable_size = false;
};

/// jemalloc: dynamic_opts_t, dynamic_opts_init
struct DynamicOptions
{
    void ** result = nullptr;
    size_t usable_size = 0;
    size_t num_items = 0;
    size_t item_size = 0;
    size_t alignment = 0;
    bool zero = false;
    unsigned thread_cache_idx = THREAD_CACHE_IDX_AUTOMATIC;
    unsigned arena_idx = ARENA_IDX_AUTOMATIC;
};

/// `idx` is optional and is only checked and filled if `alignment == 0`. Returns true if the result is out of range.
/// jemalloc: aligned_usize_get
ALLOCATOR_ALWAYS_INLINE bool
alignedUsableSizeGet(size_t size, size_t alignment, size_t * usable_size, SizeClassIdx * idx, bool bump_empty_aligned_alloc)
{
    ALLOCATOR_ASSERT(usable_size != nullptr);
    if (alignment == 0)
    {
        if (idx != nullptr)
        {
            *idx = size_classes::sizeToIndex(size);
            if (ALLOCATOR_UNLIKELY(*idx >= SIZE_CLASS_NUM_SIZES))
                return true;
            *usable_size
                = size_classes::largeSizeClassesDisabled() ? size_classes::sizeToUsableSize(size) : size_classes::indexToSize(*idx);
            ALLOCATOR_ASSERT(*usable_size > 0 && *usable_size <= SIZE_CLASS_LARGE_MAX_CLASS);
            return false;
        }
        *usable_size = size_classes::sizeToUsableSize(size);
    }
    else
    {
        if (bump_empty_aligned_alloc && ALLOCATOR_UNLIKELY(size == 0))
            size = 1;
        *usable_size = size_classes::alignedSizeToUsableSize(size, alignment);
    }
    if (ALLOCATOR_UNLIKELY(*usable_size == 0 || *usable_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return true;
    return false;
}

/// jemalloc: zero_get
ALLOCATOR_ALWAYS_INLINE bool zeroGet(bool guarantee, bool slow)
{
    if (config::fill && slow && ALLOCATOR_UNLIKELY(options.zero))
        return true;
    return guarantee;
}

/// Returns true if a manual arena is specified and `arenaGet` OOMs.
/// jemalloc: arena_get_from_ind
ALLOCATOR_ALWAYS_INLINE bool arenaGetFromIdx(ThreadState & thread_state, unsigned arena_idx, Arena ** arena_ptr)
{
    if (arena_idx == ARENA_IDX_AUTOMATIC)
    {
        /// In case of automatic arena management, we defer arena computation until as late as we can, hoping to fill
        /// the allocation out of the tcache.
        *arena_ptr = nullptr;
    }
    else
    {
        *arena_ptr = arenaGet(&thread_state, arena_idx, true);
        if (ALLOCATOR_UNLIKELY(*arena_ptr == nullptr) && arena_idx >= num_arenas_auto)
            return true;
    }
    return false;
}

/// `idx` is ignored if `dynamic_options.alignment > 0`.
/// jemalloc: imalloc_no_sample
ALLOCATOR_ALWAYS_INLINE void * allocateWithOptionsNoSample(
    StaticOptions & static_options,
    DynamicOptions & dynamic_options,
    ThreadState & thread_state,
    size_t size,
    size_t usable_size,
    SizeClassIdx idx,
    bool slab)
{
    /// Fill in the tcache.
    ThreadCache * thread_cache
        = threadCacheGetFromIdx(thread_state, dynamic_options.thread_cache_idx, static_options.slow, /* is_alloc */ true);

    /// Fill in the arena.
    Arena * arena;
    if (arenaGetFromIdx(thread_state, dynamic_options.arena_idx, &arena))
        return nullptr;

    if (ALLOCATOR_UNLIKELY(dynamic_options.alignment != 0))
        return internalAllocateAlignedWithCacheExplicitSlab(
            &thread_state, usable_size, dynamic_options.alignment, dynamic_options.zero, slab, thread_cache, arena);

    return internalAllocateFullExplicitSlab(
        &thread_state, size, idx, dynamic_options.zero, slab, thread_cache, false, arena, static_options.slow);
}

/// jemalloc: imalloc_sample
ALLOCATOR_ALWAYS_INLINE void * allocateWithOptionsSample(
    StaticOptions & static_options, DynamicOptions & dynamic_options, ThreadState & thread_state, size_t usable_size, SizeClassIdx idx)
{
    void * result;

    dynamic_options.alignment = profilingSampleAlign(usable_size, dynamic_options.alignment);
    /// If the allocation is small enough that it would normally be allocated on a slab, we need to take additional
    /// steps to ensure that it gets its own extent instead.
    if (size_classes::canUseSlab(usable_size))
    {
        ALLOCATOR_ASSERT((dynamic_options.alignment & PROFILING_SAMPLE_ALIGNMENT_MASK) == 0);
        size_t bumped_usable_size = size_classes::alignedSizeToUsableSize(usable_size, dynamic_options.alignment);
        SizeClassIdx bumped_idx = size_classes::sizeToIndex(bumped_usable_size);
        dynamic_options.thread_cache_idx = THREAD_CACHE_IDX_NONE;
        result = allocateWithOptionsNoSample(
            static_options, dynamic_options, thread_state, bumped_usable_size, bumped_usable_size, bumped_idx, /* slab */ false);
        if (ALLOCATOR_UNLIKELY(result == nullptr))
            return nullptr;
        arenaProfilingPromote(&thread_state, result, usable_size, bumped_usable_size);
    }
    else
    {
        result
            = allocateWithOptionsNoSample(static_options, dynamic_options, thread_state, usable_size, usable_size, idx, /* slab */ false);
    }
    ALLOCATOR_ASSERT(profilingSampleAligned(result));

    return result;
}

/// Returns true if the allocation will overflow, and false otherwise. Sets `*size` to the product either way.
/// jemalloc: compute_size_with_overflow
ALLOCATOR_ALWAYS_INLINE bool computeSizeWithOverflow(bool may_overflow, DynamicOptions & dynamic_options, size_t * size)
{
    /// This function is just num_items * item_size, except that we may have to check for overflow.

    if (!may_overflow)
    {
        ALLOCATOR_ASSERT(dynamic_options.num_items == 1);
        *size = dynamic_options.item_size;
        return false;
    }

    /// A size_t with its high-half bits all set to 1.
    constexpr size_t high_bits = SIZE_MAX << (sizeof(size_t) * 8 / 2);

    *size = dynamic_options.item_size * dynamic_options.num_items;

    if (ALLOCATOR_UNLIKELY(*size == 0))
        return (dynamic_options.num_items != 0 && dynamic_options.item_size != 0);

    /// We got a non-zero size, but we don't know if we overflowed to get there. To avoid having to do a divide, we'll
    /// be clever and note that if both A and B can be represented in N/2 bits, then their product can be represented
    /// in N bits (without the possibility of overflow).
    if (ALLOCATOR_LIKELY((high_bits & (dynamic_options.num_items | dynamic_options.item_size)) == 0))
        return false;
    if (ALLOCATOR_LIKELY(*size / dynamic_options.item_size == dynamic_options.num_items))
        return false;
    return true;
}

/// Returns the errno-style error code of the allocation.
/// jemalloc: imalloc_body
ALLOCATOR_ALWAYS_INLINE int
allocateWithOptionsBody(StaticOptions & static_options, DynamicOptions & dynamic_options, ThreadState & thread_state)
{
    /// Where the actual allocation memory will live.
    void * allocation = nullptr;
    /// Filled in by `computeSizeWithOverflow` below.
    size_t size = 0;
    /// The zero initialization for ind is actually a dead store, in that its value is reset before any branch on its
    /// value is taken.
    SizeClassIdx idx = 0;
    /// usize will always be properly initialized.
    size_t usable_size;

    /// Reentrancy is only checked on slow path.
    int8_t reentrancy_level;

    /// Compute the amount of memory the user wants.
    if (ALLOCATOR_UNLIKELY(computeSizeWithOverflow(static_options.may_overflow, dynamic_options, &size)))
        goto label_oom;

    if (ALLOCATOR_UNLIKELY(
            dynamic_options.alignment < static_options.min_alignment || (dynamic_options.alignment & (dynamic_options.alignment - 1)) != 0))
        goto label_invalid_alignment;

    /// This is the beginning of the "core" algorithm.
    dynamic_options.zero = zeroGet(dynamic_options.zero, static_options.slow);
    if (alignedUsableSizeGet(size, dynamic_options.alignment, &usable_size, &idx, static_options.bump_empty_aligned_alloc))
        goto label_oom;
    dynamic_options.usable_size = usable_size;
    /// Validate the user input.
    if (static_options.assert_nonempty_alloc)
        ALLOCATOR_ASSERT(size != 0);

    /// If we need to handle reentrancy, we can do it out of a known-initialized arena (i.e. arena 0).
    reentrancy_level = thread_state.reentrancyLevel();
    if (static_options.slow && ALLOCATOR_UNLIKELY(reentrancy_level > 0))
    {
        /// We should never specify particular arenas or tcaches from within our internal allocations.
        ALLOCATOR_ASSERT(
            dynamic_options.thread_cache_idx == THREAD_CACHE_IDX_AUTOMATIC || dynamic_options.thread_cache_idx == THREAD_CACHE_IDX_NONE);
        ALLOCATOR_ASSERT(dynamic_options.arena_idx == ARENA_IDX_AUTOMATIC);
        dynamic_options.thread_cache_idx = THREAD_CACHE_IDX_NONE;
        /// We know that arena 0 has already been initialized.
        dynamic_options.arena_idx = 0;
    }

    /// If `dynamic_options.alignment > 0`, then ind is still 0, but usize was computed in the previous if statement. Down the
    /// positive alignment path, `allocateWithOptionsNoSample` and `allocateWithOptionsSample` will ignore ind.

    /// If profiling is on, get our profiling context.
    if (config::profiling && options.profiling)
    {
        bool profiling_active = profilingActiveGetUnlocked();
        bool sample_event = threadEventProfilingSampleEventLookahead(thread_state, usable_size);
        ProfilingThreadContext * thread_context = profilingAllocPrepare(thread_state, profiling_active, sample_event);

        AllocContext alloc_context;
        if (ALLOCATOR_LIKELY(thread_context == PROFILING_THREAD_CONTEXT_SENTINEL))
        {
            alloc_context.slab = size_classes::canUseSlab(usable_size);
            allocation = allocateWithOptionsNoSample(
                static_options, dynamic_options, thread_state, usable_size, usable_size, idx, alloc_context.slab);
        }
        else if (thread_context != nullptr)
        {
            allocation = allocateWithOptionsSample(static_options, dynamic_options, thread_state, usable_size, idx);
            alloc_context.slab = false;
        }
        else
        {
            allocation = nullptr;
        }

        if (ALLOCATOR_UNLIKELY(allocation == nullptr))
        {
            profilingAllocRollback(thread_state, thread_context);
            goto label_oom;
        }
        profilingMalloc(thread_state, allocation, size, usable_size, &alloc_context, thread_context);
    }
    else
    {
        ALLOCATOR_ASSERT(!options.profiling);
        allocation = allocateWithOptionsNoSample(
            static_options, dynamic_options, thread_state, size, usable_size, idx, size_classes::canUseSlab(usable_size));
        if (ALLOCATOR_UNLIKELY(allocation == nullptr))
            goto label_oom;
    }

    /// Allocation has been done at this point. We still have some post-allocation work to do though.

    threadAllocationEvent(thread_state, usable_size);

    ALLOCATOR_ASSERT(dynamic_options.alignment == 0 || (reinterpret_cast<uintptr_t>(allocation) & (dynamic_options.alignment - 1)) == 0);

    ALLOCATOR_ASSERT(usable_size == allocationSize(&thread_state, allocation));

    if (config::fill && static_options.slow && !dynamic_options.zero && ALLOCATOR_UNLIKELY(options.junk_alloc))
        junkAllocCallback(allocation, usable_size);

    /// Success!
    *dynamic_options.result = allocation;
    return 0;

label_oom:
    if (ALLOCATOR_UNLIKELY(static_options.slow) && config_abort_on_out_of_memory && ALLOCATOR_UNLIKELY(options.abort_on_out_of_memory))
    {
        writeMessage(static_options.oom_string);
        abort();
    }

    if (static_options.set_errno_on_error)
        errno = ENOMEM;

    if (static_options.null_out_result_on_error)
        *dynamic_options.result = nullptr;

    return ENOMEM;

    /// This label is only jumped to by one goto; we move it out of line anyways to avoid obscuring the non-error paths,
    /// and for symmetry with the oom case.
label_invalid_alignment:
    if (config_abort_on_out_of_memory && ALLOCATOR_UNLIKELY(options.abort_on_out_of_memory))
    {
        writeMessage(static_options.invalid_alignment_string);
        abort();
    }

    if (static_options.set_errno_on_error)
        errno = EINVAL;

    if (static_options.null_out_result_on_error)
        *dynamic_options.result = nullptr;

    return EINVAL;
}

/// jemalloc: imalloc_init_check
ALLOCATOR_ALWAYS_INLINE bool allocateWithOptionsInitCheck(StaticOptions & static_options, DynamicOptions & dynamic_options)
{
    if (ALLOCATOR_UNLIKELY(!mallocInitialized()) && ALLOCATOR_UNLIKELY(mallocInit()))
    {
        if (config_abort_on_out_of_memory && ALLOCATOR_UNLIKELY(options.abort_on_out_of_memory))
        {
            writeMessage(static_options.oom_string);
            abort();
        }
        errno = ENOMEM;
        *dynamic_options.result = nullptr;

        return false;
    }

    return true;
}

/// Returns the errno-style error code of the allocation.
/// jemalloc: imalloc
ALLOCATOR_ALWAYS_INLINE int allocateWithOptions(StaticOptions & static_options, DynamicOptions & dynamic_options)
{
    if (ThreadStateStorage::get_allocates && !allocateWithOptionsInitCheck(static_options, dynamic_options))
        return ENOMEM;

    /// We always need the tsd. Let's grab it right away.
    ThreadState & thread_state = ThreadState::fetch();
    if (ALLOCATOR_LIKELY(thread_state.fast()))
    {
        /// Fast and common path.
        thread_state.assertFast();
        static_options.slow = false;
        return allocateWithOptionsBody(static_options, dynamic_options, thread_state);
    }
    else
    {
        if (!ThreadStateStorage::get_allocates && !allocateWithOptionsInitCheck(static_options, dynamic_options))
            return ENOMEM;

        static_options.slow = true;
        return allocateWithOptionsBody(static_options, dynamic_options, thread_state);
    }
}

/// The body of `je_mallocx`. jemalloc: je_mallocx
ALLOCATOR_ALWAYS_INLINE void * allocateWithFlags(size_t size, int flags)
{
    void * result;
    StaticOptions static_options;
    DynamicOptions dynamic_options;

    static_options.assert_nonempty_alloc = true;
    static_options.null_out_result_on_error = true;
    static_options.oom_string = "<jemalloc>: Error in mallocx(): out of memory\n";

    dynamic_options.result = &result;
    dynamic_options.num_items = 1;
    dynamic_options.item_size = size;
    if (ALLOCATOR_UNLIKELY(flags != 0))
    {
        dynamic_options.alignment = alignmentFromFlags(flags);
        dynamic_options.zero = zeroFromFlags(flags);
        dynamic_options.thread_cache_idx = threadCacheIdxFromFlags(flags);
        dynamic_options.arena_idx = arenaIdxFromFlags(flags);
    }

    allocateWithOptions(static_options, dynamic_options);

    return result;
}

/// Allocates up to `num` objects of `size` with `flags` (as `mallocx`) into `ptrs`; returns the number allocated.
/// Defined in BatchAlloc.cpp. jemalloc: batch_alloc
size_t batchAlloc(void ** ptrs, size_t num, size_t size, int flags);

}
