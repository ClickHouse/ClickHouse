#include <allocator/Sanitizer.h>

#include <allocator/BackgroundThread.h>
#include <allocator/ExtentOps.h>
#include <allocator/Format.h>
#include <allocator/PageAllocator.h>

#include <cstdarg>
#include <cstdlib>

namespace jemalloc
{

constinit uintptr_t sanitizer_cache_bin_non_fast_mask = SANITIZER_CACHE_BIN_NON_FAST_MASK_DEFAULT;

namespace
{

/// jemalloc: san_find_guarded_addr
ALLOCATOR_ALWAYS_INLINE void
sanitizerFindGuardedAddr(Extent * extent, void ** guard1, void ** guard2, void ** addr, size_t size, bool left, bool right)
{
    ALLOCATOR_ASSERT(!extent->guarded());
    ALLOCATOR_ASSERT(size % PAGE == 0);
    *addr = extent->base();
    if (left)
    {
        *guard1 = *addr;
        *addr = static_cast<std::byte *>(*addr) + SANITIZER_PAGE_GUARD;
    }
    else
    {
        *guard1 = nullptr;
    }

    if (right)
        *guard2 = static_cast<std::byte *>(*addr) + size;
    else
        *guard2 = nullptr;
}

/// jemalloc: san_find_unguarded_addr
ALLOCATOR_ALWAYS_INLINE void
sanitizerFindUnguardedAddr(Extent * extent, void ** guard1, void ** guard2, void ** addr, size_t size, bool left, bool right)
{
    ALLOCATOR_ASSERT(extent->guarded());
    ALLOCATOR_ASSERT(size % PAGE == 0);
    *addr = extent->base();
    if (right)
        *guard2 = static_cast<std::byte *>(*addr) + size;
    else
        *guard2 = nullptr;

    if (left)
    {
        *guard1 = static_cast<std::byte *>(*addr) - SANITIZER_PAGE_GUARD;
        ALLOCATOR_ASSERT(*guard1 != nullptr);
        *addr = *guard1;
    }
    else
    {
        *guard1 = nullptr;
    }
}

/// jemalloc: san_unguard_pages_impl
void sanitizerUnguardPagesImpl(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool left, bool right, bool remap)
{
    ALLOCATOR_ASSERT(left || right);
    /// Remove the inner boundary which no longer exists.
    if (remap)
    {
        ALLOCATOR_ASSERT(extent->state() == extent_state_active);
        extent_map->deregisterBoundary(thread_state, extent);
    }
    else
    {
        ALLOCATOR_ASSERT(extent->state() == extent_state_retained);
    }

    size_t size = extent->size();
    size_t size_with_guards = (left && right) ? sanitizerTwoSideGuardedSize(size) : sanitizerOneSideGuardedSize(size);

    void * guard1;
    void * guard2;
    void * addr;
    sanitizerFindUnguardedAddr(extent, &guard1, &guard2, &addr, size, left, right);

    extent_hooks->unguard(thread_state, guard1, guard2);

    /// Update the true addr and usable size of the extent.
    extent->setSize(size_with_guards);
    extent->setAddr(addr);
    extent->setGuarded(false);

    /// Then re-register the outer boundary including the guards, if requested.
    if (remap)
        extent_map->registerBoundary(thread_state, extent, SIZE_CLASS_NUM_SIZES, /* slab */ false);
}

/// jemalloc: san_stashed_corrupted
bool sanitizerStashedCorrupted(void * ptr, size_t size)
{
    if constexpr (sanitizerJunkPtrShouldSlow())
    {
        for (size_t i = 0; i < size; ++i)
            if (static_cast<char *>(ptr)[i] != char(use_after_free_detect_junk))
                return true;
        return false;
    }

    void * first;
    void * mid;
    void * last;
    sanitizerJunkPtrLocations(ptr, size, &first, &mid, &last);
    if (*static_cast<uintptr_t *>(first) != use_after_free_detect_junk || *static_cast<uintptr_t *>(mid) != use_after_free_detect_junk
        || *static_cast<uintptr_t *>(last) != use_after_free_detect_junk)
        return true;

    return false;
}

}

void sanitizerGuardPages(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool left, bool right, bool remap)
{
    ALLOCATOR_ASSERT(left || right);
    if (remap)
        extent_map->deregisterBoundary(thread_state, extent);

    size_t size_with_guards = extent->size();
    size_t usable_size
        = (left && right) ? sanitizerTwoSideUnguardedSize(size_with_guards) : sanitizerOneSideUnguardedSize(size_with_guards);

    void * guard1;
    void * guard2;
    void * addr;
    sanitizerFindGuardedAddr(extent, &guard1, &guard2, &addr, usable_size, left, right);

    ALLOCATOR_ASSERT(extent->state() == extent_state_active);
    extent_hooks->guard(thread_state, guard1, guard2);

    /// Update the guarded addr and usable size of the extent.
    extent->setSize(usable_size);
    extent->setAddr(addr);
    extent->setGuarded(true);

    if (remap)
        extent_map->registerBoundary(thread_state, extent, SIZE_CLASS_NUM_SIZES, /* slab */ false);
}

void sanitizerUnguardPages(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool left, bool right)
{
    sanitizerUnguardPagesImpl(thread_state, extent_hooks, extent, extent_map, left, right, /* remap */ true);
}

void sanitizerUnguardPagesPreDestroy(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map)
{
    extent_map->assertNotMapped(thread_state, extent);
    /// We don't want to touch the emap of about to be destroyed extents, as they have been unmapped upon eviction from
    /// the retained ecache. Also, we unguard the extents to the right, because retained extents only own their right
    /// guard page per `SanitizerBumpAlloc::alloc`'s logic.
    sanitizerUnguardPagesImpl(thread_state, extent_hooks, extent, extent_map, /* left */ false, /* right */ true, /* remap */ false);
}

void sanitizerCheckStashedPtrs(void ** ptrs, size_t num_stashed, size_t usable_size)
{
    /// Verify that the junk-filled and stashed pointers remain unchanged, to detect write-after-free.
    for (size_t n = 0; n < num_stashed; ++n)
    {
        void * stashed = ptrs[n];
        ALLOCATOR_ASSERT(stashed != nullptr);
        ALLOCATOR_ASSERT(
            !config::use_after_free_detection || (reinterpret_cast<uintptr_t>(stashed) & sanitizer_cache_bin_non_fast_mask) == 0);
        if (ALLOCATOR_UNLIKELY(sanitizerStashedCorrupted(stashed, usable_size)))
            safetyCheckFail("<jemalloc>: Write-after-free detected on deallocated pointer %p (size %zu).\n", stashed, usable_size);
    }
}

void threadStateSanitizerInit(ThreadState & thread_state)
{
    thread_state.sanitizer_extents_until_guard_small = options.sanitizer_guard_small;
    thread_state.sanitizer_extents_until_guard_large = options.sanitizer_guard_large;
}

void sanitizerInit(ssize_t log2_sanitizer_use_after_free_align)
{
    ALLOCATOR_ASSERT(log2_sanitizer_use_after_free_align == -1 || log2_sanitizer_use_after_free_align >= ssize_t(LOG2_PAGE));
    if (log2_sanitizer_use_after_free_align == -1)
    {
        sanitizer_cache_bin_non_fast_mask = uintptr_t(-1);
        return;
    }

    sanitizer_cache_bin_non_fast_mask = (uintptr_t(1) << log2_sanitizer_use_after_free_align) - 1;
}

/// --- SanitizerBumpAlloc --------------------------------------------------------------------------------------------------

Extent *
SanitizerBumpAlloc::alloc(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, size_t size, bool zero)
{
    ALLOCATOR_ASSERT(sanitizerBumpEnabled());

    Extent * to_destroy;
    size_t guarded_size = sanitizerOneSideGuardedSize(size);
    Extent * extent;

    mutex.lock(thread_state);

    if (current_region == nullptr || current_region->size() < guarded_size)
    {
        /// If the current region can't accommodate the allocation, try replacing it with a larger one and destroy the
        /// current one if the replacement succeeds.
        to_destroy = current_region;
        bool error = growLocked(thread_state, page_allocator, extent_hooks, guarded_size);
        if (error)
            goto label_error;
    }
    else
    {
        to_destroy = nullptr;
    }
    ALLOCATOR_ASSERT(guarded_size <= current_region->size());

    {
        size_t trail_size = current_region->size() - guarded_size;
        if (trail_size != 0)
        {
            Extent * current_region_trail = extentSplitWrapper(
                thread_state, page_allocator, extent_hooks, current_region, guarded_size, trail_size, /* holding_core_locks */ true);
            if (current_region_trail == nullptr)
                goto label_error;
            extent = current_region;
            current_region = current_region_trail;
        }
        else
        {
            extent = current_region;
            current_region = nullptr;
        }
    }

    mutex.unlock(thread_state);

    ALLOCATOR_ASSERT(!extent->guarded());
    ALLOCATOR_ASSERT(current_region == nullptr || !current_region->guarded());
    ALLOCATOR_ASSERT(to_destroy == nullptr || !to_destroy->guarded());

    if (to_destroy != nullptr)
        extentDestroyWrapper(thread_state, page_allocator, extent_hooks, to_destroy);

    sanitizerGuardPages(
        thread_state, extent_hooks, extent, page_allocator->extent_map, /* left */ false, /* right */ true, /* remap */ true);

    if (extentCommitZero(thread_state, extent_hooks, extent, /* commit */ true, zero, /* growing_retained */ false))
    {
        extentRecord(thread_state, page_allocator, extent_hooks, &page_allocator->extent_cache_retained, extent);
        return nullptr;
    }

    if constexpr (config::profiling)
        extentGrowthDumpAdd(thread_state, extent);

    return extent;

label_error:
    mutex.unlock(thread_state);
    return nullptr;
}

bool SanitizerBumpAlloc::growLocked(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, size_t size)
{
    mutex.assertOwner(thread_state);

    bool committed = false;
    bool zeroed = false;
    size_t alloc_size = size > SANITIZER_BUMP_ALLOC_RETAINED_ALLOC_SIZE ? size : SANITIZER_BUMP_ALLOC_RETAINED_ALLOC_SIZE;
    ALLOCATOR_ASSERT((alloc_size & PAGE_MASK) == 0);
    current_region = extentAllocWrapper(
        thread_state, page_allocator, extent_hooks, nullptr, alloc_size, PAGE, zeroed, &committed, /* growing_retained */ true);
    if (current_region == nullptr)
        return true;
    return false;
}

/// --- Safety checks -------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: safety_check_abort (static in `safety_check.c`)
constinit SafetyCheckAbortHook safety_check_abort = nullptr;

}

void safetyCheckFailSizedDealloc(bool current_dealloc, const void * ptr, size_t true_size, size_t input_size)
{
    const char * src = current_dealloc ? "the current pointer being freed" : "in thread cache, possibly from previous deallocations";
    /// jemalloc's `config_debug` (never set in ClickHouse).
    const char * suggest_debug_build = " --enable-debug or";

    safetyCheckFail(
        "<jemalloc>: size mismatch detected (true size %zu vs input size %zu), likely caused by application sized "
        "deallocation bugs (source address: %p, %s). Suggest building with%s address sanitizer for debugging. Abort.\n",
        true_size,
        input_size,
        ptr,
        src,
        suggest_debug_build);
}

void safetyCheckSetAbort(SafetyCheckAbortHook abort_function)
{
    safety_check_abort = abort_function;
}

/// In addition to `writeMessage`, also embed a hint in the abort function name, because there are cases where only
/// crash stack traces are logged. The name is kept verbatim from jemalloc for that reason.
/// jemalloc: safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug
ALLOCATOR_NOINLINE static void safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug(const char * buf)
{
    if (safety_check_abort == nullptr)
    {
        writeMessage(buf);
        abort();
    }
    else
    {
        safety_check_abort(buf);
    }
}

void safetyCheckFail(const char * format, ...)
{
    char buf[MALLOC_PRINTF_BUF_SIZE];

    va_list args;
    va_start(args, format);
    formatV(buf, MALLOC_PRINTF_BUF_SIZE, format, args);
    va_end(args);

    safety_check_detected_heap_corruption___run_address_sanitizer_build_to_debug(buf);
}

}
