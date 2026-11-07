#pragma once

/// Guard pages around extents, use-after-free detection helpers, the guarded-slab bump allocator, and the safety
/// check failure path. jemalloc: `san.h`, `src/san.c`, `san_bump.h`, `src/san_bump.c`, `safety_check.h`,
/// `src/safety_check.c`.
///
/// Guard pages (`sanitizer_guard_small`, `sanitizer_guard_large`) are honored in every build; the sampled write-after-free
/// detection (`log2_sanitizer_use_after_free_align`) only with `config::use_after_free_detection` (`JEMALLOC_UAF_DETECTION`).

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/ExtentHooks.h>
#include <allocator/ExtentMap.h>
#include <allocator/Mutex.h>
#include <allocator/Options.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc
{

class PageAllocator;

/// --- Guard pages (san.h) ------------------------------------------------------------------------------------------

/// jemalloc: SAN_PAGE_GUARD, SAN_PAGE_GUARDS_SIZE
inline constexpr size_t SANITIZER_PAGE_GUARD = PAGE;
inline constexpr size_t SANITIZER_PAGE_GUARDS_SIZE = SANITIZER_PAGE_GUARD * 2;

/// jemalloc: SAN_GUARD_LARGE_EVERY_N_EXTENTS_DEFAULT, SAN_GUARD_SMALL_EVERY_N_EXTENTS_DEFAULT (0 means never)
inline constexpr size_t SANITIZER_GUARD_LARGE_EVERY_NUM_EXTENTS_DEFAULT = 0;
inline constexpr size_t SANITIZER_GUARD_SMALL_EVERY_NUM_EXTENTS_DEFAULT = 0;

/// jemalloc: SAN_LG_UAF_ALIGN_DEFAULT (-1 means never check for use-after-free)
inline constexpr ssize_t SANITIZER_LOG2_USE_AFTER_FREE_ALIGN_DEFAULT = -1;
/// jemalloc: SAN_CACHE_BIN_NONFAST_MASK_DEFAULT
inline constexpr uintptr_t SANITIZER_CACHE_BIN_NON_FAST_MASK_DEFAULT = uintptr_t(-1);

/// The junk pattern written into sampled (stashed) freed regions.
/// jemalloc: uaf_detect_junk
inline constexpr uintptr_t use_after_free_detect_junk = uintptr_t(0x5b5b5b5b5b5b5b5bULL);

/// Initialized in `sanitizerInit`. When disabled, the mask is (uintptr_t)-1 so that the nonfast-aligned check always fails.
/// jemalloc: san_cache_bin_nonfast_mask
extern constinit uintptr_t sanitizer_cache_bin_non_fast_mask;

/// jemalloc: san_guard_pages
void sanitizerGuardPages(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool left, bool right, bool remap);

/// jemalloc: san_unguard_pages
void sanitizerUnguardPages(
    ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool left, bool right);

/// Unguard the extent, but don't modify emap boundaries. Must be called on an extent that has been erased from the
/// emap and shouldn't be placed back.
/// jemalloc: san_unguard_pages_pre_destroy
void sanitizerUnguardPagesPreDestroy(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map);

/// Verify that the junk-filled and stashed pointers remain unchanged, to detect write-after-free.
/// jemalloc: san_check_stashed_ptrs
void sanitizerCheckStashedPtrs(void ** ptrs, size_t num_stashed, size_t usable_size);

/// jemalloc: tsd_san_init
void threadStateSanitizerInit(ThreadState & thread_state);

/// jemalloc: san_init
void sanitizerInit(ssize_t log2_sanitizer_use_after_free_align);

/// jemalloc: san_guard_pages_two_sided
ALLOCATOR_ALWAYS_INLINE void
sanitizerGuardPagesTwoSided(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map, bool remap)
{
    sanitizerGuardPages(thread_state, extent_hooks, extent, extent_map, true, true, remap);
}

/// jemalloc: san_unguard_pages_two_sided
ALLOCATOR_ALWAYS_INLINE void
sanitizerUnguardPagesTwoSided(ThreadState * thread_state, ExtentHooks * extent_hooks, Extent * extent, ExtentMap * extent_map)
{
    sanitizerUnguardPages(thread_state, extent_hooks, extent, extent_map, true, true);
}

/// jemalloc: san_two_side_unguarded_sz
ALLOCATOR_ALWAYS_INLINE size_t sanitizerTwoSideUnguardedSize(size_t size)
{
    ALLOCATOR_ASSERT(size % PAGE == 0);
    ALLOCATOR_ASSERT(size >= SANITIZER_PAGE_GUARDS_SIZE);
    return size - SANITIZER_PAGE_GUARDS_SIZE;
}

/// jemalloc: san_two_side_guarded_sz
ALLOCATOR_ALWAYS_INLINE size_t sanitizerTwoSideGuardedSize(size_t size)
{
    ALLOCATOR_ASSERT(size % PAGE == 0);
    return size + SANITIZER_PAGE_GUARDS_SIZE;
}

/// jemalloc: san_one_side_unguarded_sz
ALLOCATOR_ALWAYS_INLINE size_t sanitizerOneSideUnguardedSize(size_t size)
{
    ALLOCATOR_ASSERT(size % PAGE == 0);
    ALLOCATOR_ASSERT(size >= SANITIZER_PAGE_GUARD);
    return size - SANITIZER_PAGE_GUARD;
}

/// jemalloc: san_one_side_guarded_sz
ALLOCATOR_ALWAYS_INLINE size_t sanitizerOneSideGuardedSize(size_t size)
{
    ALLOCATOR_ASSERT(size % PAGE == 0);
    return size + SANITIZER_PAGE_GUARD;
}

/// jemalloc: san_guard_enabled
ALLOCATOR_ALWAYS_INLINE bool sanitizerGuardEnabled()
{
    return options.sanitizer_guard_large != 0 || options.sanitizer_guard_small != 0;
}

/// Counts large extent allocations of this thread; true for every `sanitizer_guard_large`-th eligible one.
/// jemalloc: san_large_extent_decide_guard
ALLOCATOR_ALWAYS_INLINE bool
sanitizerLargeExtentDecideGuard(ThreadState * thread_state_ptr, const ExtentHooks * extent_hooks, size_t size, size_t alignment)
{
    if (options.sanitizer_guard_large == 0 || extent_hooks->guardWillFail() || thread_state_ptr == nullptr)
        return false;

    ThreadState & thread_state = *thread_state_ptr;
    uint64_t n = thread_state.sanitizer_extents_until_guard_large;
    ALLOCATOR_ASSERT(n >= 1);
    if (n > 1)
    {
        /// Subtract conditionally because the guard may not happen due to alignment or size restriction below.
        thread_state.sanitizer_extents_until_guard_large = n - 1;
    }

    if (n == 1 && (alignment <= PAGE) && (sanitizerTwoSideGuardedSize(size) <= SIZE_CLASS_LARGE_MAX_CLASS))
    {
        thread_state.sanitizer_extents_until_guard_large = options.sanitizer_guard_large;
        return true;
    }
    else
    {
        ALLOCATOR_ASSERT(thread_state.sanitizer_extents_until_guard_large >= 1);
        return false;
    }
}

/// Counts slab allocations of this thread; true for every `sanitizer_guard_small`-th one.
/// jemalloc: san_slab_extent_decide_guard
ALLOCATOR_ALWAYS_INLINE bool sanitizerSlabExtentDecideGuard(ThreadState * thread_state_ptr, const ExtentHooks * extent_hooks)
{
    if (options.sanitizer_guard_small == 0 || extent_hooks->guardWillFail() || thread_state_ptr == nullptr)
        return false;

    ThreadState & thread_state = *thread_state_ptr;
    uint64_t n = thread_state.sanitizer_extents_until_guard_small;
    ALLOCATOR_ASSERT(n >= 1);
    if (n == 1)
    {
        thread_state.sanitizer_extents_until_guard_small = options.sanitizer_guard_small;
        return true;
    }
    else
    {
        thread_state.sanitizer_extents_until_guard_small = n - 1;
        ALLOCATOR_ASSERT(thread_state.sanitizer_extents_until_guard_small >= 1);
        return false;
    }
}

/// --- Use-after-free detection (san.h) ------------------------------------------------------------------------------

/// The three words written by the fast junking: the first, the middle (pointer-aligned) and the last one.
/// jemalloc: san_junk_ptr_locations
ALLOCATOR_ALWAYS_INLINE void sanitizerJunkPtrLocations(void * ptr, size_t usable_size, void ** first, void ** mid, void ** last)
{
    size_t ptr_size = sizeof(void *);

    *first = ptr;

    *mid = static_cast<std::byte *>(ptr) + ((usable_size >> 1) & ~(ptr_size - 1));
    ALLOCATOR_ASSERT(*first != *mid || usable_size == ptr_size);
    ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(*first) <= reinterpret_cast<uintptr_t>(*mid));

    /// When usize > 32K, the gap between the requested size and usize might be greater than 4K -- this means the last
    /// write may access a likely-untouched page (default settings with 4K pages). However by default the tcache only
    /// goes up to the 32K size class, and is usually tuned lower instead of higher, which makes it less of a concern.
    *last = static_cast<std::byte *>(ptr) + usable_size - sizeof(use_after_free_detect_junk);
    ALLOCATOR_ASSERT(*first != *last || usable_size == ptr_size);
    ALLOCATOR_ASSERT(*mid != *last || usable_size <= ptr_size * 2);
    ALLOCATOR_ASSERT(reinterpret_cast<uintptr_t>(*mid) <= reinterpret_cast<uintptr_t>(*last));
}

/// The latter condition (pointer size greater than the min size class) is not expected -- fall back to the slow path
/// for simplicity. jemalloc's `config_debug` is never set in ClickHouse, so `ALLOCATOR_DEBUG` does not affect this.
/// jemalloc: san_junk_ptr_should_slow
ALLOCATOR_ALWAYS_INLINE constexpr bool sanitizerJunkPtrShouldSlow()
{
    return LG_SIZEOF_PTR > unsigned(SIZE_CLASS_LOG2_TINY_MIN);
}

/// jemalloc: san_junk_ptr
ALLOCATOR_ALWAYS_INLINE void sanitizerJunkPtr(void * ptr, size_t usable_size)
{
    if constexpr (sanitizerJunkPtrShouldSlow())
    {
        std::memset(ptr, char(use_after_free_detect_junk), usable_size);
        return;
    }

    void * first;
    void * mid;
    void * last;
    sanitizerJunkPtrLocations(ptr, usable_size, &first, &mid, &last);
    *static_cast<uintptr_t *>(first) = use_after_free_detect_junk;
    *static_cast<uintptr_t *>(mid) = use_after_free_detect_junk;
    *static_cast<uintptr_t *>(last) = use_after_free_detect_junk;
}

/// jemalloc: san_uaf_detection_enabled
ALLOCATOR_ALWAYS_INLINE bool sanitizerUseAfterFreeDetectionEnabled()
{
    bool result = config::use_after_free_detection && (options.log2_sanitizer_use_after_free_align != -1);
    if (config::use_after_free_detection && result)
        ALLOCATOR_ASSERT(sanitizer_cache_bin_non_fast_mask == (uintptr_t(1) << options.log2_sanitizer_use_after_free_align) - 1);
    return result;
}

/// --- Bump allocator for guarded slabs (san_bump.h) -----------------------------------------------------------------

/// jemalloc: SBA_RETAINED_ALLOC_SIZE
inline constexpr size_t SANITIZER_BUMP_ALLOC_RETAINED_ALLOC_SIZE = size_t(4) << 20;

/// The allocator is enabled only when it's possible to break up a mapping and unmap a part of it (`maps_coalesce`).
/// This is needed to ensure the arena destruction process can destroy all retained guarded extents one by one and to
/// unmap a trailing part of a retained guarded region when it's too small to fit a pending allocation. `retain` is
/// required, because this allocator retains a large virtual memory mapping and returns smaller parts of it.
/// jemalloc: san_bump_enabled
ALLOCATOR_ALWAYS_INLINE bool sanitizerBumpEnabled()
{
    return config::maps_coalesce && options.retain;
}

/// Allocates frequently reused guarded extents (slabs) with a guard page on the right side only, out of a 4 MiB region.
/// jemalloc: san_bump_alloc_t
class SanitizerBumpAlloc
{
public:
    constexpr SanitizerBumpAlloc() = default;

    SanitizerBumpAlloc(const SanitizerBumpAlloc &) = delete;
    SanitizerBumpAlloc & operator=(const SanitizerBumpAlloc &) = delete;

    /// Returns true on error.
    /// jemalloc: san_bump_alloc_init
    bool init()
    {
        if (mutex.init("sanitizer_bump_allocator", MutexRank::SANITIZER_BUMP_ALLOC, MutexLockOrder::RankExclusive))
            return true;
        current_region = nullptr;
        return false;
    }

    /// jemalloc: san_bump_alloc
    Extent * alloc(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, size_t size, bool zero);

    /// "sanitizer_bump_allocator", `MutexRank::SANITIZER_BUMP_ALLOC`.
    Mutex mutex;
    Extent * current_region = nullptr;

private:
    /// Returns true on error.
    /// jemalloc: san_bump_grow_locked
    bool growLocked(ThreadState * thread_state, PageAllocator * page_allocator, ExtentHooks * extent_hooks, size_t size);
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(SanitizerBumpAlloc) == 128, "san_bump_alloc_t size (aarch64 glibc)");
#endif

/// --- Safety checks (safety_check.h) ------------------------------------------------------------------------------

/// jemalloc: SAFETY_CHECK_DOUBLE_FREE_MAX_SCAN_DEFAULT
inline constexpr unsigned SAFETY_CHECK_DOUBLE_FREE_MAX_SCAN_DEFAULT = 32;

/// jemalloc: safety_check_abort_hook_t
using SafetyCheckAbortHook = void (*)(const char * message);

/// jemalloc: safety_check_fail_sized_dealloc
void safetyCheckFailSizedDealloc(bool current_dealloc, const void * ptr, size_t true_size, size_t input_size);

/// Formats the message (`Format.h` rules, 4096-byte buffer) and passes it to the abort hook, or writes it with
/// `writeMessage` and aborts if no hook is set.
/// jemalloc: safety_check_fail
void safetyCheckFail(const char * format, ...) ALLOCATOR_FORMAT_PRINTF(1, 2);

/// Can be set to null for the default (`experimental.hooks.safety_check_abort`).
/// jemalloc: safety_check_set_abort
void safetyCheckSetAbort(SafetyCheckAbortHook abort_function);

/// The redzones after sampled small allocations (`safety_check_set_redzone`, `safety_check_verify_redzone`,
/// `compute_redzone_end`) exist only with `config_opt_safety_checks`, which is never enabled in ClickHouse
/// (`config::option_safety_checks` is false), so they are not ported.

}
