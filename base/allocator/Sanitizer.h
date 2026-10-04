#pragma once

/// Guard pages around extents, use-after-free detection helpers, the guarded-slab bump allocator, and the safety
/// check failure path. jemalloc: `san.h`, `src/san.c`, `san_bump.h`, `src/san_bump.c`, `safety_check.h`,
/// `src/safety_check.c`.
///
/// Guard pages (`san_guard_small`, `san_guard_large`) are honored in every build; the sampled write-after-free
/// detection (`lg_san_uaf_align`) only with `config::uaf_detection` (`JEMALLOC_UAF_DETECTION`).

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
inline constexpr size_t SAN_PAGE_GUARD = PAGE;
inline constexpr size_t SAN_PAGE_GUARDS_SIZE = SAN_PAGE_GUARD * 2;

/// jemalloc: SAN_GUARD_LARGE_EVERY_N_EXTENTS_DEFAULT, SAN_GUARD_SMALL_EVERY_N_EXTENTS_DEFAULT (0 means never)
inline constexpr size_t SAN_GUARD_LARGE_EVERY_N_EXTENTS_DEFAULT = 0;
inline constexpr size_t SAN_GUARD_SMALL_EVERY_N_EXTENTS_DEFAULT = 0;

/// jemalloc: SAN_LG_UAF_ALIGN_DEFAULT (-1 means never check for use-after-free)
inline constexpr ssize_t SAN_LG_UAF_ALIGN_DEFAULT = -1;
/// jemalloc: SAN_CACHE_BIN_NONFAST_MASK_DEFAULT
inline constexpr uintptr_t SAN_CACHE_BIN_NONFAST_MASK_DEFAULT = uintptr_t(-1);

/// The junk pattern written into sampled (stashed) freed regions.
/// jemalloc: uaf_detect_junk
inline constexpr uintptr_t uaf_detect_junk = uintptr_t(0x5b5b5b5b5b5b5b5bULL);

/// Initialized in `sanInit`. When disabled, the mask is (uintptr_t)-1 so that the nonfast-aligned check always fails.
/// jemalloc: san_cache_bin_nonfast_mask
extern constinit uintptr_t san_cache_bin_nonfast_mask;

/// jemalloc: san_guard_pages
void sanGuardPages(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool left, bool right, bool remap);

/// jemalloc: san_unguard_pages
void sanUnguardPages(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool left, bool right);

/// Unguard the extent, but don't modify emap boundaries. Must be called on an extent that has been erased from the
/// emap and shouldn't be placed back.
/// jemalloc: san_unguard_pages_pre_destroy
void sanUnguardPagesPreDestroy(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap);

/// Verify that the junk-filled and stashed pointers remain unchanged, to detect write-after-free.
/// jemalloc: san_check_stashed_ptrs
void sanCheckStashedPtrs(void ** ptrs, size_t nstashed, size_t usize);

/// jemalloc: tsd_san_init
void tsdSanInit(ThreadState & tsd);

/// jemalloc: san_init
void sanInit(ssize_t lg_san_uaf_align);

/// jemalloc: san_guard_pages_two_sided
JE_ALWAYS_INLINE void sanGuardPagesTwoSided(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap, bool remap)
{
    sanGuardPages(tsdn, ehooks, edata, emap, true, true, remap);
}

/// jemalloc: san_unguard_pages_two_sided
JE_ALWAYS_INLINE void sanUnguardPagesTwoSided(ThreadState * tsdn, ExtentHooks * ehooks, Extent * edata, ExtentMap * emap)
{
    sanUnguardPages(tsdn, ehooks, edata, emap, true, true);
}

/// jemalloc: san_two_side_unguarded_sz
JE_ALWAYS_INLINE size_t sanTwoSideUnguardedSize(size_t size)
{
    JE_ASSERT(size % PAGE == 0);
    JE_ASSERT(size >= SAN_PAGE_GUARDS_SIZE);
    return size - SAN_PAGE_GUARDS_SIZE;
}

/// jemalloc: san_two_side_guarded_sz
JE_ALWAYS_INLINE size_t sanTwoSideGuardedSize(size_t size)
{
    JE_ASSERT(size % PAGE == 0);
    return size + SAN_PAGE_GUARDS_SIZE;
}

/// jemalloc: san_one_side_unguarded_sz
JE_ALWAYS_INLINE size_t sanOneSideUnguardedSize(size_t size)
{
    JE_ASSERT(size % PAGE == 0);
    JE_ASSERT(size >= SAN_PAGE_GUARD);
    return size - SAN_PAGE_GUARD;
}

/// jemalloc: san_one_side_guarded_sz
JE_ALWAYS_INLINE size_t sanOneSideGuardedSize(size_t size)
{
    JE_ASSERT(size % PAGE == 0);
    return size + SAN_PAGE_GUARD;
}

/// jemalloc: san_guard_enabled
JE_ALWAYS_INLINE bool sanGuardEnabled()
{
    return opt.san_guard_large != 0 || opt.san_guard_small != 0;
}

/// Counts large extent allocations of this thread; true for every `san_guard_large`-th eligible one.
/// jemalloc: san_large_extent_decide_guard
JE_ALWAYS_INLINE bool sanLargeExtentDecideGuard(ThreadState * tsdn, const ExtentHooks * ehooks, size_t size, size_t alignment)
{
    if (opt.san_guard_large == 0 || ehooks->guardWillFail() || tsdn == nullptr)
        return false;

    ThreadState & tsd = *tsdn;
    uint64_t n = tsd.san_extents_until_guard_large;
    JE_ASSERT(n >= 1);
    if (n > 1)
    {
        /// Subtract conditionally because the guard may not happen due to alignment or size restriction below.
        tsd.san_extents_until_guard_large = n - 1;
    }

    if (n == 1 && (alignment <= PAGE) && (sanTwoSideGuardedSize(size) <= SC_LARGE_MAXCLASS))
    {
        tsd.san_extents_until_guard_large = opt.san_guard_large;
        return true;
    }
    else
    {
        JE_ASSERT(tsd.san_extents_until_guard_large >= 1);
        return false;
    }
}

/// Counts slab allocations of this thread; true for every `san_guard_small`-th one.
/// jemalloc: san_slab_extent_decide_guard
JE_ALWAYS_INLINE bool sanSlabExtentDecideGuard(ThreadState * tsdn, const ExtentHooks * ehooks)
{
    if (opt.san_guard_small == 0 || ehooks->guardWillFail() || tsdn == nullptr)
        return false;

    ThreadState & tsd = *tsdn;
    uint64_t n = tsd.san_extents_until_guard_small;
    JE_ASSERT(n >= 1);
    if (n == 1)
    {
        tsd.san_extents_until_guard_small = opt.san_guard_small;
        return true;
    }
    else
    {
        tsd.san_extents_until_guard_small = n - 1;
        JE_ASSERT(tsd.san_extents_until_guard_small >= 1);
        return false;
    }
}

/// --- Use-after-free detection (san.h) ------------------------------------------------------------------------------

/// The three words written by the fast junking: the first, the middle (pointer-aligned) and the last one.
/// jemalloc: san_junk_ptr_locations
JE_ALWAYS_INLINE void sanJunkPtrLocations(void * ptr, size_t usize, void ** first, void ** mid, void ** last)
{
    size_t ptr_sz = sizeof(void *);

    *first = ptr;

    *mid = static_cast<std::byte *>(ptr) + ((usize >> 1) & ~(ptr_sz - 1));
    JE_ASSERT(*first != *mid || usize == ptr_sz);
    JE_ASSERT(reinterpret_cast<uintptr_t>(*first) <= reinterpret_cast<uintptr_t>(*mid));

    /// When usize > 32K, the gap between the requested size and usize might be greater than 4K -- this means the last
    /// write may access a likely-untouched page (default settings with 4K pages). However by default the tcache only
    /// goes up to the 32K size class, and is usually tuned lower instead of higher, which makes it less of a concern.
    *last = static_cast<std::byte *>(ptr) + usize - sizeof(uaf_detect_junk);
    JE_ASSERT(*first != *last || usize == ptr_sz);
    JE_ASSERT(*mid != *last || usize <= ptr_sz * 2);
    JE_ASSERT(reinterpret_cast<uintptr_t>(*mid) <= reinterpret_cast<uintptr_t>(*last));
}

/// The latter condition (pointer size greater than the min size class) is not expected -- fall back to the slow path
/// for simplicity. jemalloc's `config_debug` is never set in ClickHouse, so `ALLOCATOR_DEBUG` does not affect this.
/// jemalloc: san_junk_ptr_should_slow
JE_ALWAYS_INLINE constexpr bool sanJunkPtrShouldSlow()
{
    return LG_SIZEOF_PTR > unsigned(SC_LG_TINY_MIN);
}

/// jemalloc: san_junk_ptr
JE_ALWAYS_INLINE void sanJunkPtr(void * ptr, size_t usize)
{
    if constexpr (sanJunkPtrShouldSlow())
    {
        std::memset(ptr, char(uaf_detect_junk), usize);
        return;
    }

    void * first;
    void * mid;
    void * last;
    sanJunkPtrLocations(ptr, usize, &first, &mid, &last);
    *static_cast<uintptr_t *>(first) = uaf_detect_junk;
    *static_cast<uintptr_t *>(mid) = uaf_detect_junk;
    *static_cast<uintptr_t *>(last) = uaf_detect_junk;
}

/// jemalloc: san_uaf_detection_enabled
JE_ALWAYS_INLINE bool sanUafDetectionEnabled()
{
    bool ret = config::uaf_detection && (opt.lg_san_uaf_align != -1);
    if (config::uaf_detection && ret)
        JE_ASSERT(san_cache_bin_nonfast_mask == (uintptr_t(1) << opt.lg_san_uaf_align) - 1);
    return ret;
}

/// --- Bump allocator for guarded slabs (san_bump.h) -----------------------------------------------------------------

/// jemalloc: SBA_RETAINED_ALLOC_SIZE
inline constexpr size_t SBA_RETAINED_ALLOC_SIZE = size_t(4) << 20;

/// The allocator is enabled only when it's possible to break up a mapping and unmap a part of it (`maps_coalesce`).
/// This is needed to ensure the arena destruction process can destroy all retained guarded extents one by one and to
/// unmap a trailing part of a retained guarded region when it's too small to fit a pending allocation. `retain` is
/// required, because this allocator retains a large virtual memory mapping and returns smaller parts of it.
/// jemalloc: san_bump_enabled
JE_ALWAYS_INLINE bool sanBumpEnabled()
{
    return config::maps_coalesce && opt.retain;
}

/// Allocates frequently reused guarded extents (slabs) with a guard page on the right side only, out of a 4 MiB region.
/// jemalloc: san_bump_alloc_t
class SanBumpAlloc
{
public:
    constexpr SanBumpAlloc() = default;

    SanBumpAlloc(const SanBumpAlloc &) = delete;
    SanBumpAlloc & operator=(const SanBumpAlloc &) = delete;

    /// Returns true on error.
    /// jemalloc: san_bump_alloc_init
    bool init()
    {
        if (mtx.init("sanitizer_bump_allocator", MutexRank::SAN_BUMP_ALLOC, MutexLockOrder::RankExclusive))
            return true;
        curr_reg = nullptr;
        return false;
    }

    /// jemalloc: san_bump_alloc
    Extent * alloc(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, size_t size, bool zero);

    /// "sanitizer_bump_allocator", `MutexRank::SAN_BUMP_ALLOC`.
    Mutex mtx;
    Extent * curr_reg = nullptr;

private:
    /// Returns true on error.
    /// jemalloc: san_bump_grow_locked
    bool growLocked(ThreadState * tsdn, PageAllocator * pac, ExtentHooks * ehooks, size_t size);
};

#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(SanBumpAlloc) == 128, "san_bump_alloc_t size (aarch64 glibc)");
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
void safetyCheckFail(const char * format, ...) JE_FORMAT_PRINTF(1, 2);

/// Can be set to null for the default (`experimental.hooks.safety_check_abort`).
/// jemalloc: safety_check_set_abort
void safetyCheckSetAbort(SafetyCheckAbortHook abort_fn);

/// The redzones after sampled small allocations (`safety_check_set_redzone`, `safety_check_verify_redzone`,
/// `compute_redzone_end`) exist only with `config_opt_safety_checks`, which is never enabled in ClickHouse
/// (`config::opt_safety_checks` is false), so they are not ported.

}
