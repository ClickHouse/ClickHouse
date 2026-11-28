#pragma once

/// Extent hooks: the interface between the page-level algorithms and the OS.
/// jemalloc: `ehooks.h`, `src/ehooks.c`.
///
/// `ExtentHooks` (= `ehooks_t`) keeps the arena index and the pointer to the public `extent_hooks_t` table. It is
/// `extent_hooks_default_extent_hooks` unless the application installed its own table with `arenas.create`,
/// `experimental.arenas_create_ext` or `arena.<i>.extent_hooks` (ClickHouse does: `JemallocNoDumpArenas`). For the
/// default table every operation calls the default implementation directly (no function pointers on the hot path);
/// the hooks of a custom table are called out of line (`extentHooksCustom*`), under the raw reentrancy guards.
///
/// DSS (`sbrk`) is dropped too: allocation always uses mmap, so the DSS branches of the default hooks reduce to the
/// non-DSS case; `SbrkPrecedence` remains only for reporting through mallctl (and for the side effects of a failed DSS
/// attempt, reproduced by `extentAllocWrapper`).

#include <allocator/Common.h>
#include <allocator/Pages.h>

#include <atomic>
#include <cstring>

extern "C" {
#include <stdbool.h>
#include <stddef.h>
#include <jemalloc/jemalloc_typedefs.h>
}

namespace jemalloc
{

class ThreadState;

/// The `dss` option / `arena.<i>.dss` (reporting only: sbrk is never used).
/// jemalloc: dss_prec_t (`extent_dss.h`)
enum class SbrkPrecedence : unsigned
{
    Disabled = 0,
    Primary = 1,
    Secondary = 2,
    Limit = 3,
};

/// jemalloc: DSS_PREC_DEFAULT
inline constexpr SbrkPrecedence SBRK_PRECEDENCE_DEFAULT = SbrkPrecedence::Secondary;
/// jemalloc: DSS_DEFAULT
inline constexpr const char * SBRK_DEFAULT = "secondary";
/// jemalloc: dss_prec_names
extern const char * const sbrk_precedence_names[];

/// The public table of the default hooks.
/// jemalloc: ehooks_default_extent_hooks
extern const extent_hooks_t extent_hooks_default_extent_hooks;

/// --- Default implementations (jemalloc: `ehooks_default_*_impl`) ---------------------------------------------------

/// If the caller specifies `!*zero`, it is still possible to receive zeroed memory, in which case `*zero` is toggled
/// to true.
/// jemalloc: ehooks_default_alloc_impl (with `extent_alloc_core`; the DSS branches are dropped)
void * extentHooksDefaultAllocImpl(
    ThreadState * thread_state, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_idx);

/// Returns true if the memory was not deallocated (always with `opt_retain`).
/// jemalloc: ehooks_default_dalloc_impl
bool extentHooksDefaultDeallocateImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_destroy_impl
void extentHooksDefaultDestroyImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_commit_impl
bool extentHooksDefaultCommitImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_decommit_impl
bool extentHooksDefaultDecommitImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_purge_lazy_impl
bool extentHooksDefaultPurgeLazyImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_purge_forced_impl
bool extentHooksDefaultPurgeForcedImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_split_impl
ALLOCATOR_ALWAYS_INLINE bool extentHooksDefaultSplitImpl()
{
    if constexpr (!config::maps_coalesce)
    {
        /// Without retain, only whole regions can be purged (required by MEM_RELEASE on Windows) -- therefore
        /// disallow splitting.
        return !options.retain;
    }
    return false;
}

/// For non-DSS cases --
/// a) W/o maps_coalesce, merge is not always allowed (Windows):
///   1) w/o retain, never merge (first branch below).
///   2) with retain, only merge extents from the same VirtualAlloc region (in which case MEM_DECOMMIT is utilized
///      for purging).
/// b) With maps_coalesce, it's always possible to merge.
///   1) w/o retain, always allow merge (only about dirty / muzzy).
///   2) with retain, to preserve the SN / first-fit, merge is still disallowed if b is a head extent, i.e. no merging
///      across different mmap regions.
/// a2) and b2) are implemented in `emap_try_acquire_edata_neighbor`.
/// The DSS check (`extent_dss_mergeable`) is always "mergeable" without DSS.
/// jemalloc: ehooks_default_merge_impl
ALLOCATOR_ALWAYS_INLINE bool extentHooksDefaultMergeImpl(ThreadState * /*tsdn*/, void * addr_a, void * addr_b)
{
    ALLOCATOR_ASSERT(addr_a < addr_b);
    (void)addr_a;
    (void)addr_b;
    if constexpr (!config::maps_coalesce)
    {
        if (!options.retain)
            return true;
    }
    /// NOTE: the `config_debug` check of the head states via the emap is not ported (the callers check it).
    return false;
}

/// By default, we try to zero out memory using OS-provided demand-zeroed pages. If the user has specifically
/// requested hugepages, though, we don't want to purge in the middle of a hugepage (which would break it up), so we
/// act conservatively and use memset.
/// jemalloc: ehooks_default_zero_impl
void extentHooksDefaultZeroImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_guard_impl
ALLOCATOR_ALWAYS_INLINE void extentHooksDefaultGuardImpl(void * guard1, void * guard2)
{
    pages::markGuards(guard1, guard2);
}

/// jemalloc: ehooks_default_unguard_impl
ALLOCATOR_ALWAYS_INLINE void extentHooksDefaultUnguardImpl(void * guard1, void * guard2)
{
    pages::unmarkGuards(guard1, guard2);
}

/// Some hooks are required to return zeroed memory in certain situations. In debug mode, we do some heuristic checks
/// that they did what they were supposed to.
/// jemalloc: ehooks_debug_zero_check
inline void extentHooksDebugZeroCheck(void * addr, size_t size)
{
    ALLOCATOR_ASSERT((reinterpret_cast<uintptr_t>(addr) & PAGE_MASK) == 0);
    ALLOCATOR_ASSERT((size & PAGE_MASK) == 0);
    ALLOCATOR_ASSERT(size > 0);
    if constexpr (config::debug)
    {
        /// Check the whole first page.
        const size_t * p = static_cast<const size_t *>(addr);
        for (size_t i = 0; i < PAGE / sizeof(size_t); ++i)
            ALLOCATOR_ASSERT(p[i] == 0);
        /// And 4 spots within.
        constexpr size_t num_checks = 4;
        static_assert(PAGE >= sizeof(size_t) * num_checks);
        for (size_t i = 0; i < num_checks; ++i)
            ALLOCATOR_ASSERT(p[i * (size / sizeof(size_t) / num_checks)] == 0);
    }
}

/// --- Hooks of a custom table (the non-default branches of jemalloc's `ehooks_*`) --------------------------------------
///
/// A hook that the table does not have fails (returns true), except `alloc`, which is mandatory, and `destroy`, which
/// then does nothing. The hooks are called under the raw reentrancy guards (`ehooks_pre_reentrancy`,
/// `ehooks_post_reentrancy`): the usual guards assert that the arena is not arena 0, which may have custom hooks too.

void * extentHooksCustomAlloc(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool * zero,
    bool * commit,
    unsigned arena_idx);
bool extentHooksCustomDeallocate(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, bool committed, unsigned arena_idx);
void extentHooksCustomDestroy(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, bool committed, unsigned arena_idx);
bool extentHooksCustomCommit(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx);
bool extentHooksCustomDecommit(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx);
bool extentHooksCustomPurgeLazy(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx);
bool extentHooksCustomPurgeForced(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx);
bool extentHooksCustomSplit(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * addr,
    size_t size,
    size_t size_a,
    size_t size_b,
    bool committed,
    unsigned arena_idx);
bool extentHooksCustomMerge(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * addr_a,
    size_t size_a,
    void * addr_b,
    size_t size_b,
    bool committed,
    unsigned arena_idx);

/// jemalloc: ehooks_t
class ExtentHooks
{
public:
    constexpr ExtentHooks() = default;

    ExtentHooks(const ExtentHooks &) = delete;
    ExtentHooks & operator=(const ExtentHooks &) = delete;

    /// jemalloc: ehooks_init
    void init(extent_hooks_t * extent_hooks_ptr, unsigned idx_)
    {
        /// All other hooks are optional; this one is not.
        ALLOCATOR_ASSERT(extent_hooks_ptr->alloc != nullptr);
        idx = idx_;
        setExtentHooksPtr(extent_hooks_ptr);
    }

    /// The user-visible id that goes with the hooks (that of the base they're a part of, the associated arena's index).
    /// jemalloc: ehooks_ind_get
    ALLOCATOR_ALWAYS_INLINE unsigned idxGet() const { return idx; }

    /// jemalloc: ehooks_set_extent_hooks_ptr
    ALLOCATOR_ALWAYS_INLINE void setExtentHooksPtr(extent_hooks_t * extent_hooks_ptr)
    {
        ptr.store(extent_hooks_ptr, std::memory_order_release);
    }

    /// jemalloc: ehooks_get_extent_hooks_ptr
    ALLOCATOR_ALWAYS_INLINE extent_hooks_t * getExtentHooksPtr() const { return ptr.load(std::memory_order_acquire); }

    /// jemalloc: ehooks_are_default
    ALLOCATOR_ALWAYS_INLINE bool areDefault() const { return getExtentHooksPtr() == &extent_hooks_default_extent_hooks; }

    /// In some cases, a caller needs to allocate resources before attempting to call a hook. If that hook is doomed
    /// to fail, this is wasteful. We therefore include some checks for such cases.
    /// jemalloc: ehooks_dalloc_will_fail
    ALLOCATOR_ALWAYS_INLINE bool deallocateWillFail() const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (extent_hooks_ptr == &extent_hooks_default_extent_hooks)
            return options.retain;
        return extent_hooks_ptr->dalloc == nullptr;
    }

    /// jemalloc: ehooks_split_will_fail
    ALLOCATOR_ALWAYS_INLINE bool splitWillFail() const { return getExtentHooksPtr()->split == nullptr; }

    /// jemalloc: ehooks_merge_will_fail
    ALLOCATOR_ALWAYS_INLINE bool mergeWillFail() const { return getExtentHooksPtr()->merge == nullptr; }

    /// Before the guard hooks are officially introduced, limit the use to the default hooks only.
    /// jemalloc: ehooks_guard_will_fail
    ALLOCATOR_ALWAYS_INLINE bool guardWillFail() const { return !areDefault(); }

    /// jemalloc: ehooks_alloc
    ALLOCATOR_ALWAYS_INLINE void *
    alloc(ThreadState * thread_state, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit) const
    {
        [[maybe_unused]] bool original_zero = *zero;
        void * result;
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            result = extentHooksDefaultAllocImpl(thread_state, new_addr, size, alignment, zero, commit, idxGet());
        else
            result = extentHooksCustomAlloc(thread_state, extent_hooks_ptr, new_addr, size, alignment, zero, commit, idxGet());
        ALLOCATOR_ASSERT(new_addr == nullptr || result == nullptr || new_addr == result);
        ALLOCATOR_ASSERT(!original_zero || *zero);
        if constexpr (config::debug)
        {
            if (*zero && result != nullptr)
                extentHooksDebugZeroCheck(result, size);
        }
        return result;
    }

    /// Returns true on error (the memory was not deallocated).
    /// jemalloc: ehooks_dalloc
    ALLOCATOR_ALWAYS_INLINE bool deallocate(ThreadState * thread_state, void * addr, size_t size, bool committed) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            return extentHooksDefaultDeallocateImpl(addr, size);
        return extentHooksCustomDeallocate(thread_state, extent_hooks_ptr, addr, size, committed, idxGet());
    }

    /// jemalloc: ehooks_destroy
    ALLOCATOR_ALWAYS_INLINE void destroy(ThreadState * thread_state, void * addr, size_t size, bool committed) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            extentHooksDefaultDestroyImpl(addr, size);
        else
            extentHooksCustomDestroy(thread_state, extent_hooks_ptr, addr, size, committed, idxGet());
    }

    /// Returns true on error.
    /// jemalloc: ehooks_commit
    ALLOCATOR_ALWAYS_INLINE bool commit(ThreadState * thread_state, void * addr, size_t size, size_t offset, size_t length) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        bool error;
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            error = extentHooksDefaultCommitImpl(addr, offset, length);
        else
            error = extentHooksCustomCommit(thread_state, extent_hooks_ptr, addr, size, offset, length, idxGet());
        if constexpr (config::debug)
        {
            if (!error)
                extentHooksDebugZeroCheck(addr, size);
        }
        return error;
    }

    /// Returns true on error.
    /// jemalloc: ehooks_decommit
    ALLOCATOR_ALWAYS_INLINE bool decommit(ThreadState * thread_state, void * addr, size_t size, size_t offset, size_t length) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            return extentHooksDefaultDecommitImpl(addr, offset, length);
        return extentHooksCustomDecommit(thread_state, extent_hooks_ptr, addr, size, offset, length, idxGet());
    }

    /// Returns true on error.
    /// jemalloc: ehooks_purge_lazy
    ALLOCATOR_ALWAYS_INLINE bool purgeLazy(ThreadState * thread_state, void * addr, size_t size, size_t offset, size_t length) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if constexpr (pages::can_purge_lazy)
        {
            if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
                return extentHooksDefaultPurgeLazyImpl(addr, offset, length);
        }
        return extentHooksCustomPurgeLazy(thread_state, extent_hooks_ptr, addr, size, offset, length, idxGet());
    }

    /// Returns true on error. (`purge_forced` is required to zero, but it is not checked even in debug mode: that
    /// would touch the pages.)
    /// jemalloc: ehooks_purge_forced
    ALLOCATOR_ALWAYS_INLINE bool purgeForced(ThreadState * thread_state, void * addr, size_t size, size_t offset, size_t length) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if constexpr (pages::can_purge_forced)
        {
            if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
                return extentHooksDefaultPurgeForcedImpl(addr, offset, length);
        }
        return extentHooksCustomPurgeForced(thread_state, extent_hooks_ptr, addr, size, offset, length, idxGet());
    }

    /// Returns true on error.
    /// jemalloc: ehooks_split
    ALLOCATOR_ALWAYS_INLINE bool
    split(ThreadState * thread_state, void * addr, size_t size, size_t size_a, size_t size_b, bool committed) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            return extentHooksDefaultSplitImpl();
        return extentHooksCustomSplit(thread_state, extent_hooks_ptr, addr, size, size_a, size_b, committed, idxGet());
    }

    /// Returns true on error (the extents must not be merged).
    /// jemalloc: ehooks_merge
    ALLOCATOR_ALWAYS_INLINE bool
    merge(ThreadState * thread_state, void * addr_a, size_t size_a, void * addr_b, size_t size_b, bool committed) const
    {
        extent_hooks_t * extent_hooks_ptr = getExtentHooksPtr();
        if (ALLOCATOR_LIKELY(extent_hooks_ptr == &extent_hooks_default_extent_hooks))
            return extentHooksDefaultMergeImpl(thread_state, addr_a, addr_b);
        return extentHooksCustomMerge(thread_state, extent_hooks_ptr, addr_a, size_a, addr_b, size_b, committed, idxGet());
    }

    /// jemalloc: ehooks_zero
    ALLOCATOR_ALWAYS_INLINE void zero(ThreadState * /*tsdn*/, void * addr, size_t size) const
    {
        if (ALLOCATOR_LIKELY(areDefault()))
        {
            extentHooksDefaultZeroImpl(addr, size);
        }
        else
        {
            /// It would be correct to try using the user-provided purge hooks (since they are required to have zeroed
            /// the extent if they indicate success), but we don't necessarily know their cost. We'll be conservative
            /// and use memset.
            memset(addr, 0, size);
        }
    }

    /// Returns true on error (always for custom hooks, which have no guard hook).
    /// jemalloc: ehooks_guard
    ALLOCATOR_ALWAYS_INLINE bool guard(ThreadState * /*tsdn*/, void * guard1, void * guard2) const
    {
        if (!areDefault())
            return true;
        extentHooksDefaultGuardImpl(guard1, guard2);
        return false;
    }

    /// Returns true on error (always for custom hooks, which have no guard hook).
    /// jemalloc: ehooks_unguard
    ALLOCATOR_ALWAYS_INLINE bool unguard(ThreadState * /*tsdn*/, void * guard1, void * guard2) const
    {
        if (!areDefault())
            return true;
        extentHooksDefaultUnguardImpl(guard1, guard2);
        return false;
    }

private:
    unsigned idx = 0;
    /// Logically an `extent_hooks_t *`.
    std::atomic<extent_hooks_t *> ptr{nullptr};
};

static_assert(sizeof(ExtentHooks) == 16, "ehooks_t is 16 bytes");

}
