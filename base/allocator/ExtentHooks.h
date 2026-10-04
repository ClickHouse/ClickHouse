#pragma once

/// Extent hooks: the interface between the page-level algorithms and the OS.
/// jemalloc: `ehooks.h`, `src/ehooks.c`.
///
/// Only the default hooks are implemented (custom extent hooks are a dropped feature: ClickHouse never installs them).
/// `ExtentHooks` (= `ehooks_t`) keeps the arena index and the pointer to the public `extent_hooks_t` table, which is
/// always `ehooks_default_extent_hooks` (its address is what `arena.<i>.extent_hooks` returns). All operations call
/// the default implementations directly (no function pointers on the hot path).
///
/// DSS (`sbrk`) is dropped too: allocation always uses mmap, so the DSS branches of the default hooks reduce to the
/// non-DSS case; `DssPrec` remains only for reporting through mallctl (and for the side effects of a failed DSS
/// attempt, reproduced by `extentAllocWrapper`).

#include <allocator/Common.h>
#include <allocator/Pages.h>

#include <atomic>
#include <cstring>

extern "C"
{
#include <stdbool.h>
#include <stddef.h>
#include <jemalloc/jemalloc_typedefs.h>
}

namespace jemalloc
{

class ThreadState;

/// The `dss` option / `arena.<i>.dss` (reporting only: sbrk is never used).
/// jemalloc: dss_prec_t (`extent_dss.h`)
enum class DssPrec : unsigned
{
    Disabled = 0,
    Primary = 1,
    Secondary = 2,
    Limit = 3,
};

/// jemalloc: DSS_PREC_DEFAULT
inline constexpr DssPrec DSS_PREC_DEFAULT = DssPrec::Secondary;
/// jemalloc: DSS_DEFAULT
inline constexpr const char * DSS_DEFAULT = "secondary";
/// jemalloc: dss_prec_names
extern const char * const dss_prec_names[];

/// The public table of the default hooks.
/// jemalloc: ehooks_default_extent_hooks
extern const extent_hooks_t ehooks_default_extent_hooks;

/// --- Default implementations (jemalloc: `ehooks_default_*_impl`) ---------------------------------------------------

/// If the caller specifies `!*zero`, it is still possible to receive zeroed memory, in which case `*zero` is toggled
/// to true.
/// jemalloc: ehooks_default_alloc_impl (with `extent_alloc_core`; the DSS branches are dropped)
void * ehooksDefaultAllocImpl(ThreadState * tsdn, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind);

/// Returns true if the memory was not deallocated (always with `opt_retain`).
/// jemalloc: ehooks_default_dalloc_impl
bool ehooksDefaultDallocImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_destroy_impl
void ehooksDefaultDestroyImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_commit_impl
bool ehooksDefaultCommitImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_decommit_impl
bool ehooksDefaultDecommitImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_purge_lazy_impl
bool ehooksDefaultPurgeLazyImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_purge_forced_impl
bool ehooksDefaultPurgeForcedImpl(void * addr, size_t offset, size_t length);

/// jemalloc: ehooks_default_split_impl
JE_ALWAYS_INLINE bool ehooksDefaultSplitImpl()
{
    if constexpr (!config::maps_coalesce)
    {
        /// Without retain, only whole regions can be purged (required by MEM_RELEASE on Windows) -- therefore
        /// disallow splitting.
        return !opt.retain;
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
JE_ALWAYS_INLINE bool ehooksDefaultMergeImpl(ThreadState * /*tsdn*/, void * addr_a, void * addr_b)
{
    JE_ASSERT(addr_a < addr_b);
    (void)addr_a;
    (void)addr_b;
    if constexpr (!config::maps_coalesce)
    {
        if (!opt.retain)
            return true;
    }
    /// NOTE: the `config_debug` check of the head states via the emap is not ported (the callers check it).
    return false;
}

/// By default, we try to zero out memory using OS-provided demand-zeroed pages. If the user has specifically
/// requested hugepages, though, we don't want to purge in the middle of a hugepage (which would break it up), so we
/// act conservatively and use memset.
/// jemalloc: ehooks_default_zero_impl
void ehooksDefaultZeroImpl(void * addr, size_t size);

/// jemalloc: ehooks_default_guard_impl
JE_ALWAYS_INLINE void ehooksDefaultGuardImpl(void * guard1, void * guard2)
{
    pages::markGuards(guard1, guard2);
}

/// jemalloc: ehooks_default_unguard_impl
JE_ALWAYS_INLINE void ehooksDefaultUnguardImpl(void * guard1, void * guard2)
{
    pages::unmarkGuards(guard1, guard2);
}

/// Some hooks are required to return zeroed memory in certain situations. In debug mode, we do some heuristic checks
/// that they did what they were supposed to.
/// jemalloc: ehooks_debug_zero_check
inline void ehooksDebugZeroCheck(void * addr, size_t size)
{
    JE_ASSERT((reinterpret_cast<uintptr_t>(addr) & PAGE_MASK) == 0);
    JE_ASSERT((size & PAGE_MASK) == 0);
    JE_ASSERT(size > 0);
    if constexpr (config::debug)
    {
        /// Check the whole first page.
        const size_t * p = static_cast<const size_t *>(addr);
        for (size_t i = 0; i < PAGE / sizeof(size_t); ++i)
            JE_ASSERT(p[i] == 0);
        /// And 4 spots within.
        constexpr size_t nchecks = 4;
        static_assert(PAGE >= sizeof(size_t) * nchecks);
        for (size_t i = 0; i < nchecks; ++i)
            JE_ASSERT(p[i * (size / sizeof(size_t) / nchecks)] == 0);
    }
}

/// jemalloc: ehooks_t
class ExtentHooks
{
public:
    constexpr ExtentHooks() = default;

    ExtentHooks(const ExtentHooks &) = delete;
    ExtentHooks & operator=(const ExtentHooks &) = delete;

    /// jemalloc: ehooks_init
    void init(extent_hooks_t * extent_hooks, unsigned ind_)
    {
        /// All other hooks are optional; this one is not.
        JE_ASSERT(extent_hooks->alloc != nullptr);
        ind = ind_;
        setExtentHooksPtr(extent_hooks);
    }

    /// The user-visible id that goes with the hooks (that of the base they're a part of, the associated arena's index).
    /// jemalloc: ehooks_ind_get
    JE_ALWAYS_INLINE unsigned indGet() const { return ind; }

    /// jemalloc: ehooks_set_extent_hooks_ptr
    JE_ALWAYS_INLINE void setExtentHooksPtr(extent_hooks_t * extent_hooks) { ptr.store(extent_hooks, std::memory_order_release); }

    /// jemalloc: ehooks_get_extent_hooks_ptr
    JE_ALWAYS_INLINE extent_hooks_t * getExtentHooksPtr() const { return ptr.load(std::memory_order_acquire); }

    /// jemalloc: ehooks_are_default
    JE_ALWAYS_INLINE bool areDefault() const { return getExtentHooksPtr() == &ehooks_default_extent_hooks; }

    /// In some cases, a caller needs to allocate resources before attempting to call a hook. If that hook is doomed
    /// to fail, this is wasteful. We therefore include some checks for such cases.
    /// jemalloc: ehooks_dalloc_will_fail
    JE_ALWAYS_INLINE bool dallocWillFail() const
    {
        assertDefault();
        return opt.retain;
    }

    /// jemalloc: ehooks_split_will_fail (the default table has `split`)
    JE_ALWAYS_INLINE bool splitWillFail() const
    {
        assertDefault();
        return false;
    }

    /// jemalloc: ehooks_merge_will_fail (the default table has `merge`)
    JE_ALWAYS_INLINE bool mergeWillFail() const
    {
        assertDefault();
        return false;
    }

    /// jemalloc: ehooks_guard_will_fail
    JE_ALWAYS_INLINE bool guardWillFail() const
    {
        assertDefault();
        return false;
    }

    /// jemalloc: ehooks_alloc
    JE_ALWAYS_INLINE void * alloc(ThreadState * tsdn, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit) const
    {
        assertDefault();
        [[maybe_unused]] bool orig_zero = *zero;
        void * ret = ehooksDefaultAllocImpl(tsdn, new_addr, size, alignment, zero, commit, indGet());
        JE_ASSERT(new_addr == nullptr || ret == nullptr || new_addr == ret);
        JE_ASSERT(!orig_zero || *zero);
        if constexpr (config::debug)
        {
            if (*zero && ret != nullptr)
                ehooksDebugZeroCheck(ret, size);
        }
        return ret;
    }

    /// Returns true on error (the memory was not deallocated).
    /// jemalloc: ehooks_dalloc
    JE_ALWAYS_INLINE bool dalloc(ThreadState * /*tsdn*/, void * addr, size_t size, bool /*committed*/) const
    {
        assertDefault();
        return ehooksDefaultDallocImpl(addr, size);
    }

    /// jemalloc: ehooks_destroy
    JE_ALWAYS_INLINE void destroy(ThreadState * /*tsdn*/, void * addr, size_t size, bool /*committed*/) const
    {
        assertDefault();
        ehooksDefaultDestroyImpl(addr, size);
    }

    /// Returns true on error.
    /// jemalloc: ehooks_commit
    JE_ALWAYS_INLINE bool commit(ThreadState * /*tsdn*/, void * addr, size_t size, size_t offset, size_t length) const
    {
        assertDefault();
        bool err = ehooksDefaultCommitImpl(addr, offset, length);
        if constexpr (config::debug)
        {
            if (!err)
                ehooksDebugZeroCheck(addr, size);
        }
        (void)size;
        return err;
    }

    /// Returns true on error.
    /// jemalloc: ehooks_decommit
    JE_ALWAYS_INLINE bool decommit(ThreadState * /*tsdn*/, void * addr, size_t /*size*/, size_t offset, size_t length) const
    {
        assertDefault();
        return ehooksDefaultDecommitImpl(addr, offset, length);
    }

    /// Returns true on error.
    /// jemalloc: ehooks_purge_lazy
    JE_ALWAYS_INLINE bool purgeLazy(ThreadState * /*tsdn*/, void * addr, size_t /*size*/, size_t offset, size_t length) const
    {
        assertDefault();
        return ehooksDefaultPurgeLazyImpl(addr, offset, length);
    }

    /// Returns true on error. (`purge_forced` is required to zero, but it is not checked even in debug mode: that
    /// would touch the pages.)
    /// jemalloc: ehooks_purge_forced
    JE_ALWAYS_INLINE bool purgeForced(ThreadState * /*tsdn*/, void * addr, size_t /*size*/, size_t offset, size_t length) const
    {
        assertDefault();
        return ehooksDefaultPurgeForcedImpl(addr, offset, length);
    }

    /// Returns true on error.
    /// jemalloc: ehooks_split
    JE_ALWAYS_INLINE bool split(ThreadState * /*tsdn*/, void * /*addr*/, size_t /*size*/, size_t /*size_a*/, size_t /*size_b*/, bool /*committed*/) const
    {
        assertDefault();
        return ehooksDefaultSplitImpl();
    }

    /// Returns true on error (the extents must not be merged).
    /// jemalloc: ehooks_merge
    JE_ALWAYS_INLINE bool merge(ThreadState * tsdn, void * addr_a, size_t /*size_a*/, void * addr_b, size_t /*size_b*/, bool /*committed*/) const
    {
        assertDefault();
        return ehooksDefaultMergeImpl(tsdn, addr_a, addr_b);
    }

    /// jemalloc: ehooks_zero
    JE_ALWAYS_INLINE void zero(ThreadState * /*tsdn*/, void * addr, size_t size) const
    {
        assertDefault();
        ehooksDefaultZeroImpl(addr, size);
    }

    /// Returns true on error.
    /// jemalloc: ehooks_guard
    JE_ALWAYS_INLINE bool guard(ThreadState * /*tsdn*/, void * guard1, void * guard2) const
    {
        assertDefault();
        ehooksDefaultGuardImpl(guard1, guard2);
        return false;
    }

    /// Returns true on error.
    /// jemalloc: ehooks_unguard
    JE_ALWAYS_INLINE bool unguard(ThreadState * /*tsdn*/, void * guard1, void * guard2) const
    {
        assertDefault();
        ehooksDefaultUnguardImpl(guard1, guard2);
        return false;
    }

private:
    JE_ALWAYS_INLINE void assertDefault() const { JE_ASSERT(areDefault()); }

    unsigned ind = 0;
    /// Logically an `extent_hooks_t *`.
    std::atomic<extent_hooks_t *> ptr{nullptr};
};

static_assert(sizeof(ExtentHooks) == 16, "ehooks_t is 16 bytes");

}
