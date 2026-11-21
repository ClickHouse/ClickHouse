#include <allocator/ExtentHooks.h>

#include <allocator/Pages.h>
#include <allocator/ThreadState.h>

#include <cstring>

namespace jemalloc
{

constinit const char * const sbrk_precedence_names[] = {"disabled", "primary", "secondary", "N/A"};

/// jemalloc: ehooks_default_alloc_impl
void * extentHooksDefaultAllocImpl(
    ThreadState * /*tsdn*/, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned /*arena_ind*/)
{
    /// jemalloc: extent_alloc_core. The "primary" and "secondary" DSS attempts (`arena->dss_prec`) are dropped:
    /// only mmap is used. The side effects of a failed DSS attempt are reproduced by `extentAllocWrapper`.
    ALLOCATOR_ASSERT(size != 0);
    ALLOCATOR_ASSERT(alignment != 0);
    void * result = extentAllocMmap(new_addr, size, alignment, zero, commit);

    if (config::have_madvise_huge && result)
        pages::setTransparentHugePagesState(result, size);
    return result;
}

/// jemalloc: ehooks_default_dalloc_impl
bool extentHooksDefaultDeallocateImpl(void * addr, size_t size)
{
    return extentDeallocateMmap(addr, size);
}

/// jemalloc: ehooks_default_destroy_impl
void extentHooksDefaultDestroyImpl(void * addr, size_t size)
{
    pages::unmap(addr, size);
}

/// jemalloc: ehooks_default_commit_impl
bool extentHooksDefaultCommitImpl(void * addr, size_t offset, size_t length)
{
    return pages::commit(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_decommit_impl
bool extentHooksDefaultDecommitImpl(void * addr, size_t offset, size_t length)
{
    return pages::decommit(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_purge_lazy_impl
bool extentHooksDefaultPurgeLazyImpl(void * addr, size_t offset, size_t length)
{
    return pages::purgeLazy(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_purge_forced_impl
bool extentHooksDefaultPurgeForcedImpl(void * addr, size_t offset, size_t length)
{
    return pages::purgeForced(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_zero_impl
void extentHooksDefaultZeroImpl(void * addr, size_t size)
{
    bool needs_memset = true;
    if (options.transparent_huge_pages != TransparentHugePagesMode::Always)
        needs_memset = pages::purgeForced(addr, size);
    if (needs_memset)
        memset(addr, 0, size);
}

namespace
{

/// jemalloc: ehooks_pre_reentrancy, ehooks_post_reentrancy
class ExtentHooksReentrancyGuard
{
public:
    explicit ExtentHooksReentrancyGuard(ThreadState * thread_state_ptr)
        : thread_state(thread_state_ptr == nullptr ? ThreadState::fetch() : *thread_state_ptr)
    {
        thread_state.preReentrancyRaw();
    }

    ~ExtentHooksReentrancyGuard() { thread_state.postReentrancyRaw(); }

    ExtentHooksReentrancyGuard(const ExtentHooksReentrancyGuard &) = delete;
    ExtentHooksReentrancyGuard & operator=(const ExtentHooksReentrancyGuard &) = delete;

private:
    ThreadState & thread_state;
};

}

/// jemalloc: ehooks_alloc (custom hooks)
void * extentHooksCustomAlloc(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * new_addr,
    size_t size,
    size_t alignment,
    bool * zero,
    bool * commit,
    unsigned arena_idx)
{
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->alloc(extent_hooks_ptr, new_addr, size, alignment, zero, commit, arena_idx);
}

/// jemalloc: ehooks_dalloc (custom hooks)
bool extentHooksCustomDeallocate(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, bool committed, unsigned arena_idx)
{
    if (extent_hooks_ptr->dalloc == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->dalloc(extent_hooks_ptr, addr, size, committed, arena_idx);
}

/// jemalloc: ehooks_destroy (custom hooks)
void extentHooksCustomDestroy(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, bool committed, unsigned arena_idx)
{
    if (extent_hooks_ptr->destroy == nullptr)
        return;
    ExtentHooksReentrancyGuard guard(thread_state);
    extent_hooks_ptr->destroy(extent_hooks_ptr, addr, size, committed, arena_idx);
}

/// jemalloc: ehooks_commit (custom hooks)
bool extentHooksCustomCommit(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx)
{
    if (extent_hooks_ptr->commit == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->commit(extent_hooks_ptr, addr, size, offset, length, arena_idx);
}

/// jemalloc: ehooks_decommit (custom hooks)
bool extentHooksCustomDecommit(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx)
{
    if (extent_hooks_ptr->decommit == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->decommit(extent_hooks_ptr, addr, size, offset, length, arena_idx);
}

/// jemalloc: ehooks_purge_lazy (custom hooks; also the default table where the pages cannot be purged lazily, which
/// has no `purge_lazy`)
bool extentHooksCustomPurgeLazy(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx)
{
    if (extent_hooks_ptr->purge_lazy == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->purge_lazy(extent_hooks_ptr, addr, size, offset, length, arena_idx);
}

/// jemalloc: ehooks_purge_forced (custom hooks; also the default table where the pages cannot be purged forcibly,
/// which has no `purge_forced`)
bool extentHooksCustomPurgeForced(
    ThreadState * thread_state, extent_hooks_t * extent_hooks_ptr, void * addr, size_t size, size_t offset, size_t length, unsigned arena_idx)
{
    if (extent_hooks_ptr->purge_forced == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->purge_forced(extent_hooks_ptr, addr, size, offset, length, arena_idx);
}

/// jemalloc: ehooks_split (custom hooks)
bool extentHooksCustomSplit(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * addr,
    size_t size,
    size_t size_a,
    size_t size_b,
    bool committed,
    unsigned arena_idx)
{
    if (extent_hooks_ptr->split == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->split(extent_hooks_ptr, addr, size, size_a, size_b, committed, arena_idx);
}

/// jemalloc: ehooks_merge (custom hooks)
bool extentHooksCustomMerge(
    ThreadState * thread_state,
    extent_hooks_t * extent_hooks_ptr,
    void * addr_a,
    size_t size_a,
    void * addr_b,
    size_t size_b,
    bool committed,
    unsigned arena_idx)
{
    if (extent_hooks_ptr->merge == nullptr)
        return true;
    ExtentHooksReentrancyGuard guard(thread_state);
    return extent_hooks_ptr->merge(extent_hooks_ptr, addr_a, size_a, addr_b, size_b, committed, arena_idx);
}

namespace
{

/// The C entry points of the default hooks table. They are only reachable by an application that reads the table
/// through `arena.<i>.extent_hooks` and calls it (custom hooks usually wrap them, as `JemallocNoDumpArenas` does); the
/// allocator itself calls the implementations directly.

/// jemalloc: ehooks_default_alloc
void * extentHooksDefaultAlloc(
    extent_hooks_t * /*extent_hooks*/, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_idx)
{
    /// jemalloc passes `tsdn_fetch()`, which the implementation does not use without DSS.
    return extentHooksDefaultAllocImpl(nullptr, new_addr, size, alignmentCeiling(alignment, PAGE), zero, commit, arena_idx);
}

/// jemalloc: ehooks_default_dalloc
bool extentHooksDefaultDeallocate(extent_hooks_t * /*extent_hooks*/, void * addr, size_t size, bool /*committed*/, unsigned /*arena_ind*/)
{
    return extentHooksDefaultDeallocateImpl(addr, size);
}

/// jemalloc: ehooks_default_destroy
void extentHooksDefaultDestroy(extent_hooks_t * /*extent_hooks*/, void * addr, size_t size, bool /*committed*/, unsigned /*arena_ind*/)
{
    extentHooksDefaultDestroyImpl(addr, size);
}

/// jemalloc: ehooks_default_commit
bool extentHooksDefaultCommit(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    return extentHooksDefaultCommitImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_decommit
bool extentHooksDefaultDecommit(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    return extentHooksDefaultDecommitImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_purge_lazy
bool extentHooksDefaultPurgeLazy(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    ALLOCATOR_ASSERT(addr != nullptr);
    ALLOCATOR_ASSERT((offset & PAGE_MASK) == 0);
    ALLOCATOR_ASSERT(length != 0);
    ALLOCATOR_ASSERT((length & PAGE_MASK) == 0);
    return extentHooksDefaultPurgeLazyImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_purge_forced
bool extentHooksDefaultPurgeForced(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    ALLOCATOR_ASSERT(addr != nullptr);
    ALLOCATOR_ASSERT((offset & PAGE_MASK) == 0);
    ALLOCATOR_ASSERT(length != 0);
    ALLOCATOR_ASSERT((length & PAGE_MASK) == 0);
    return extentHooksDefaultPurgeForcedImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_split
bool extentHooksDefaultSplit(
    extent_hooks_t * /*extent_hooks*/,
    void * /*addr*/,
    size_t /*size*/,
    size_t /*size_a*/,
    size_t /*size_b*/,
    bool /*committed*/,
    unsigned /*arena_ind*/)
{
    return extentHooksDefaultSplitImpl();
}

/// jemalloc: ehooks_default_merge
bool extentHooksDefaultMerge(
    extent_hooks_t * /*extent_hooks*/,
    void * addr_a,
    size_t /*size_a*/,
    void * addr_b,
    size_t /*size_b*/,
    bool /*committed*/,
    unsigned /*arena_ind*/)
{
    /// jemalloc passes `tsdn_fetch()`, which the implementation does not use.
    return extentHooksDefaultMergeImpl(nullptr, addr_a, addr_b);
}

}

/// jemalloc: ehooks_default_extent_hooks
constinit const extent_hooks_t extent_hooks_default_extent_hooks = {
    extentHooksDefaultAlloc,
    extentHooksDefaultDeallocate,
    extentHooksDefaultDestroy,
    extentHooksDefaultCommit,
    extentHooksDefaultDecommit,
    pages::can_purge_lazy ? extentHooksDefaultPurgeLazy : nullptr,
    pages::can_purge_forced ? extentHooksDefaultPurgeForced : nullptr,
    extentHooksDefaultSplit,
    extentHooksDefaultMerge,
};

}
