#include <allocator/ExtentHooks.h>

#include <allocator/Pages.h>

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

/// The C entry points of the default hooks table. They are only reachable by an application that reads the table
/// through `arena.<i>.extent_hooks` and calls it; the allocator itself calls the implementations directly.

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
