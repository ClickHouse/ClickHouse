#include <allocator/ExtentHooks.h>

#include <allocator/Pages.h>

#include <cstring>

namespace jemalloc
{

constinit const char * const dss_prec_names[] = {"disabled", "primary", "secondary", "N/A"};

/// jemalloc: ehooks_default_alloc_impl
void * ehooksDefaultAllocImpl(
    ThreadState * /*tsdn*/, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned /*arena_ind*/)
{
    /// jemalloc: extent_alloc_core. The "primary" and "secondary" DSS attempts (`arena->dss_prec`) are dropped:
    /// only mmap is used. The side effects of a failed DSS attempt are reproduced by `extentAllocWrapper`.
    JE_ASSERT(size != 0);
    JE_ASSERT(alignment != 0);
    void * ret = extentAllocMmap(new_addr, size, alignment, zero, commit);

    if (config::have_madvise_huge && ret)
        pages::setThpState(ret, size);
    return ret;
}

/// jemalloc: ehooks_default_dalloc_impl
bool ehooksDefaultDallocImpl(void * addr, size_t size)
{
    return extentDallocMmap(addr, size);
}

/// jemalloc: ehooks_default_destroy_impl
void ehooksDefaultDestroyImpl(void * addr, size_t size)
{
    pages::unmap(addr, size);
}

/// jemalloc: ehooks_default_commit_impl
bool ehooksDefaultCommitImpl(void * addr, size_t offset, size_t length)
{
    return pages::commit(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_decommit_impl
bool ehooksDefaultDecommitImpl(void * addr, size_t offset, size_t length)
{
    return pages::decommit(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_purge_lazy_impl
bool ehooksDefaultPurgeLazyImpl(void * addr, size_t offset, size_t length)
{
    return pages::purgeLazy(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_purge_forced_impl
bool ehooksDefaultPurgeForcedImpl(void * addr, size_t offset, size_t length)
{
    return pages::purgeForced(static_cast<char *>(addr) + offset, length);
}

/// jemalloc: ehooks_default_zero_impl
void ehooksDefaultZeroImpl(void * addr, size_t size)
{
    bool needs_memset = true;
    if (opt.thp != ThpMode::Always)
        needs_memset = pages::purgeForced(addr, size);
    if (needs_memset)
        memset(addr, 0, size);
}

namespace
{

/// The C entry points of the default hooks table. They are only reachable by an application that reads the table
/// through `arena.<i>.extent_hooks` and calls it; the allocator itself calls the implementations directly.

/// jemalloc: ehooks_default_alloc
void * ehooksDefaultAlloc(
    extent_hooks_t * /*extent_hooks*/, void * new_addr, size_t size, size_t alignment, bool * zero, bool * commit, unsigned arena_ind)
{
    /// jemalloc passes `tsdn_fetch()`, which the implementation does not use without DSS.
    return ehooksDefaultAllocImpl(nullptr, new_addr, size, alignmentCeiling(alignment, PAGE), zero, commit, arena_ind);
}

/// jemalloc: ehooks_default_dalloc
bool ehooksDefaultDalloc(extent_hooks_t * /*extent_hooks*/, void * addr, size_t size, bool /*committed*/, unsigned /*arena_ind*/)
{
    return ehooksDefaultDallocImpl(addr, size);
}

/// jemalloc: ehooks_default_destroy
void ehooksDefaultDestroy(extent_hooks_t * /*extent_hooks*/, void * addr, size_t size, bool /*committed*/, unsigned /*arena_ind*/)
{
    ehooksDefaultDestroyImpl(addr, size);
}

/// jemalloc: ehooks_default_commit
bool ehooksDefaultCommit(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    return ehooksDefaultCommitImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_decommit
bool ehooksDefaultDecommit(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    return ehooksDefaultDecommitImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_purge_lazy
bool ehooksDefaultPurgeLazy(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    JE_ASSERT(addr != nullptr);
    JE_ASSERT((offset & PAGE_MASK) == 0);
    JE_ASSERT(length != 0);
    JE_ASSERT((length & PAGE_MASK) == 0);
    return ehooksDefaultPurgeLazyImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_purge_forced
bool ehooksDefaultPurgeForced(
    extent_hooks_t * /*extent_hooks*/, void * addr, size_t /*size*/, size_t offset, size_t length, unsigned /*arena_ind*/)
{
    JE_ASSERT(addr != nullptr);
    JE_ASSERT((offset & PAGE_MASK) == 0);
    JE_ASSERT(length != 0);
    JE_ASSERT((length & PAGE_MASK) == 0);
    return ehooksDefaultPurgeForcedImpl(addr, offset, length);
}

/// jemalloc: ehooks_default_split
bool ehooksDefaultSplit(
    extent_hooks_t * /*extent_hooks*/,
    void * /*addr*/,
    size_t /*size*/,
    size_t /*size_a*/,
    size_t /*size_b*/,
    bool /*committed*/,
    unsigned /*arena_ind*/)
{
    return ehooksDefaultSplitImpl();
}

/// jemalloc: ehooks_default_merge
bool ehooksDefaultMerge(
    extent_hooks_t * /*extent_hooks*/,
    void * addr_a,
    size_t /*size_a*/,
    void * addr_b,
    size_t /*size_b*/,
    bool /*committed*/,
    unsigned /*arena_ind*/)
{
    /// jemalloc passes `tsdn_fetch()`, which the implementation does not use.
    return ehooksDefaultMergeImpl(nullptr, addr_a, addr_b);
}

}

/// jemalloc: ehooks_default_extent_hooks
constinit const extent_hooks_t ehooks_default_extent_hooks = {
    ehooksDefaultAlloc,
    ehooksDefaultDalloc,
    ehooksDefaultDestroy,
    ehooksDefaultCommit,
    ehooksDefaultDecommit,
    pages::can_purge_lazy ? ehooksDefaultPurgeLazy : nullptr,
    pages::can_purge_forced ? ehooksDefaultPurgeForced : nullptr,
    ehooksDefaultSplit,
    ehooksDefaultMerge,
};

}
