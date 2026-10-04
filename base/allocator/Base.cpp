#include <allocator/Base.h>

#include <allocator/SizeClasses.h>

#include <cstring>
#include <new>

namespace jemalloc
{

/// The layout of `base_t` (its size is bump-allocated from the first block and counted in `stats.metadata`).
static_assert(sizeof(Base) == 2 * sizeof(ExtentHooks) + sizeof(Mutex) + 8 + 8 + 8 + SC_NSIZES * 16 + 16 + 6 * 8);
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Base) == 3952, "base_t is 3952 bytes on aarch64 glibc");
#endif

namespace
{

constinit Base * b0 = nullptr;

/// jemalloc: metadata_thp_madvise
JE_ALWAYS_INLINE bool metadataThpMadvise()
{
    return metadataThpEnabled() && init_system_thp_mode == SystemThpMode::Madvise;
}

/// Borrow the guarded bit to indicate if the extent is a recycled one, i.e. the ones returned to base for reuse;
/// currently only tcache bin stacks. Skips stats updating if so (needed for this purpose only).
/// jemalloc: base_edata_is_reused
JE_ALWAYS_INLINE bool baseEdataIsReused(const Extent * edata)
{
    return edata->guarded();
}

/// jemalloc: base_edata_init
void baseEdataInit(size_t * extent_sn_next, Extent * edata, void * addr, size_t size)
{
    size_t sn = *extent_sn_next;
    ++(*extent_sn_next);

    edata->initBase(addr, size, sn, /* reused */ false);
}

/// jemalloc: base_block_size_ceil
size_t baseBlockSizeCeil(size_t block_size)
{
    return opt.metadata_thp == MetadataThpMode::Disabled ? alignmentCeiling(block_size, BASE_BLOCK_MIN_ALIGN)
                                                         : hugepageCeiling(block_size);
}

/// jemalloc: b0_alloc_header_size
JE_ALWAYS_INLINE void b0AllocHeaderSize(size_t * header_size, size_t * alignment)
{
    *alignment = QUANTUM;
    *header_size = QUANTUM > sizeof(Extent *) ? QUANTUM : sizeof(Extent *);
}

}

/// jemalloc: base_map
void * Base::map(ThreadState * /*tsdn*/, ExtentHooks * ehooks, unsigned /*ind*/, size_t size)
{
    bool zero = true;
    bool commit = true;

    /// Use huge page sizes and alignment when opt.metadata_thp is enabled or auto.
    size_t alignment;
    if (opt.metadata_thp == MetadataThpMode::Disabled)
        alignment = BASE_BLOCK_MIN_ALIGN;
    else
    {
        JE_ASSERT(size == hugepageCeiling(size));
        alignment = HUGEPAGE;
    }
    /// Only the default hooks exist (custom extent hooks are dropped): jemalloc calls `extent_alloc_mmap` directly
    /// for them, bypassing `ehooks_default_alloc_impl`.
    JE_ASSERT(ehooks->areDefault());
    (void)ehooks;
    return extentAllocMmap(nullptr, size, alignment, &zero, &commit);
}

/// Cascade through dalloc, decommit, purge_forced, and purge_lazy, stopping at first success. This cascade is
/// performed for consistency with the cascade in `extent_dalloc_wrapper`. This function is only ever called as a side
/// effect of arena destruction.
/// jemalloc: base_unmap
void Base::unmap(ThreadState * /*tsdn*/, ExtentHooks * ehooks, unsigned /*ind*/, void * addr, size_t size)
{
    JE_ASSERT(ehooks->areDefault());
    (void)ehooks;
    if (!extentDallocMmap(addr, size))
    {
    }
    else if (!pages::decommit(addr, size))
    {
    }
    else if (!pages::purgeForced(addr, size))
    {
    }
    else if (!pages::purgeLazy(addr, size))
    {
    }
    else
    {
        /// Nothing worked. This should never happen.
        JE_NOT_REACHED();
    }

    /// label_done:
    if (metadataThpMadvise())
    {
        /// Set NOHUGEPAGE after unmap to avoid kernel defrag.
        JE_ASSERT((reinterpret_cast<uintptr_t>(addr) & HUGEPAGE_MASK) == 0 && (size & HUGEPAGE_MASK) == 0);
        pages::nohuge(addr, size);
    }
}

/// jemalloc: base_get_num_blocks
size_t Base::getNumBlocks(bool with_new_block) const
{
    const BaseBlock * b = blocks;
    JE_ASSERT(b != nullptr);

    size_t n_blocks = with_new_block ? 2 : 1;
    while (b->next != nullptr)
    {
        ++n_blocks;
        b = b->next;
    }

    return n_blocks;
}

/// jemalloc: base_auto_thp_switch
void Base::autoThpSwitch(ThreadState * tsdn)
{
    JE_ASSERT(opt.metadata_thp == MetadataThpMode::Auto);
    mtx.assertOwner(tsdn);
    if (auto_thp_switched)
        return;
    /// Called when adding a new block.
    bool should_switch;
    if (indGet() != 0)
        should_switch = (getNumBlocks(true) == BASE_AUTO_THP_THRESHOLD);
    else
        should_switch = (getNumBlocks(true) == BASE_AUTO_THP_THRESHOLD_A0);
    if (!should_switch)
        return;

    auto_thp_switched = true;
    JE_ASSERT(!config::stats || n_thp == 0);
    /// Make the initial blocks THP lazily.
    BaseBlock * block = blocks;
    while (block != nullptr)
    {
        JE_ASSERT((block->size & HUGEPAGE_MASK) == 0);
        pages::huge(block, block->size);
        if constexpr (config::stats)
            n_thp += hugepageCeiling(block->size - block->edata.bsize()) >> LG_HUGEPAGE;
        block = block->next;
        JE_ASSERT(block == nullptr || (indGet() == 0));
    }

    /// The THP auto switch of the huge arena (`huge_arena_auto_thp_switch`) belongs to the `huge_arena_pac_thp`
    /// feature, which is dead under ClickHouse's configuration (`huge_arena_pac_thp` is off) and dropped.
}

/// jemalloc: base_extent_bump_alloc_helper
void * Base::extentBumpAllocHelper(Extent * edata, size_t * gap_size, size_t size, size_t alignment)
{
    JE_ASSERT(alignment == alignmentCeiling(alignment, QUANTUM));
    JE_ASSERT(size == alignmentCeiling(size, alignment));

    uintptr_t addr = reinterpret_cast<uintptr_t>(edata->addr());
    *gap_size = alignmentCeiling(addr, alignment) - addr;
    void * ret = reinterpret_cast<char *>(addr) + *gap_size;
    JE_ASSERT(edata->bsize() >= *gap_size + size);
    edata->initBase(
        reinterpret_cast<char *>(addr) + *gap_size + size, edata->bsize() - *gap_size - size, edata->sn(), baseEdataIsReused(edata));
    return ret;
}

/// jemalloc: base_edata_heap_insert
void Base::edataHeapInsert(ThreadState * tsdn, Extent * edata)
{
    mtx.assertOwner(tsdn);

    size_t bsize = edata->bsize();
    JE_ASSERT(bsize > 0);
    /// Compute the index for the largest size class that does not exceed extent's size.
    szind_t index_floor = sz::sizeToIndex(bsize + 1) - 1;
    avail[index_floor].insert(edata);
}

/// Only can be called by top-level functions, since it may call `allocExtent` internally when cache is empty.
/// jemalloc: base_alloc_base_edata
Extent * Base::allocBaseEdata(ThreadState * tsdn)
{
    Extent * edata;

    mtx.lock(tsdn);
    edata = edata_avail.first();
    if (edata != nullptr)
        edata_avail.remove(edata);
    mtx.unlock(tsdn);

    if (edata == nullptr)
        edata = allocExtent(tsdn);

    return edata;
}

/// jemalloc: base_extent_bump_alloc_post
void Base::extentBumpAllocPost(ThreadState * tsdn, Extent * edata, size_t gap_size, void * addr, size_t size)
{
    if (edata->bsize() > 0)
        edataHeapInsert(tsdn, edata);
    else
    {
        /// Freed base `Extent` stored in `edata_avail`.
        edata_avail.insert(edata);
    }

    if (config::stats && !baseEdataIsReused(edata))
    {
        allocated += size;
        /// Add one PAGE to `resident` for every page boundary that is crossed by the new allocation. Adjust `n_thp`
        /// similarly when metadata_thp is enabled.
        uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        resident += pageCeiling(a + size) - pageCeiling(a - gap_size);
        JE_ASSERT(allocated <= resident);
        JE_ASSERT(resident <= mapped);
        if (metadataThpMadvise() && (opt.metadata_thp == MetadataThpMode::Always || auto_thp_switched))
        {
            n_thp += (hugepageCeiling(a + size) - hugepageCeiling(a - gap_size)) >> LG_HUGEPAGE;
            JE_ASSERT(mapped >= n_thp << LG_HUGEPAGE);
        }
    }
}

/// jemalloc: base_extent_bump_alloc
void * Base::extentBumpAlloc(ThreadState * tsdn, Extent * edata, size_t size, size_t alignment)
{
    size_t gap_size;
    void * ret = extentBumpAllocHelper(edata, &gap_size, size, alignment);
    extentBumpAllocPost(tsdn, edata, gap_size, ret, size);
    return ret;
}

/// Allocate a block of virtual memory that is large enough to start with a `BaseBlock` header, followed by an object
/// of specified size and alignment. On success a pointer to the initialized `BaseBlock` header is returned.
/// jemalloc: base_block_alloc
BaseBlock * Base::blockAlloc(
    ThreadState * tsdn,
    Base * base,
    ExtentHooks * ehooks,
    unsigned ind,
    pszind_t * pind_last,
    size_t * extent_sn_next,
    size_t size,
    size_t alignment)
{
    alignment = alignmentCeiling(alignment, QUANTUM);
    size_t usize = alignmentCeiling(size, alignment);
    size_t header_size = sizeof(BaseBlock);
    size_t gap_size = alignmentCeiling(header_size, alignment) - header_size;
    /// Create increasingly larger blocks in order to limit the total number of disjoint virtual memory ranges.
    /// Choose the next size in the page size class series (skipping size classes that are not a multiple of HUGEPAGE
    /// when using metadata_thp), or a size large enough to satisfy the requested size and alignment, whichever is
    /// larger.
    size_t min_block_size = baseBlockSizeCeil(sz::psz2u(header_size + gap_size + usize));
    pszind_t pind_next = (*pind_last + 1 < sz::psz2ind(SC_LARGE_MAXCLASS)) ? *pind_last + 1 : *pind_last;
    size_t next_block_size = baseBlockSizeCeil(sz::pind2sz(pind_next));
    size_t block_size = (min_block_size > next_block_size) ? min_block_size : next_block_size;
    BaseBlock * block = static_cast<BaseBlock *>(map(tsdn, ehooks, ind, block_size));
    if (block == nullptr)
        return nullptr;

    if (metadataThpMadvise())
    {
        void * addr = block;
        JE_ASSERT((reinterpret_cast<uintptr_t>(addr) & HUGEPAGE_MASK) == 0 && (block_size & HUGEPAGE_MASK) == 0);
        if (opt.metadata_thp == MetadataThpMode::Always)
            pages::huge(addr, block_size);
        else if (opt.metadata_thp == MetadataThpMode::Auto && base != nullptr)
        {
            /// base != nullptr indicates this is not a new base.
            base->mtx.lock(tsdn);
            base->autoThpSwitch(tsdn);
            if (base->auto_thp_switched)
                pages::huge(addr, block_size);
            base->mtx.unlock(tsdn);
        }
    }

    *pind_last = sz::psz2ind(block_size);
    block->size = block_size;
    block->next = nullptr;
    JE_ASSERT(block_size >= header_size);
    baseEdataInit(extent_sn_next, &block->edata, reinterpret_cast<char *>(block) + header_size, block_size - header_size);
    return block;
}

/// Allocate an extent that is at least as large as specified size, with specified alignment.
/// jemalloc: base_extent_alloc
Extent * Base::extentAlloc(ThreadState * tsdn, size_t size, size_t alignment)
{
    mtx.assertOwner(tsdn);

    ExtentHooks * metadata_ehooks = ehooksGetForMetadata();
    /// Drop mutex during `blockAlloc`, because an extent hook will be called.
    mtx.unlock(tsdn);
    BaseBlock * block = blockAlloc(tsdn, this, metadata_ehooks, indGet(), &pind_last, &extent_sn_next, size, alignment);
    mtx.lock(tsdn);
    if (block == nullptr)
        return nullptr;
    block->next = blocks;
    blocks = block;
    if constexpr (config::stats)
    {
        allocated += sizeof(BaseBlock);
        resident += pageCeiling(sizeof(BaseBlock));
        mapped += block->size;
        if (metadataThpMadvise() && !(opt.metadata_thp == MetadataThpMode::Auto && !auto_thp_switched))
        {
            JE_ASSERT(n_thp > 0);
            n_thp += hugepageCeiling(sizeof(BaseBlock)) >> LG_HUGEPAGE;
        }
        JE_ASSERT(allocated <= resident);
        JE_ASSERT(resident <= mapped);
        JE_ASSERT(n_thp << LG_HUGEPAGE <= mapped);
    }
    return &block->edata;
}

/// jemalloc: b0get
Base * b0get()
{
    return b0;
}

/// jemalloc: base_new
Base * Base::create(ThreadState * tsdn, unsigned ind, const extent_hooks_t * extent_hooks, bool metadata_use_hooks)
{
    pszind_t pind_last = 0;
    size_t extent_sn_next = 0;

    /// The base will contain the hooks eventually, but it itself is allocated using them. So we use some stack hooks
    /// to bootstrap its memory, and then initialize the hooks within the `Base`.
    extent_hooks_t * metadata_hooks = metadata_use_hooks ? const_cast<extent_hooks_t *>(extent_hooks)
                                                         : const_cast<extent_hooks_t *>(&ehooks_default_extent_hooks);
    ExtentHooks fake_ehooks;
    fake_ehooks.init(metadata_hooks, ind);

    BaseBlock * block = blockAlloc(tsdn, nullptr, &fake_ehooks, ind, &pind_last, &extent_sn_next, sizeof(Base), QUANTUM);
    if (block == nullptr)
        return nullptr;

    size_t gap_size;
    size_t base_alignment = CACHELINE;
    size_t base_size = alignmentCeiling(sizeof(Base), base_alignment);
    void * base_memory = extentBumpAllocHelper(&block->edata, &gap_size, base_size, base_alignment);
    /// The memory is zero-filled by mmap, which is also the state the constructor produces.
    Base * base = new (base_memory) Base;
    base->ehooks.init(const_cast<extent_hooks_t *>(extent_hooks), ind);
    base->ehooks_base.init(metadata_hooks, ind);
    if (base->mtx.init("base", MutexRank::BASE, MutexLockOrder::RankExclusive))
    {
        unmap(tsdn, &fake_ehooks, ind, block, block->size);
        return nullptr;
    }
    base->pind_last = pind_last;
    base->extent_sn_next = extent_sn_next;
    base->blocks = block;
    base->auto_thp_switched = false;
    for (szind_t i = 0; i < SC_NSIZES; ++i)
        base->avail[i].init();
    base->edata_avail.init();

    if constexpr (config::stats)
    {
        base->edata_allocated = 0;
        base->rtree_allocated = 0;
        base->allocated = sizeof(BaseBlock);
        base->resident = pageCeiling(sizeof(BaseBlock));
        base->mapped = block->size;
        base->n_thp = (opt.metadata_thp == MetadataThpMode::Always) && metadataThpMadvise()
            ? hugepageCeiling(sizeof(BaseBlock)) >> LG_HUGEPAGE
            : 0;
        JE_ASSERT(base->allocated <= base->resident);
        JE_ASSERT(base->resident <= base->mapped);
        JE_ASSERT(base->n_thp << LG_HUGEPAGE <= base->mapped);
    }

    /// Locking here is only necessary because of assertions.
    base->mtx.lock(tsdn);
    base->extentBumpAllocPost(tsdn, &block->edata, gap_size, base, base_size);
    base->mtx.unlock(tsdn);

    return base;
}

/// jemalloc: base_delete
void Base::destroy(ThreadState * tsdn)
{
    ExtentHooks * metadata_ehooks = ehooksGetForMetadata();
    unsigned ind = indGet();
    BaseBlock * next = blocks;
    do
    {
        BaseBlock * block = next;
        next = block->next;
        /// NOTE: the block containing `*this` may be unmapped here (without `opt_retain`); nothing of `*this` is
        /// accessed afterwards.
        unmap(tsdn, metadata_ehooks, ind, block, block->size);
    } while (next != nullptr);
}

/// jemalloc: base_extent_hooks_set
extent_hooks_t * Base::extentHooksSet(extent_hooks_t * extent_hooks)
{
    extent_hooks_t * old_extent_hooks = ehooks.getExtentHooksPtr();
    ehooks.init(extent_hooks, ehooks.indGet());
    return old_extent_hooks;
}

/// jemalloc: base_alloc_impl
void * Base::allocImpl(ThreadState * tsdn, size_t size, size_t alignment, size_t * esn, size_t * ret_usize)
{
    alignment = quantumCeiling(alignment);
    size_t usize = alignmentCeiling(size, alignment);
    size_t asize = usize + alignment - QUANTUM;

    Extent * edata = nullptr;
    void * ret = nullptr;
    mtx.lock(tsdn);
    for (szind_t i = sz::sizeToIndex(asize); i < SC_NSIZES; ++i)
    {
        edata = avail[i].removeFirst();
        if (edata != nullptr)
        {
            /// Use existing space.
            break;
        }
    }
    if (edata == nullptr)
    {
        /// Try to allocate more space.
        edata = extentAlloc(tsdn, usize, alignment);
    }
    if (edata != nullptr)
    {
        ret = extentBumpAlloc(tsdn, edata, usize, alignment);
        if (esn != nullptr)
            *esn = static_cast<size_t>(edata->sn());
        if (ret_usize != nullptr)
            *ret_usize = usize;
    }
    mtx.unlock(tsdn);
    return ret;
}

/// jemalloc: base_alloc
void * Base::alloc(ThreadState * tsdn, size_t size, size_t alignment)
{
    return allocImpl(tsdn, size, alignment, nullptr, nullptr);
}

/// jemalloc: base_alloc_edata
Extent * Base::allocExtent(ThreadState * tsdn)
{
    size_t esn;
    size_t usize;
    Extent * edata = static_cast<Extent *>(allocImpl(tsdn, sizeof(Extent), EDATA_ALIGNMENT, &esn, &usize));
    if (edata == nullptr)
        return nullptr;
    if constexpr (config::stats)
        edata_allocated += usize;
    edata->setEsn(esn);
    return edata;
}

/// jemalloc: base_alloc_rtree
void * Base::allocRtree(ThreadState * tsdn, size_t size)
{
    size_t usize;
    void * rtree = allocImpl(tsdn, size, CACHELINE, nullptr, &usize);
    if (rtree == nullptr)
        return nullptr;
    if constexpr (config::stats)
        rtree_allocated += usize;
    return rtree;
}

/// jemalloc: b0_alloc_tcache_stack
void * b0AllocTcacheStack(ThreadState * tsdn, size_t stack_size)
{
    Base * base = b0get();
    Extent * edata = base->allocBaseEdata(tsdn);
    if (edata == nullptr)
        return nullptr;

    /// Reserve room for the header, which stores a pointer to the managing `Extent`. The header itself is located
    /// right before the return address, so that the extent can be retrieved on dalloc. Bump up to usize to improve
    /// reusability -- otherwise the freed stacks will be put back into the previous size class.
    size_t esn;
    size_t alignment;
    size_t header_size;
    b0AllocHeaderSize(&header_size, &alignment);

    size_t alloc_size = sz::s2u(stack_size + header_size);
    void * addr = base->allocImpl(tsdn, alloc_size, alignment, &esn, nullptr);
    if (addr == nullptr)
    {
        /// jemalloc inserts without holding the base mutex here (a data race on this OOM path); take it.
        base->mtx.lock(tsdn);
        base->edata_avail.insert(edata);
        base->mtx.unlock(tsdn);
        return nullptr;
    }

    /// Set is_reused: see comments in `baseEdataIsReused`.
    edata->initBase(addr, alloc_size, esn, /* reused */ true);
    *static_cast<Extent **>(addr) = edata;

    return static_cast<char *>(addr) + header_size;
}

/// jemalloc: b0_dalloc_tcache_stack
void b0DallocTcacheStack(ThreadState * tsdn, void * tcache_stack)
{
    /// The `Extent` pointer is stored in the header.
    size_t alignment;
    size_t header_size;
    b0AllocHeaderSize(&header_size, &alignment);

    Extent * edata = *reinterpret_cast<Extent **>(static_cast<char *>(tcache_stack) - header_size);
    void * addr = edata->addr();
    size_t bsize = edata->bsize();
    /// Marked as "reused" to avoid double counting stats.
    JE_ASSERT(baseEdataIsReused(edata));
    JE_ASSERT(addr != nullptr && bsize > 0);

    /// Zero out since base_alloc returns zeroed memory.
    memset(addr, 0, bsize);

    Base * base = b0get();
    base->mtx.lock(tsdn);
    base->edataHeapInsert(tsdn, edata);
    base->mtx.unlock(tsdn);
}

/// jemalloc: base_stats_get
void Base::statsGet(
    ThreadState * tsdn,
    size_t * allocated_,
    size_t * edata_allocated_,
    size_t * rtree_allocated_,
    size_t * resident_,
    size_t * mapped_,
    size_t * n_thp_)
{
    static_assert(config::stats);

    mtx.lock(tsdn);
    JE_ASSERT(allocated <= resident);
    JE_ASSERT(resident <= mapped);
    JE_ASSERT(edata_allocated + rtree_allocated <= allocated);
    *allocated_ = allocated;
    *edata_allocated_ = edata_allocated;
    *rtree_allocated_ = rtree_allocated;
    *resident_ = resident;
    *mapped_ = mapped;
    *n_thp_ = n_thp;
    mtx.unlock(tsdn);
}

/// jemalloc: base_boot
bool baseBoot(ThreadState * tsdn)
{
    b0 = Base::create(tsdn, 0, &ehooks_default_extent_hooks, /* metadata_use_hooks */ true);
    return b0 == nullptr;
}

}
