#include <allocator/Base.h>

#include <allocator/SizeClasses.h>

#include <cstring>
#include <new>

namespace jemalloc
{

/// The layout of `base_t` (its size is bump-allocated from the first block and counted in `stats.metadata`).
static_assert(sizeof(Base) == 2 * sizeof(ExtentHooks) + sizeof(Mutex) + 8 + 8 + 8 + SIZE_CLASS_NUM_SIZES * 16 + 16 + 6 * 8);
#if defined(__linux__) && defined(__GLIBC__) && defined(__aarch64__)
static_assert(sizeof(Base) == 3952, "base_t is 3952 bytes on aarch64 glibc");
#endif

namespace
{

constinit Base * b0 = nullptr;

/// jemalloc: metadata_thp_madvise
ALLOCATOR_ALWAYS_INLINE bool metadataTransparentHugePagesMadvise()
{
    return metadataTransparentHugePagesEnabled() && init_system_transparent_huge_pages_mode == SystemTransparentHugePagesMode::Madvise;
}

/// Borrow the guarded bit to indicate if the extent is a recycled one, i.e. the ones returned to base for reuse;
/// currently only tcache bin stacks. Skips stats updating if so (needed for this purpose only).
/// jemalloc: base_edata_is_reused
ALLOCATOR_ALWAYS_INLINE bool baseExtentIsReused(const Extent * extent)
{
    return extent->guarded();
}

/// jemalloc: base_edata_init
void baseExtentInit(size_t * extent_serial_number_next, Extent * extent, void * addr, size_t size)
{
    size_t serial_number = *extent_serial_number_next;
    ++(*extent_serial_number_next);

    extent->initBase(addr, size, serial_number, /* reused */ false);
}

/// jemalloc: base_block_size_ceil
size_t baseBlockSizeCeil(size_t block_size)
{
    return options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Disabled
        ? alignmentCeiling(block_size, BASE_BLOCK_MIN_ALIGN)
        : hugePageCeiling(block_size);
}

/// jemalloc: b0_alloc_header_size
ALLOCATOR_ALWAYS_INLINE void b0AllocHeaderSize(size_t * header_size, size_t * alignment)
{
    *alignment = QUANTUM;
    *header_size = QUANTUM > sizeof(Extent *) ? QUANTUM : sizeof(Extent *);
}

}

/// jemalloc: base_map
void * Base::map(ThreadState * /*tsdn*/, ExtentHooks * extent_hooks, unsigned /*ind*/, size_t size)
{
    bool zero = true;
    bool commit = true;

    /// Use huge page sizes and alignment when opt.metadata_thp is enabled or auto.
    size_t alignment;
    if (options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Disabled)
        alignment = BASE_BLOCK_MIN_ALIGN;
    else
    {
        ALLOCATOR_ASSERT(size == hugePageCeiling(size));
        alignment = HUGE_PAGE;
    }
    /// Only the default hooks exist (custom extent hooks are dropped): jemalloc calls `extent_alloc_mmap` directly
    /// for them, bypassing `ehooks_default_alloc_impl`.
    ALLOCATOR_ASSERT(extent_hooks->areDefault());
    (void)extent_hooks;
    return extentAllocMmap(nullptr, size, alignment, &zero, &commit);
}

/// Cascade through dalloc, decommit, purge_forced, and purge_lazy, stopping at first success. This cascade is
/// performed for consistency with the cascade in `extent_dalloc_wrapper`. This function is only ever called as a side
/// effect of arena destruction.
/// jemalloc: base_unmap
void Base::unmap(ThreadState * /*tsdn*/, ExtentHooks * extent_hooks, unsigned /*ind*/, void * addr, size_t size)
{
    ALLOCATOR_ASSERT(extent_hooks->areDefault());
    (void)extent_hooks;
    if (!extentDeallocateMmap(addr, size))
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
        ALLOCATOR_NOT_REACHED();
    }

    /// label_done:
    if (metadataTransparentHugePagesMadvise())
    {
        /// Set NOHUGEPAGE after unmap to avoid kernel defrag.
        ALLOCATOR_ASSERT((reinterpret_cast<uintptr_t>(addr) & HUGE_PAGE_MASK) == 0 && (size & HUGE_PAGE_MASK) == 0);
        pages::noHuge(addr, size);
    }
}

/// jemalloc: base_get_num_blocks
size_t Base::getNumBlocks(bool with_new_block) const
{
    const BaseBlock * b = blocks;
    ALLOCATOR_ASSERT(b != nullptr);

    size_t num_blocks = with_new_block ? 2 : 1;
    while (b->next != nullptr)
    {
        ++num_blocks;
        b = b->next;
    }

    return num_blocks;
}

/// jemalloc: base_auto_thp_switch
void Base::autoTransparentHugePagesSwitch(ThreadState * thread_state)
{
    ALLOCATOR_ASSERT(options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Auto);
    mutex.assertOwner(thread_state);
    if (auto_transparent_huge_pages_switched)
        return;
    /// Called when adding a new block.
    bool should_switch;
    if (idxGet() != 0)
        should_switch = (getNumBlocks(true) == BASE_AUTO_TRANSPARENT_HUGE_PAGES_THRESHOLD);
    else
        should_switch = (getNumBlocks(true) == BASE_AUTO_TRANSPARENT_HUGE_PAGES_THRESHOLD_A0);
    if (!should_switch)
        return;

    auto_transparent_huge_pages_switched = true;
    ALLOCATOR_ASSERT(!config::stats || num_transparent_huge_pages == 0);
    /// Make the initial blocks THP lazily.
    BaseBlock * block = blocks;
    while (block != nullptr)
    {
        ALLOCATOR_ASSERT((block->size & HUGE_PAGE_MASK) == 0);
        pages::huge(block, block->size);
        if constexpr (config::stats)
            num_transparent_huge_pages += hugePageCeiling(block->size - block->extent.baseSize()) >> LOG2_HUGE_PAGE;
        block = block->next;
        ALLOCATOR_ASSERT(block == nullptr || (idxGet() == 0));
    }

    /// The THP auto switch of the huge arena (`huge_arena_auto_thp_switch`) belongs to the `huge_arena_transparent_huge_pages`
    /// feature, which is dead under ClickHouse's configuration (`huge_arena_transparent_huge_pages` is off) and dropped.
}

/// jemalloc: base_extent_bump_alloc_helper
void * Base::extentBumpAllocHelper(Extent * extent, size_t * gap_size, size_t size, size_t alignment)
{
    ALLOCATOR_ASSERT(alignment == alignmentCeiling(alignment, QUANTUM));
    ALLOCATOR_ASSERT(size == alignmentCeiling(size, alignment));

    uintptr_t addr = reinterpret_cast<uintptr_t>(extent->addr());
    *gap_size = alignmentCeiling(addr, alignment) - addr;
    void * result = reinterpret_cast<char *>(addr) + *gap_size;
    ALLOCATOR_ASSERT(extent->baseSize() >= *gap_size + size);
    extent->initBase(
        reinterpret_cast<char *>(addr) + *gap_size + size,
        extent->baseSize() - *gap_size - size,
        extent->serialNumber(),
        baseExtentIsReused(extent));
    return result;
}

/// jemalloc: base_edata_heap_insert
void Base::extentHeapInsert(ThreadState * thread_state, Extent * extent)
{
    mutex.assertOwner(thread_state);

    size_t base_size = extent->baseSize();
    ALLOCATOR_ASSERT(base_size > 0);
    /// Compute the index for the largest size class that does not exceed extent's size.
    SizeClassIdx index_floor = size_classes::sizeToIndex(base_size + 1) - 1;
    available[index_floor].insert(extent);
}

/// Only can be called by top-level functions, since it may call `allocExtent` internally when cache is empty.
/// jemalloc: base_alloc_base_edata
Extent * Base::allocBaseExtent(ThreadState * thread_state)
{
    Extent * extent;

    mutex.lock(thread_state);
    extent = extent_available.first();
    if (extent != nullptr)
        extent_available.remove(extent);
    mutex.unlock(thread_state);

    if (extent == nullptr)
        extent = allocExtent(thread_state);

    return extent;
}

/// jemalloc: base_extent_bump_alloc_post
void Base::extentBumpAllocPost(ThreadState * thread_state, Extent * extent, size_t gap_size, void * addr, size_t size)
{
    if (extent->baseSize() > 0)
        extentHeapInsert(thread_state, extent);
    else
    {
        /// Freed base `Extent` stored in `extent_available`.
        extent_available.insert(extent);
    }

    if (config::stats && !baseExtentIsReused(extent))
    {
        allocated += size;
        /// Add one PAGE to `resident` for every page boundary that is crossed by the new allocation. Adjust `num_transparent_huge_pages`
        /// similarly when metadata_thp is enabled.
        uintptr_t a = reinterpret_cast<uintptr_t>(addr);
        resident += pageCeiling(a + size) - pageCeiling(a - gap_size);
        ALLOCATOR_ASSERT(allocated <= resident);
        ALLOCATOR_ASSERT(resident <= mapped);
        if (metadataTransparentHugePagesMadvise()
            && (options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Always
                || auto_transparent_huge_pages_switched))
        {
            num_transparent_huge_pages += (hugePageCeiling(a + size) - hugePageCeiling(a - gap_size)) >> LOG2_HUGE_PAGE;
            ALLOCATOR_ASSERT(mapped >= num_transparent_huge_pages << LOG2_HUGE_PAGE);
        }
    }
}

/// jemalloc: base_extent_bump_alloc
void * Base::extentBumpAlloc(ThreadState * thread_state, Extent * extent, size_t size, size_t alignment)
{
    size_t gap_size;
    void * result = extentBumpAllocHelper(extent, &gap_size, size, alignment);
    extentBumpAllocPost(thread_state, extent, gap_size, result, size);
    return result;
}

/// Allocate a block of virtual memory that is large enough to start with a `BaseBlock` header, followed by an object
/// of specified size and alignment. On success a pointer to the initialized `BaseBlock` header is returned.
/// jemalloc: base_block_alloc
BaseBlock * Base::blockAlloc(
    ThreadState * thread_state,
    Base * base,
    ExtentHooks * extent_hooks,
    unsigned idx,
    PageSizeClassIdx * page_size_class_idx_last,
    size_t * extent_serial_number_next,
    size_t size,
    size_t alignment)
{
    alignment = alignmentCeiling(alignment, QUANTUM);
    size_t usable_size = alignmentCeiling(size, alignment);
    size_t header_size = sizeof(BaseBlock);
    size_t gap_size = alignmentCeiling(header_size, alignment) - header_size;
    /// Create increasingly larger blocks in order to limit the total number of disjoint virtual memory ranges.
    /// Choose the next size in the page size class series (skipping size classes that are not a multiple of HUGEPAGE
    /// when using metadata_thp), or a size large enough to satisfy the requested size and alignment, whichever is
    /// larger.
    size_t min_block_size = baseBlockSizeCeil(size_classes::pageSizeToUsableSize(header_size + gap_size + usable_size));
    PageSizeClassIdx page_size_class_idx_next
        = (*page_size_class_idx_last + 1 < size_classes::pageSizeToPageSizeClassIdx(SIZE_CLASS_LARGE_MAX_CLASS))
        ? *page_size_class_idx_last + 1
        : *page_size_class_idx_last;
    size_t next_block_size = baseBlockSizeCeil(size_classes::pageSizeClassIdxToSize(page_size_class_idx_next));
    size_t block_size = (min_block_size > next_block_size) ? min_block_size : next_block_size;
    BaseBlock * block = static_cast<BaseBlock *>(map(thread_state, extent_hooks, idx, block_size));
    if (block == nullptr)
        return nullptr;

    if (metadataTransparentHugePagesMadvise())
    {
        void * addr = block;
        ALLOCATOR_ASSERT((reinterpret_cast<uintptr_t>(addr) & HUGE_PAGE_MASK) == 0 && (block_size & HUGE_PAGE_MASK) == 0);
        if (options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Always)
            pages::huge(addr, block_size);
        else if (options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Auto && base != nullptr)
        {
            /// base != nullptr indicates this is not a new base.
            base->mutex.lock(thread_state);
            base->autoTransparentHugePagesSwitch(thread_state);
            if (base->auto_transparent_huge_pages_switched)
                pages::huge(addr, block_size);
            base->mutex.unlock(thread_state);
        }
    }

    *page_size_class_idx_last = size_classes::pageSizeToPageSizeClassIdx(block_size);
    block->size = block_size;
    block->next = nullptr;
    ALLOCATOR_ASSERT(block_size >= header_size);
    baseExtentInit(extent_serial_number_next, &block->extent, reinterpret_cast<char *>(block) + header_size, block_size - header_size);
    return block;
}

/// Allocate an extent that is at least as large as specified size, with specified alignment.
/// jemalloc: base_extent_alloc
Extent * Base::extentAlloc(ThreadState * thread_state, size_t size, size_t alignment)
{
    mutex.assertOwner(thread_state);

    ExtentHooks * metadata_extent_hooks = extentHooksGetForMetadata();
    /// Drop mutex during `blockAlloc`, because an extent hook will be called.
    mutex.unlock(thread_state);
    BaseBlock * block = blockAlloc(
        thread_state, this, metadata_extent_hooks, idxGet(), &page_size_class_idx_last, &extent_serial_number_next, size, alignment);
    mutex.lock(thread_state);
    if (block == nullptr)
        return nullptr;
    block->next = blocks;
    blocks = block;
    if constexpr (config::stats)
    {
        allocated += sizeof(BaseBlock);
        resident += pageCeiling(sizeof(BaseBlock));
        mapped += block->size;
        if (metadataTransparentHugePagesMadvise()
            && !(
                options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Auto && !auto_transparent_huge_pages_switched))
        {
            ALLOCATOR_ASSERT(num_transparent_huge_pages > 0);
            num_transparent_huge_pages += hugePageCeiling(sizeof(BaseBlock)) >> LOG2_HUGE_PAGE;
        }
        ALLOCATOR_ASSERT(allocated <= resident);
        ALLOCATOR_ASSERT(resident <= mapped);
        ALLOCATOR_ASSERT(num_transparent_huge_pages << LOG2_HUGE_PAGE <= mapped);
    }
    return &block->extent;
}

/// jemalloc: b0get
Base * base0Get()
{
    return b0;
}

/// jemalloc: base_new
Base * Base::create(ThreadState * thread_state, unsigned idx, const extent_hooks_t * extent_hooks_ptr, bool metadata_use_hooks)
{
    PageSizeClassIdx page_size_class_idx_last = 0;
    size_t extent_serial_number_next = 0;

    /// The base will contain the hooks eventually, but it itself is allocated using them. So we use some stack hooks
    /// to bootstrap its memory, and then initialize the hooks within the `Base`.
    extent_hooks_t * metadata_hooks = metadata_use_hooks ? const_cast<extent_hooks_t *>(extent_hooks_ptr)
                                                         : const_cast<extent_hooks_t *>(&extent_hooks_default_extent_hooks);
    ExtentHooks fake_extent_hooks;
    fake_extent_hooks.init(metadata_hooks, idx);

    BaseBlock * block = blockAlloc(
        thread_state, nullptr, &fake_extent_hooks, idx, &page_size_class_idx_last, &extent_serial_number_next, sizeof(Base), QUANTUM);
    if (block == nullptr)
        return nullptr;

    size_t gap_size;
    size_t base_alignment = CACHE_LINE;
    size_t base_size = alignmentCeiling(sizeof(Base), base_alignment);
    void * base_memory = extentBumpAllocHelper(&block->extent, &gap_size, base_size, base_alignment);
    /// The memory is zero-filled by mmap, which is also the state the constructor produces.
    Base * base = new (base_memory) Base;
    base->extent_hooks.init(const_cast<extent_hooks_t *>(extent_hooks_ptr), idx);
    base->extent_hooks_base.init(metadata_hooks, idx);
    if (base->mutex.init("base", MutexRank::BASE, MutexLockOrder::RankExclusive))
    {
        unmap(thread_state, &fake_extent_hooks, idx, block, block->size);
        return nullptr;
    }
    base->page_size_class_idx_last = page_size_class_idx_last;
    base->extent_serial_number_next = extent_serial_number_next;
    base->blocks = block;
    base->auto_transparent_huge_pages_switched = false;
    for (SizeClassIdx i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
        base->available[i].init();
    base->extent_available.init();

    if constexpr (config::stats)
    {
        base->extent_allocated = 0;
        base->radix_tree_allocated = 0;
        base->allocated = sizeof(BaseBlock);
        base->resident = pageCeiling(sizeof(BaseBlock));
        base->mapped = block->size;
        base->num_transparent_huge_pages
            = (options.metadata_transparent_huge_pages == MetadataTransparentHugePagesMode::Always) && metadataTransparentHugePagesMadvise()
            ? hugePageCeiling(sizeof(BaseBlock)) >> LOG2_HUGE_PAGE
            : 0;
        ALLOCATOR_ASSERT(base->allocated <= base->resident);
        ALLOCATOR_ASSERT(base->resident <= base->mapped);
        ALLOCATOR_ASSERT(base->num_transparent_huge_pages << LOG2_HUGE_PAGE <= base->mapped);
    }

    /// Locking here is only necessary because of assertions.
    base->mutex.lock(thread_state);
    base->extentBumpAllocPost(thread_state, &block->extent, gap_size, base, base_size);
    base->mutex.unlock(thread_state);

    return base;
}

/// jemalloc: base_delete
void Base::destroy(ThreadState * thread_state)
{
    ExtentHooks * metadata_extent_hooks = extentHooksGetForMetadata();
    unsigned idx = idxGet();
    BaseBlock * next = blocks;
    do
    {
        BaseBlock * block = next;
        next = block->next;
        /// NOTE: the block containing `*this` may be unmapped here (without `opt_retain`); nothing of `*this` is
        /// accessed afterwards.
        unmap(thread_state, metadata_extent_hooks, idx, block, block->size);
    } while (next != nullptr);
}

/// jemalloc: base_extent_hooks_set
extent_hooks_t * Base::extentHooksSet(extent_hooks_t * extent_hooks_ptr)
{
    extent_hooks_t * old_extent_hooks_ptr = extent_hooks.getExtentHooksPtr();
    extent_hooks.init(extent_hooks_ptr, extent_hooks.idxGet());
    return old_extent_hooks_ptr;
}

/// jemalloc: base_alloc_impl
void *
Base::allocImpl(ThreadState * thread_state, size_t size, size_t alignment, size_t * struct_serial_number, size_t * result_usable_size)
{
    alignment = quantumCeiling(alignment);
    size_t usable_size = alignmentCeiling(size, alignment);
    size_t aligned_size = usable_size + alignment - QUANTUM;

    Extent * extent = nullptr;
    void * result = nullptr;
    mutex.lock(thread_state);
    for (SizeClassIdx i = size_classes::sizeToIndex(aligned_size); i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        extent = available[i].removeFirst();
        if (extent != nullptr)
        {
            /// Use existing space.
            break;
        }
    }
    if (extent == nullptr)
    {
        /// Try to allocate more space.
        extent = extentAlloc(thread_state, usable_size, alignment);
    }
    if (extent != nullptr)
    {
        result = extentBumpAlloc(thread_state, extent, usable_size, alignment);
        if (struct_serial_number != nullptr)
            *struct_serial_number = static_cast<size_t>(extent->serialNumber());
        if (result_usable_size != nullptr)
            *result_usable_size = usable_size;
    }
    mutex.unlock(thread_state);
    return result;
}

/// jemalloc: base_alloc
void * Base::alloc(ThreadState * thread_state, size_t size, size_t alignment)
{
    return allocImpl(thread_state, size, alignment, nullptr, nullptr);
}

/// jemalloc: base_alloc_edata
Extent * Base::allocExtent(ThreadState * thread_state)
{
    size_t struct_serial_number;
    size_t usable_size;
    Extent * extent = static_cast<Extent *>(allocImpl(thread_state, sizeof(Extent), EXTENT_ALIGNMENT, &struct_serial_number, &usable_size));
    if (extent == nullptr)
        return nullptr;
    if constexpr (config::stats)
        extent_allocated += usable_size;
    extent->setStructSerialNumber(struct_serial_number);
    return extent;
}

/// jemalloc: base_alloc_rtree
void * Base::allocRadixTree(ThreadState * thread_state, size_t size)
{
    size_t usable_size;
    void * radix_tree = allocImpl(thread_state, size, CACHE_LINE, nullptr, &usable_size);
    if (radix_tree == nullptr)
        return nullptr;
    if constexpr (config::stats)
        radix_tree_allocated += usable_size;
    return radix_tree;
}

/// jemalloc: b0_alloc_tcache_stack
void * b0AllocThreadCacheStack(ThreadState * thread_state, size_t stack_size)
{
    Base * base = base0Get();
    Extent * extent = base->allocBaseExtent(thread_state);
    if (extent == nullptr)
        return nullptr;

    /// Reserve room for the header, which stores a pointer to the managing `Extent`. The header itself is located
    /// right before the return address, so that the extent can be retrieved on dalloc. Bump up to usize to improve
    /// reusability -- otherwise the freed stacks will be put back into the previous size class.
    size_t struct_serial_number;
    size_t alignment;
    size_t header_size;
    b0AllocHeaderSize(&header_size, &alignment);

    size_t alloc_size = size_classes::sizeToUsableSize(stack_size + header_size);
    void * addr = base->allocImpl(thread_state, alloc_size, alignment, &struct_serial_number, nullptr);
    if (addr == nullptr)
    {
        /// jemalloc inserts without holding the base mutex here (a data race on this OOM path); take it.
        base->mutex.lock(thread_state);
        base->extent_available.insert(extent);
        base->mutex.unlock(thread_state);
        return nullptr;
    }

    /// Set is_reused: see comments in `baseExtentIsReused`.
    extent->initBase(addr, alloc_size, struct_serial_number, /* reused */ true);
    *static_cast<Extent **>(addr) = extent;

    return static_cast<char *>(addr) + header_size;
}

/// jemalloc: b0_dalloc_tcache_stack
void b0DeallocateThreadCacheStack(ThreadState * thread_state, void * thread_cache_stack)
{
    /// The `Extent` pointer is stored in the header.
    size_t alignment;
    size_t header_size;
    b0AllocHeaderSize(&header_size, &alignment);

    Extent * extent = *reinterpret_cast<Extent **>(static_cast<char *>(thread_cache_stack) - header_size);
    void * addr = extent->addr();
    size_t base_size = extent->baseSize();
    /// Marked as "reused" to avoid double counting stats.
    ALLOCATOR_ASSERT(baseExtentIsReused(extent));
    ALLOCATOR_ASSERT(addr != nullptr && base_size > 0);

    /// Zero out since base_alloc returns zeroed memory.
    memset(addr, 0, base_size);

    Base * base = base0Get();
    base->mutex.lock(thread_state);
    base->extentHeapInsert(thread_state, extent);
    base->mutex.unlock(thread_state);
}

/// jemalloc: base_stats_get
void Base::statsGet(
    ThreadState * thread_state,
    size_t * allocated_,
    size_t * extent_allocated_,
    size_t * radix_tree_allocated_,
    size_t * resident_,
    size_t * mapped_,
    size_t * num_transparent_huge_pages_)
{
    static_assert(config::stats);

    mutex.lock(thread_state);
    ALLOCATOR_ASSERT(allocated <= resident);
    ALLOCATOR_ASSERT(resident <= mapped);
    ALLOCATOR_ASSERT(extent_allocated + radix_tree_allocated <= allocated);
    *allocated_ = allocated;
    *extent_allocated_ = extent_allocated;
    *radix_tree_allocated_ = radix_tree_allocated;
    *resident_ = resident;
    *mapped_ = mapped;
    *num_transparent_huge_pages_ = num_transparent_huge_pages;
    mutex.unlock(thread_state);
}

/// jemalloc: base_boot
bool baseBoot(ThreadState * thread_state)
{
    b0 = Base::create(thread_state, 0, &extent_hooks_default_extent_hooks, /* metadata_use_hooks */ true);
    return b0 == nullptr;
}

}
