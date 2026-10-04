#include <allocator/CacheBin.h>

#include <allocator/Pages.h>

namespace jemalloc
{

constinit const uintptr_t disabled_bin = JUNK_ADDR;

/// jemalloc: cache_bin_info_init
void CacheBinInfo::init(cache_bin_sz_t ncached_max_)
{
    JE_ASSERT(ncached_max_ <= CACHE_BIN_NCACHED_MAX);
    [[maybe_unused]] size_t stack_size = size_t(ncached_max_) * sizeof(void *);
    JE_ASSERT(stack_size < (size_t(1) << (sizeof(cache_bin_sz_t) * 8)));
    ncached_max = ncached_max_;
}

/// The downside of allocating the stacks from the base allocator is that it never purges freed memory, and may cache
/// a fair amount of memory after many threads are terminated and not reused.
/// jemalloc: cache_bin_stack_use_thp
bool cacheBinStackUseThp()
{
    return metadataThpEnabled();
}

/// jemalloc: cache_bin_info_compute_alloc
void cacheBinInfoComputeAlloc(const CacheBinInfo * infos, szind_t ninfos, size_t & size, size_t & alignment)
{
    /// For the total bin stack region (per tcache), reserve 2 more slots so that
    /// 1) the empty position can be safely read on the fast path before checking "is_empty"; and
    /// 2) the head can go beyond the empty position by 1 step safely on the fast path (i.e. no overflow).
    size = sizeof(void *) * 2;
    for (szind_t i = 0; i < ninfos; ++i)
        size += infos[i].ncached_max * sizeof(void *);

    /// When not using THP, align to at least PAGE, to minimize the # of TLBs needed by the smaller sizes; also helps
    /// if the larger sizes don't get used at all.
    alignment = cacheBinStackUseThp() ? QUANTUM : PAGE;
}

/// jemalloc: cache_bin_preincrement
void cacheBinPreincrement([[maybe_unused]] const CacheBinInfo * infos, [[maybe_unused]] szind_t ninfos, void * alloc, size_t & cur_offset)
{
    if constexpr (config::debug)
    {
        size_t computed_size;
        size_t computed_alignment;

        /// The pointer should be as aligned as we asked for.
        cacheBinInfoComputeAlloc(infos, ninfos, computed_size, computed_alignment);
        JE_ASSERT((reinterpret_cast<uintptr_t>(alloc) & (computed_alignment - 1)) == 0);
    }

    *reinterpret_cast<uintptr_t *>(static_cast<std::byte *>(alloc) + cur_offset) = cache_bin_preceding_junk;
    cur_offset += sizeof(void *);
}

/// jemalloc: cache_bin_postincrement
void cacheBinPostincrement(void * alloc, size_t & cur_offset)
{
    *reinterpret_cast<uintptr_t *>(static_cast<std::byte *>(alloc) + cur_offset) = cache_bin_trailing_junk;
    cur_offset += sizeof(void *);
}

/// jemalloc: cache_bin_init
void CacheBin::init(const CacheBinInfo & info, void * alloc, size_t & cur_offset)
{
    /// The full position points to the lowest available space. Allocations will access the slots toward higher
    /// addresses (for the benefit of adjacent prefetch).
    void * stack_cur = static_cast<std::byte *>(alloc) + cur_offset;
    void * full_position = stack_cur;
    cache_bin_sz_t bin_stack_size = static_cast<cache_bin_sz_t>(info.ncached_max * sizeof(void *));

    cur_offset += bin_stack_size;
    void * empty_position = static_cast<std::byte *>(alloc) + cur_offset;

    /// Init to the empty position.
    stack_head = static_cast<void **>(empty_position);
    low_bits_low_water = lowBitsHead();
    low_bits_full = static_cast<cache_bin_sz_t>(reinterpret_cast<uintptr_t>(full_position));
    low_bits_empty = static_cast<cache_bin_sz_t>(reinterpret_cast<uintptr_t>(empty_position));
    bin_info.init(info.ncached_max);
    [[maybe_unused]] cache_bin_sz_t free_spots = diff(low_bits_full, lowBitsHead());
    JE_ASSERT(free_spots == bin_stack_size);
    if (!disabled())
        JE_ASSERT(ncachedGetLocal() == 0);
    JE_ASSERT(emptyPositionGet() == empty_position);

    JE_ASSERT(bin_stack_size > 0 || empty_position == full_position);
}

/// jemalloc: cache_bin_init_disabled
void CacheBin::initDisabled(cache_bin_sz_t ncached_max)
{
    const void * fake_stack = disabledBinStack();
    size_t fake_offset = 0;
    CacheBinInfo fake_info;
    fake_info.init(0);
    init(fake_info, const_cast<void *>(fake_stack), fake_offset);
    bin_info.init(ncached_max);
    JE_ASSERT(fake_offset == 0);
}

}
