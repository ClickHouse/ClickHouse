#include <allocator/CacheBin.h>

#include <allocator/Pages.h>

namespace jemalloc
{

constinit const uintptr_t disabled_bin = JUNK_ADDR;

/// jemalloc: cache_bin_info_init
void CacheBinInfo::init(CacheBinSize num_cached_max_)
{
    ALLOCATOR_ASSERT(num_cached_max_ <= CACHE_BIN_NUM_CACHED_MAX);
    [[maybe_unused]] size_t stack_size = size_t(num_cached_max_) * sizeof(void *);
    ALLOCATOR_ASSERT(stack_size < (size_t(1) << (sizeof(CacheBinSize) * 8)));
    num_cached_max = num_cached_max_;
}

/// The downside of allocating the stacks from the base allocator is that it never purges freed memory, and may cache
/// a fair amount of memory after many threads are terminated and not reused.
/// jemalloc: cache_bin_stack_use_thp
bool cacheBinStackUseTransparentHugePages()
{
    return metadataTransparentHugePagesEnabled();
}

/// jemalloc: cache_bin_info_compute_alloc
void cacheBinInfoComputeAlloc(const CacheBinInfo * infos, SizeClassIdx num_infos, size_t & size, size_t & alignment)
{
    /// For the total bin stack region (per tcache), reserve 2 more slots so that
    /// 1) the empty position can be safely read on the fast path before checking "is_empty"; and
    /// 2) the head can go beyond the empty position by 1 step safely on the fast path (i.e. no overflow).
    size = sizeof(void *) * 2;
    for (SizeClassIdx i = 0; i < num_infos; ++i)
        size += infos[i].num_cached_max * sizeof(void *);

    /// When not using THP, align to at least PAGE, to minimize the # of TLBs needed by the smaller sizes; also helps
    /// if the larger sizes don't get used at all.
    alignment = cacheBinStackUseTransparentHugePages() ? QUANTUM : PAGE;
}

/// jemalloc: cache_bin_preincrement
void cacheBinPreincrement(
    [[maybe_unused]] const CacheBinInfo * infos, [[maybe_unused]] SizeClassIdx num_infos, void * alloc, size_t & current_offset)
{
    if constexpr (config::debug)
    {
        size_t computed_size;
        size_t computed_alignment;

        /// The pointer should be as aligned as we asked for.
        cacheBinInfoComputeAlloc(infos, num_infos, computed_size, computed_alignment);
        ALLOCATOR_ASSERT((reinterpret_cast<uintptr_t>(alloc) & (computed_alignment - 1)) == 0);
    }

    *reinterpret_cast<uintptr_t *>(static_cast<std::byte *>(alloc) + current_offset) = cache_bin_preceding_junk;
    current_offset += sizeof(void *);
}

/// jemalloc: cache_bin_postincrement
void cacheBinPostincrement(void * alloc, size_t & current_offset)
{
    *reinterpret_cast<uintptr_t *>(static_cast<std::byte *>(alloc) + current_offset) = cache_bin_trailing_junk;
    current_offset += sizeof(void *);
}

/// jemalloc: cache_bin_init
void CacheBin::init(const CacheBinInfo & info, void * alloc, size_t & current_offset)
{
    /// The full position points to the lowest available space. Allocations will access the slots toward higher
    /// addresses (for the benefit of adjacent prefetch).
    void * stack_current = static_cast<std::byte *>(alloc) + current_offset;
    void * full_position = stack_current;
    CacheBinSize bin_stack_size = static_cast<CacheBinSize>(info.num_cached_max * sizeof(void *));

    current_offset += bin_stack_size;
    void * empty_position = static_cast<std::byte *>(alloc) + current_offset;

    /// Init to the empty position.
    stack_head = static_cast<void **>(empty_position);
    low_bits_low_water = lowBitsHead();
    low_bits_full = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(full_position));
    low_bits_empty = static_cast<CacheBinSize>(reinterpret_cast<uintptr_t>(empty_position));
    bin_info.init(info.num_cached_max);
    [[maybe_unused]] CacheBinSize free_spots = diff(low_bits_full, lowBitsHead());
    ALLOCATOR_ASSERT(free_spots == bin_stack_size);
    if (!disabled())
        ALLOCATOR_ASSERT(numCachedGetLocal() == 0);
    ALLOCATOR_ASSERT(emptyPositionGet() == empty_position);

    ALLOCATOR_ASSERT(bin_stack_size > 0 || empty_position == full_position);
}

/// jemalloc: cache_bin_init_disabled
void CacheBin::initDisabled(CacheBinSize num_cached_max)
{
    const void * fake_stack = disabledBinStack();
    size_t fake_offset = 0;
    CacheBinInfo fake_info;
    fake_info.init(0);
    init(fake_info, const_cast<void *>(fake_stack), fake_offset);
    bin_info.init(num_cached_max);
    ALLOCATOR_ASSERT(fake_offset == 0);
}

}
