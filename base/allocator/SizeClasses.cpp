#include <allocator/SizeClasses.h>

namespace jemalloc
{

constinit size_t large_pad = config::cache_oblivious ? PAGE : 0;

constinit std::array<BinInfo, SIZE_CLASS_NUM_BINS> bin_infos = default_bin_infos;

/// --- sc.c ----------------------------------------------------------------------------------------------------

namespace
{

/// jemalloc: sc_data_update_sc_slab_size
void sizeClassDataUpdateSizeClassSlabSize(SizeClass & size_class, size_t region_size, size_t pages_guess)
{
    size_t min_pages = region_size / PAGE;
    if (region_size % PAGE != 0)
        ++min_pages;
    /// BITMAP_MAX_BITS is actually determined by putting the smallest possible size-class on one page, so this can
    /// never be 0.
    size_t max_pages = BITMAP_MAX_BITS * region_size / PAGE;

    ALLOCATOR_ASSERT(min_pages <= max_pages);
    ALLOCATOR_ASSERT(min_pages > 0);
    ALLOCATOR_ASSERT(max_pages >= 1);
    if (pages_guess < min_pages)
        size_class.pages = int(min_pages);
    else if (pages_guess > max_pages)
        size_class.pages = int(max_pages);
    else
        size_class.pages = int(pages_guess);
}

}

/// jemalloc: sc_data_update_slab_size
void sizeClassDataUpdateSlabSize(SizeClassData & data, size_t begin, size_t end, int pages)
{
    ALLOCATOR_ASSERT(data.initialized);
    for (int i = 0; i < data.num_sizes; ++i)
    {
        SizeClass & size_class = data.size_class[i];
        if (!size_class.bin)
            break;
        size_t region_size = regionSizeCompute(size_class.log2_base, size_class.log2_delta, size_class.num_delta);
        if (begin <= region_size && region_size <= end)
            sizeClassDataUpdateSizeClassSlabSize(
                size_class, region_size, size_t(pages)); /// A negative `pages` becomes huge, as in jemalloc.
    }
}

/// jemalloc: sc_boot
void sizeClassBoot(SizeClassData & data)
{
    sizeClassDataInit(data);
}

/// --- sz.c ----------------------------------------------------------------------------------------------------

/// jemalloc: sz_boot
void sizeBoot(const SizeClassData & size_class_data, bool cache_oblivious)
{
    large_pad = cache_oblivious ? PAGE : 0;

    /// The tables are compile-time constants; check that they are what `sz_boot` would compute from `size_class_data`.
    if constexpr (config::debug)
    {
        unsigned page_size_class_idx = 0;
        for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
        {
            const SizeClass & size_class = size_class_data.size_class[i];
            size_t size = (size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta);
            ALLOCATOR_ASSERT(index_to_size_table[i] == size);
            if (size_class.page_size)
            {
                ALLOCATOR_ASSERT(page_size_class_idx_to_size_table[page_size_class_idx] == size);
                ++page_size_class_idx;
            }
        }
        ALLOCATOR_ASSERT(page_size_class_idx == SIZE_CLASS_NUM_PAGE_SIZES);
        ALLOCATOR_ASSERT(page_size_class_idx_to_size_table[SIZE_CLASS_NUM_PAGE_SIZES] == size_class_data.large_max_class + PAGE);
    }
}

namespace size_classes
{

/// jemalloc: sz_psz_quantize_floor
size_t pageSizeQuantizeFloor(size_t size)
{
    ALLOCATOR_ASSERT(size > 0);
    ALLOCATOR_ASSERT((size & PAGE_MASK) == 0);

    PageSizeClassIdx page_size_class_idx = pageSizeToPageSizeClassIdx(size - large_pad + 1);
    if (page_size_class_idx == 0)
    {
        /// Avoid underflow. This short-circuit would also do the right thing for all sizes in the range for which
        /// there are PAGE-spaced size classes, but it's simplest to just handle the one case that would cause
        /// erroneous results.
        return size;
    }
    size_t result = pageSizeClassIdxToSize(page_size_class_idx - 1) + large_pad;
    ALLOCATOR_ASSERT(result <= size);
    return result;
}

/// jemalloc: sz_psz_quantize_ceil
size_t pageSizeQuantizeCeil(size_t size)
{
    ALLOCATOR_ASSERT(size > 0);
    ALLOCATOR_ASSERT(size - large_pad <= SIZE_CLASS_LARGE_MAX_CLASS);
    ALLOCATOR_ASSERT((size & PAGE_MASK) == 0);

    size_t result = pageSizeQuantizeFloor(size);
    if (result < size)
    {
        /// Skip a quantization that may have an adequately large extent, because under-sized extents may be mixed
        /// in. This only happens when an unusual size is requested, i.e. for aligned allocation.
        result = pageSizeClassIdxToSize(pageSizeToPageSizeClassIdx(result - large_pad + 1)) + large_pad;
    }
    return result;
}

}

/// --- bin_info.c, bin.c ---------------------------------------------------------------------------------------

/// jemalloc: bin_info_boot
void binInfoBoot(const SizeClassData & size_class_data, const unsigned * bin_shard_sizes)
{
    ALLOCATOR_ASSERT(size_class_data.initialized);
    detail::binInfosInit(size_class_data, bin_shard_sizes, bin_infos.data());
}

/// jemalloc: bin_update_shard_size
bool binUpdateShardSize(unsigned * bin_shard_sizes, size_t start_size, size_t end_size, size_t num_shards)
{
    if (num_shards > BIN_SHARDS_MAX || num_shards == 0)
        return true;

    if (start_size > SIZE_CLASS_SMALL_MAX_CLASS)
        return false;
    if (end_size > SIZE_CLASS_SMALL_MAX_CLASS)
        end_size = SIZE_CLASS_SMALL_MAX_CLASS;

    /// Compute the index since this may happen before sz init.
    SizeClassIdx idx1 = size_classes::sizeToIndexCompute(start_size);
    SizeClassIdx idx2 = size_classes::sizeToIndexCompute(end_size);
    for (unsigned i = idx1; i <= idx2; ++i)
        bin_shard_sizes[i] = unsigned(num_shards);

    return false;
}

/// jemalloc: bin_shard_sizes_boot
void binShardSizesBoot(unsigned * bin_shard_sizes)
{
    /// Load the default number of shards.
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
        bin_shard_sizes[i] = NUM_BIN_SHARDS_DEFAULT;
}

}
