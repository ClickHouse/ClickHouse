#pragma once

/// Size classes, size computations, bin metadata and region index division.
/// jemalloc: `sc.h`/`sc.c`, `sz.h`/`sz.c`, `bin_info.h`/`bin_info.c`, `div.h`/`div.c`, `bin_update_shard_size` and
/// `bin_shard_sizes_boot` from `bin.c`.
///
/// The size class table (`sc_data_t`) depends only on compile-time constants, except the slab page counts which the
/// `slab_sizes` option can change at boot. Therefore:
/// - the default table `default_size_class_data` and the lookup tables `index_to_size_table`, `size_to_index_table`,
///   `page_size_class_idx_to_size_table` (which do not depend on slab sizes) are compile-time constants;
/// - `bin_infos` (slab sizes, shards) and `large_pad` (`cache_oblivious`) are constant-initialized with the
///   defaults and recomputed by `binInfoBoot` / `sizeBoot` at boot, after the options are parsed.

#include <allocator/Bitmap.h>
#include <allocator/Common.h>
#include <allocator/Options.h>
#include <allocator/SizeClassConstants.h>

#include <array>

namespace jemalloc
{

/// --- Size class generation (sc.c) ------------------------------------------------------------------------------

/// jemalloc: sc_t
struct SizeClass
{
    /// Size class index, or -1 if not a valid size class.
    int index;
    /// Lg group base size (no deltas added).
    int log2_base;
    /// Lg delta to previous size class.
    int log2_delta;
    /// Delta multiplier. size == 1<<lg_base + ndelta<<lg_delta
    int num_delta;
    /// True if the size class is a multiple of the page size.
    bool page_size;
    /// True if the size class is a small, bin, size class.
    bool bin;
    /// The slab page count if a small bin size class, 0 otherwise.
    int pages;
    /// Same as lg_delta if a lookup table size class, 0 otherwise.
    int log2_delta_lookup;
};

/// jemalloc: sc_data_t
struct SizeClassData
{
    /// Number of tiny size classes.
    unsigned num_tiny;
    /// Number of bins supported by the lookup table.
    int num_large_bins;
    /// Number of small size class bins.
    int num_bins;
    /// Number of size classes.
    int num_sizes;
    /// Number of bits required to store NSIZES.
    int log2_ceil_num_sizes;
    /// Number of size classes that are a multiple of PAGE.
    unsigned num_page_sizes;
    /// Lg of maximum tiny size class (or -1, if none).
    int log2_tiny_max_class;
    /// Maximum size class included in lookup table.
    size_t lookup_max_class;
    /// Maximum small size class.
    size_t small_max_class;
    /// Lg of minimum large size class.
    int log2_large_min_class;
    /// The minimum large size class.
    size_t large_min_class;
    /// Maximum (large) size class.
    size_t large_max_class;
    /// True if the data has been initialized (for debugging only).
    bool initialized;

    SizeClass size_class[SIZE_CLASS_NUM_SIZES];
};

/// jemalloc: reg_size_compute
constexpr size_t regionSizeCompute(int log2_base, int log2_delta, int num_delta)
{
    return (size_t(1) << log2_base) + (size_t(num_delta) << log2_delta);
}

namespace detail
{

/// jemalloc: slab_size. Returns the number of pages in the slab: the smallest page count whose size is an exact
/// multiple of the region size.
constexpr int slabSize(int log2_page, int log2_base, int log2_delta, int num_delta)
{
    size_t page = size_t(1) << log2_page;
    size_t region_size = regionSizeCompute(log2_base, log2_delta, num_delta);

    size_t try_slab_size = page;
    size_t try_num_regions = try_slab_size / region_size;
    size_t perfect_slab_size = 0;
    bool perfect = false;
    while (!perfect)
    {
        perfect_slab_size = try_slab_size;
        size_t perfect_num_regions = try_num_regions;
        try_slab_size += page;
        try_num_regions = try_slab_size / region_size;
        if (perfect_slab_size == perfect_num_regions * region_size)
            perfect = true;
    }
    return int(perfect_slab_size / page);
}

/// jemalloc: size_class
constexpr void sizeClass(
    SizeClass & size_class,
    int log2_max_lookup,
    int log2_page,
    int log2_group_size,
    int index,
    int log2_base,
    int log2_delta,
    int num_delta)
{
    size_class.index = index;
    size_class.log2_base = log2_base;
    size_class.log2_delta = log2_delta;
    size_class.num_delta = num_delta;
    size_t size = regionSizeCompute(log2_base, log2_delta, num_delta);
    size_class.page_size = (size % (size_t(1) << log2_page) == 0);
    if (size < (size_t(1) << (log2_page + log2_group_size)))
    {
        size_class.bin = true;
        size_class.pages = slabSize(log2_page, log2_base, log2_delta, num_delta);
    }
    else
    {
        size_class.bin = false;
        size_class.pages = 0;
    }
    if (size <= (size_t(1) << log2_max_lookup))
        size_class.log2_delta_lookup = log2_delta;
    else
        size_class.log2_delta_lookup = 0;
}

/// jemalloc: size_classes
constexpr void sizeClasses(
    SizeClassData & size_class_data,
    size_t log2_ptr_size,
    int log2_quantum,
    int log2_tiny_min,
    int log2_max_lookup,
    int log2_page,
    int log2_group_size)
{
    int ptr_bits = (1 << log2_ptr_size) * 8;
    int group_size = (1 << log2_group_size);
    int num_tiny = 0;
    int num_large_bins = 0;
    int log2_tiny_max_class = -1;
    int num_bins = 0;
    int num_page_sizes = 0;

    int index = 0;

    int num_delta = 0;
    int log2_base = log2_tiny_min;
    int log2_delta = log2_base;

    /// Outputs that we update as we go.
    size_t lookup_max_class = 0;
    size_t small_max_class = 0;
    int log2_large_min_class = 0;
    size_t large_max_class = 0;

    /// Tiny size classes.
    while (log2_base < log2_quantum)
    {
        SizeClass & size_class = size_class_data.size_class[index];
        sizeClass(size_class, log2_max_lookup, log2_page, log2_group_size, index, log2_base, log2_delta, num_delta);
        if (size_class.log2_delta_lookup != 0)
            num_large_bins = index + 1;
        if (size_class.page_size)
            ++num_page_sizes;
        if (size_class.bin)
            ++num_bins;
        ++num_tiny;
        /// Final written value is correct.
        log2_tiny_max_class = log2_base;
        ++index;
        log2_delta = log2_base;
        ++log2_base;
    }

    /// First non-tiny (pseudo) group.
    if (num_tiny != 0)
    {
        SizeClass & size_class = size_class_data.size_class[index];
        /// The first non-tiny size class has an unusual encoding.
        --log2_base;
        num_delta = 1;
        sizeClass(size_class, log2_max_lookup, log2_page, log2_group_size, index, log2_base, log2_delta, num_delta);
        ++index;
        ++log2_base;
        ++log2_delta;
        if (size_class.page_size)
            ++num_page_sizes;
        if (size_class.bin)
            ++num_bins;
    }
    while (num_delta < group_size)
    {
        SizeClass & size_class = size_class_data.size_class[index];
        sizeClass(size_class, log2_max_lookup, log2_page, log2_group_size, index, log2_base, log2_delta, num_delta);
        ++index;
        ++num_delta;
        if (size_class.page_size)
            ++num_page_sizes;
        if (size_class.bin)
            ++num_bins;
    }

    /// All remaining groups.
    log2_base = log2_base + log2_group_size;
    while (log2_base < ptr_bits - 1)
    {
        num_delta = 1;
        int num_delta_limit;
        if (log2_base == ptr_bits - 2)
            num_delta_limit = group_size - 1;
        else
            num_delta_limit = group_size;
        while (num_delta <= num_delta_limit)
        {
            SizeClass & size_class = size_class_data.size_class[index];
            sizeClass(size_class, log2_max_lookup, log2_page, log2_group_size, index, log2_base, log2_delta, num_delta);
            if (size_class.log2_delta_lookup != 0)
            {
                num_large_bins = index + 1;
                /// Final written value is correct.
                lookup_max_class = (size_t(1) << log2_base) + (size_t(num_delta) << log2_delta);
            }
            if (size_class.page_size)
                ++num_page_sizes;
            if (size_class.bin)
            {
                ++num_bins;
                /// Final written value is correct.
                small_max_class = (size_t(1) << log2_base) + (size_t(num_delta) << log2_delta);
                if (log2_group_size > 0)
                    log2_large_min_class = log2_base + 1;
                else
                    log2_large_min_class = log2_base + 2;
            }
            large_max_class = (size_t(1) << log2_base) + (size_t(num_delta) << log2_delta);
            ++index;
            ++num_delta;
        }
        ++log2_base;
        ++log2_delta;
    }
    /// Additional outputs.
    int num_sizes = index;
    unsigned log2_ceil_num_sizes = log2Ceil(size_t(num_sizes));

    /// Fill in the output data.
    size_class_data.num_tiny = unsigned(num_tiny);
    size_class_data.num_large_bins = num_large_bins;
    size_class_data.num_bins = num_bins;
    size_class_data.num_sizes = num_sizes;
    size_class_data.log2_ceil_num_sizes = int(log2_ceil_num_sizes);
    size_class_data.num_page_sizes = unsigned(num_page_sizes);
    size_class_data.log2_tiny_max_class = log2_tiny_max_class;
    size_class_data.lookup_max_class = lookup_max_class;
    size_class_data.small_max_class = small_max_class;
    size_class_data.log2_large_min_class = log2_large_min_class;
    size_class_data.large_min_class = size_t(1) << log2_large_min_class;
    size_class_data.large_max_class = large_max_class;
}

}

/// jemalloc: sc_data_init
constexpr void sizeClassDataInit(SizeClassData & size_class_data)
{
    detail::sizeClasses(
        size_class_data,
        LG_SIZEOF_PTR,
        LOG2_QUANTUM,
        SIZE_CLASS_LOG2_TINY_MIN,
        SIZE_CLASS_LOG2_MAX_LOOKUP,
        LOG2_PAGE,
        SIZE_CLASS_LOG2_GROUP_SIZE);
    size_class_data.initialized = true;
}

/// Updates slab sizes of the small classes with sizes in [begin, end] to be `pages` pages in length, if possible.
/// Otherwise, does its best to accommodate the request (clamps to the valid range for each class).
/// jemalloc: sc_data_update_slab_size
void sizeClassDataUpdateSlabSize(SizeClassData & data, size_t begin, size_t end, int pages);

/// jemalloc: sc_boot
void sizeClassBoot(SizeClassData & data);

namespace detail
{

consteval SizeClassData makeDefaultSizeClassData()
{
    SizeClassData data{};
    sizeClassDataInit(data);
    return data;
}

}

/// The default size class table (`sc_boot` result before any `slab_sizes` option is applied).
inline constexpr SizeClassData default_size_class_data = detail::makeDefaultSizeClassData();

/// The two computations of the size class parameters (incremental and macros) must agree (`size_class.c:238-252`).
static_assert(default_size_class_data.num_sizes == int(SIZE_CLASS_NUM_SIZES));
static_assert(default_size_class_data.num_bins == int(SIZE_CLASS_NUM_BINS));
static_assert(default_size_class_data.num_tiny == SIZE_CLASS_NUM_TINY);
static_assert(default_size_class_data.num_page_sizes == SIZE_CLASS_NUM_PAGE_SIZES);
static_assert(default_size_class_data.log2_tiny_max_class == SIZE_CLASS_LOG2_TINY_MAX_CLASS);
static_assert(default_size_class_data.lookup_max_class == SIZE_CLASS_LOOKUP_MAX_CLASS);
static_assert(default_size_class_data.small_max_class == SIZE_CLASS_SMALL_MAX_CLASS);
static_assert(default_size_class_data.large_min_class == SIZE_CLASS_LARGE_MIN_CLASS);
static_assert(default_size_class_data.log2_large_min_class == int(SIZE_CLASS_LOG2_LARGE_MIN_CLASS));
static_assert(default_size_class_data.large_max_class == SIZE_CLASS_LARGE_MAX_CLASS);
static_assert(default_size_class_data.log2_ceil_num_sizes == int(log2CeilConst(SIZE_CLASS_NUM_SIZES)));

/// --- Options that affect size computations -------------------------------------------------------------------

/// `opt.disable_large_size_classes` (default true, settable through the `disable_large_size_classes` conf key) is in
/// Options.h.

/// Padding for large allocations: PAGE when `opt.cache_oblivious` (to enable cache index randomization), 0 otherwise.
/// Set by `sizeBoot`; initialized for the default `cache_oblivious = true`.
/// jemalloc: sz_large_pad
extern constinit size_t large_pad;

/// --- Lookup tables (sz.c) --------------------------------------------------------------------------------------

namespace detail
{

/// jemalloc: sz_boot_pind2sz_tab
consteval std::array<size_t, SIZE_CLASS_NUM_PAGE_SIZES + 1> makePageSizeClassIdxToSizeTable(const SizeClassData & size_class_data)
{
    std::array<size_t, SIZE_CLASS_NUM_PAGE_SIZES + 1> table{};
    unsigned page_size_class_idx = 0;
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        const SizeClass & size_class = size_class_data.size_class[i];
        if (size_class.page_size)
        {
            table[page_size_class_idx] = (size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta);
            ++page_size_class_idx;
        }
    }
    /// jemalloc writes `tab[pind]` (not `table[i]`) in this loop; it does not matter because pind == SIZE_CLASS_NUM_PAGE_SIZES here.
    for (unsigned i = page_size_class_idx; i <= SIZE_CLASS_NUM_PAGE_SIZES; ++i)
        table[page_size_class_idx] = size_class_data.large_max_class + PAGE;
    return table;
}

/// jemalloc: sz_boot_index2size_tab
consteval std::array<size_t, SIZE_CLASS_NUM_SIZES> makeIndexToSizeTable(const SizeClassData & size_class_data)
{
    std::array<size_t, SIZE_CLASS_NUM_SIZES> table{};
    for (unsigned i = 0; i < SIZE_CLASS_NUM_SIZES; ++i)
    {
        const SizeClass & size_class = size_class_data.size_class[i];
        table[i] = (size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta);
    }
    return table;
}

inline constexpr size_t SIZE_TO_INDEX_TABLE_SIZE = (SIZE_CLASS_LOOKUP_MAX_CLASS >> SIZE_CLASS_LOG2_TINY_MIN) + 1;

/// jemalloc: sz_boot_size2index_tab. Entry k is the index of the smallest class >= 8k.
consteval std::array<uint8_t, SIZE_TO_INDEX_TABLE_SIZE> makeSizeToIndexTable(const SizeClassData & size_class_data)
{
    std::array<uint8_t, SIZE_TO_INDEX_TABLE_SIZE> table{};
    size_t dst_max = SIZE_TO_INDEX_TABLE_SIZE;
    size_t dst_idx = 0;
    for (unsigned size_class_idx = 0; size_class_idx < SIZE_CLASS_NUM_SIZES && dst_idx < dst_max; ++size_class_idx)
    {
        const SizeClass & size_class = size_class_data.size_class[size_class_idx];
        size_t size = (size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta);
        size_t max_idx = ((size + (size_t(1) << SIZE_CLASS_LOG2_TINY_MIN) - 1) >> SIZE_CLASS_LOG2_TINY_MIN);
        for (; dst_idx <= max_idx && dst_idx < dst_max; ++dst_idx)
            table[dst_idx] = uint8_t(size_class_idx);
    }
    return table;
}

}

/// These tables only depend on the class sizes, which are compile-time constants (the `slab_sizes` option changes
/// only slab page counts), so unlike jemalloc they are built at compile time; `sizeBoot` verifies them in debug builds.

/// jemalloc: sz_pind2sz_tab
alignas(CACHE_LINE) inline constexpr std::array<size_t, SIZE_CLASS_NUM_PAGE_SIZES + 1> page_size_class_idx_to_size_table
    = detail::makePageSizeClassIdxToSizeTable(default_size_class_data);
/// jemalloc: sz_index2size_tab
alignas(CACHE_LINE) inline constexpr std::array<size_t, SIZE_CLASS_NUM_SIZES> index_to_size_table
    = detail::makeIndexToSizeTable(default_size_class_data);
/// jemalloc: sz_size2index_tab. Compressed by dividing sizes by the tiny min size.
alignas(CACHE_LINE) inline constexpr std::array<uint8_t, detail::SIZE_TO_INDEX_TABLE_SIZE> size_to_index_table
    = detail::makeSizeToIndexTable(default_size_class_data);

/// jemalloc: sz_boot. Sets `sz_large_pad`; the tables are constant (see above).
void sizeBoot(const SizeClassData & size_class_data, bool cache_oblivious);

/// --- Size computations (sz.h) ----------------------------------------------------------------------------------

namespace size_classes
{

/// jemalloc: sz_large_size_classes_disabled
ALLOCATOR_ALWAYS_INLINE bool largeSizeClassesDisabled()
{
    return options.disable_large_size_classes;
}

/// Page size to page size index. jemalloc: sz_psz2ind
ALLOCATOR_ALWAYS_INLINE PageSizeClassIdx pageSizeToPageSizeClassIdx(size_t page_size)
{
    ALLOCATOR_ASSERT(page_size > 0);
    if (ALLOCATOR_UNLIKELY(page_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return SIZE_CLASS_NUM_PAGE_SIZES;
    /// x is the lg of the first base >= psz.
    PageSizeClassIdx x = log2Ceil(page_size);
    /// The offset from the first group whose classes are all multiples of PAGE (base == PAGE * SIZE_CLASS_GROUP_SIZE);
    /// starts from 1 for (PAGE * SIZE_CLASS_GROUP_SIZE, PAGE * SIZE_CLASS_GROUP_SIZE * 2].
    PageSizeClassIdx offset_to_first_page_size_group
        = (x < SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_PAGE) ? 0 : x - (SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_PAGE);
    /// Delta for off_to_first_ps_rg == 1 is PAGE, and it doubles for every next group.
    PageSizeClassIdx log2_delta = (offset_to_first_page_size_group == 0) ? LOG2_PAGE : LOG2_PAGE + (offset_to_first_page_size_group - 1);
    /// (psz - 1) handles the case psz % (1 << lg_delta) == 0.
    PageSizeClassIdx offset_in_group = PageSizeClassIdx(((page_size - 1)) >> log2_delta) & (SIZE_CLASS_GROUP_SIZE - 1);
    PageSizeClassIdx base_idx = offset_to_first_page_size_group << SIZE_CLASS_LOG2_GROUP_SIZE;
    PageSizeClassIdx idx = base_idx + offset_in_group;
    return idx;
}

/// jemalloc: sz_pind2sz_compute
constexpr size_t pageSizeClassIdxToSizeCompute(PageSizeClassIdx page_size_class_idx)
{
    if (ALLOCATOR_UNLIKELY(page_size_class_idx == SIZE_CLASS_NUM_PAGE_SIZES))
        return SIZE_CLASS_LARGE_MAX_CLASS + PAGE;
    size_t group = page_size_class_idx >> SIZE_CLASS_LOG2_GROUP_SIZE;
    size_t mod = page_size_class_idx & ((size_t(1) << SIZE_CLASS_LOG2_GROUP_SIZE) - 1);

    size_t group_size_mask = ~((!!group) - size_t(1));
    size_t group_size = ((size_t(1) << (LOG2_PAGE + (SIZE_CLASS_LOG2_GROUP_SIZE - 1))) << group) & group_size_mask;

    size_t shift = (group == 0) ? 1 : group;
    size_t log2_delta = shift + (LOG2_PAGE - 1);
    size_t mod_size = (mod + 1) << log2_delta;

    size_t size = group_size + mod_size;
    return size;
}

/// jemalloc: sz_pind2sz_lookup
ALLOCATOR_ALWAYS_INLINE size_t pageSizeClassIdxToSizeLookup(PageSizeClassIdx page_size_class_idx)
{
    size_t result = page_size_class_idx_to_size_table[page_size_class_idx];
    ALLOCATOR_ASSERT(result == pageSizeClassIdxToSizeCompute(page_size_class_idx));
    return result;
}

/// Page size index to page size. jemalloc: sz_pind2sz
ALLOCATOR_ALWAYS_INLINE size_t pageSizeClassIdxToSize(PageSizeClassIdx page_size_class_idx)
{
    ALLOCATOR_ASSERT(page_size_class_idx < SIZE_CLASS_NUM_PAGE_SIZES + 1);
    return pageSizeClassIdxToSizeLookup(page_size_class_idx);
}

/// Page size to usable page size. jemalloc: sz_psz2u
ALLOCATOR_ALWAYS_INLINE size_t pageSizeToUsableSize(size_t page_size)
{
    if (ALLOCATOR_UNLIKELY(page_size > SIZE_CLASS_LARGE_MAX_CLASS))
        return SIZE_CLASS_LARGE_MAX_CLASS + PAGE;
    size_t x = log2Floor((page_size << 1) - 1);
    size_t log2_delta = (x < SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_PAGE + 1) ? LOG2_PAGE : x - SIZE_CLASS_LOG2_GROUP_SIZE - 1;
    size_t delta = size_t(1) << log2_delta;
    size_t delta_mask = delta - 1;
    size_t usable_size = (page_size + delta_mask) & ~delta_mask;
    return usable_size;
}

/// jemalloc: sz_size2index_compute_inline (and sz_size2index_compute, which is the same).
ALLOCATOR_ALWAYS_INLINE constexpr SizeClassIdx sizeToIndexCompute(size_t size)
{
    if (ALLOCATOR_UNLIKELY(size > SIZE_CLASS_LARGE_MAX_CLASS))
        return SIZE_CLASS_NUM_SIZES;

    if (size == 0)
        return 0;

    if constexpr (SIZE_CLASS_NUM_TINY != 0)
    {
        if (size <= (size_t(1) << SIZE_CLASS_LOG2_TINY_MAX_CLASS))
        {
            SizeClassIdx log2_tiny_min = SIZE_CLASS_LOG2_TINY_MAX_CLASS - SIZE_CLASS_NUM_TINY + 1;
            SizeClassIdx log2_ceil = log2Floor(pow2Ceil(size));
            return (log2_ceil < log2_tiny_min ? 0 : log2_ceil - log2_tiny_min);
        }
    }

    SizeClassIdx x = log2Floor((size << 1) - 1);
    SizeClassIdx shift = (x < SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_QUANTUM) ? 0 : x - (SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_QUANTUM);
    SizeClassIdx group = shift << SIZE_CLASS_LOG2_GROUP_SIZE;

    SizeClassIdx log2_delta = (x < SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_QUANTUM + 1) ? LOG2_QUANTUM : x - SIZE_CLASS_LOG2_GROUP_SIZE - 1;

    size_t delta_inverse_mask = size_t(-1) << log2_delta;
    SizeClassIdx mod = SizeClassIdx((((size - 1) & delta_inverse_mask) >> log2_delta) & ((size_t(1) << SIZE_CLASS_LOG2_GROUP_SIZE) - 1));

    SizeClassIdx index = SIZE_CLASS_NUM_TINY + group + mod;
    return index;
}

/// jemalloc: sz_size2index_lookup_impl
ALLOCATOR_ALWAYS_INLINE SizeClassIdx sizeToIndexLookupImpl(size_t size)
{
    ALLOCATOR_ASSERT(size <= SIZE_CLASS_LOOKUP_MAX_CLASS);
    return size_to_index_table[(size + (size_t(1) << SIZE_CLASS_LOG2_TINY_MIN) - 1) >> SIZE_CLASS_LOG2_TINY_MIN];
}

/// jemalloc: sz_size2index_lookup
ALLOCATOR_ALWAYS_INLINE SizeClassIdx sizeToIndexLookup(size_t size)
{
    SizeClassIdx result = sizeToIndexLookupImpl(size);
    ALLOCATOR_ASSERT(result == sizeToIndexCompute(size));
    return result;
}

/// Size to size class index; SIZE_CLASS_NUM_SIZES if the size is too large. jemalloc: sz_size2index
ALLOCATOR_ALWAYS_INLINE SizeClassIdx sizeToIndex(size_t size)
{
    if (ALLOCATOR_LIKELY(size <= SIZE_CLASS_LOOKUP_MAX_CLASS))
        return sizeToIndexLookup(size);
    return sizeToIndexCompute(size);
}

/// jemalloc: sz_index2size_compute_inline (and sz_index2size_compute, which is the same).
ALLOCATOR_ALWAYS_INLINE constexpr size_t indexToSizeCompute(SizeClassIdx index)
{
    if constexpr (SIZE_CLASS_NUM_TINY > 0)
    {
        if (index < SIZE_CLASS_NUM_TINY)
            return size_t(1) << (SIZE_CLASS_LOG2_TINY_MAX_CLASS - SIZE_CLASS_NUM_TINY + 1 + index);
    }

    size_t reduced_index = index - SIZE_CLASS_NUM_TINY;
    size_t group = reduced_index >> SIZE_CLASS_LOG2_GROUP_SIZE;
    size_t mod = reduced_index & ((size_t(1) << SIZE_CLASS_LOG2_GROUP_SIZE) - 1);

    size_t group_size_mask = ~((!!group) - size_t(1));
    size_t group_size = ((size_t(1) << (LOG2_QUANTUM + (SIZE_CLASS_LOG2_GROUP_SIZE - 1))) << group) & group_size_mask;

    size_t shift = (group == 0) ? 1 : group;
    size_t log2_delta = shift + (LOG2_QUANTUM - 1);
    size_t mod_size = (mod + 1) << log2_delta;

    size_t usable_size = group_size + mod_size;
    return usable_size;
}

/// jemalloc: sz_index2size_lookup_impl
ALLOCATOR_ALWAYS_INLINE size_t indexToSizeLookupImpl(SizeClassIdx index)
{
    return index_to_size_table[index];
}

/// jemalloc: sz_index2size_lookup
ALLOCATOR_ALWAYS_INLINE size_t indexToSizeLookup(SizeClassIdx index)
{
    size_t result = indexToSizeLookupImpl(index);
    ALLOCATOR_ASSERT(result == indexToSizeCompute(index));
    return result;
}

/// Size class index to size, for any index (also large classes when they are disabled).
/// jemalloc: sz_index2size_unsafe
ALLOCATOR_ALWAYS_INLINE size_t indexToSizeUnsafe(SizeClassIdx index)
{
    ALLOCATOR_ASSERT(index < SIZE_CLASS_NUM_SIZES);
    return indexToSizeLookup(index);
}

/// Size class index to size. With large size classes disabled, only indices up to the class of
/// USABLE_SIZE_GROW_SLOW_THRESHOLD are meaningful (asserted). jemalloc: sz_index2size
ALLOCATOR_ALWAYS_INLINE size_t indexToSize(SizeClassIdx index)
{
    ALLOCATOR_ASSERT(!largeSizeClassesDisabled() || index <= sizeToIndex(USABLE_SIZE_GROW_SLOW_THRESHOLD));
    size_t size = indexToSizeUnsafe(index);
    /// With large size classes disabled, the usize above SIZE_CLASS_LARGE_MIN_CLASS should grow by PAGE. However, for sizes
    /// in [SIZE_CLASS_LARGE_MIN_CLASS, USABLE_SIZE_GROW_SLOW_THRESHOLD] the class gap is just PAGE, and tcache caches up to
    /// USABLE_SIZE_GROW_SLOW_THRESHOLD, hence this bound.
    ALLOCATOR_ASSERT(!largeSizeClassesDisabled() || size <= USABLE_SIZE_GROW_SLOW_THRESHOLD);
    return size;
}

/// Index and usable size for `size <= SIZE_CLASS_LOOKUP_MAX_CLASS`. jemalloc: sz_size2index_usize_fastpath
ALLOCATOR_ALWAYS_INLINE void sizeToIndexUsableSizeFastPath(size_t size, SizeClassIdx * idx, size_t * usable_size)
{
    if (__builtin_constant_p(size))
    {
        /// When inlined, the size may become known at compile time, which allows static computation.
        *idx = sizeToIndexCompute(size);
        ALLOCATOR_ASSERT(*idx == sizeToIndexLookupImpl(size));
        *usable_size = indexToSizeCompute(*idx);
        ALLOCATOR_ASSERT(*usable_size == indexToSizeLookupImpl(*idx));
    }
    else
    {
        *idx = sizeToIndexLookupImpl(size);
        *usable_size = indexToSizeLookupImpl(*idx);
    }
}

/// jemalloc: sz_s2u_compute_using_delta
ALLOCATOR_ALWAYS_INLINE size_t sizeToUsableSizeComputeUsingDelta(size_t size)
{
    size_t x = log2Floor((size << 1) - 1);
    size_t log2_delta = (x < SIZE_CLASS_LOG2_GROUP_SIZE + LOG2_QUANTUM + 1) ? LOG2_QUANTUM : x - SIZE_CLASS_LOG2_GROUP_SIZE - 1;
    size_t delta = size_t(1) << log2_delta;
    size_t delta_mask = delta - 1;
    size_t usable_size = (size + delta_mask) & ~delta_mask;
    return usable_size;
}

/// jemalloc: sz_s2u_compute. Returns 0 if the size is too large.
ALLOCATOR_ALWAYS_INLINE size_t sizeToUsableSizeCompute(size_t size)
{
    if (ALLOCATOR_UNLIKELY(size > SIZE_CLASS_LARGE_MAX_CLASS))
        return 0;

    if (size == 0)
        ++size;

    if constexpr (SIZE_CLASS_NUM_TINY > 0)
    {
        if (size <= (size_t(1) << SIZE_CLASS_LOG2_TINY_MAX_CLASS))
        {
            size_t log2_tiny_min = SIZE_CLASS_LOG2_TINY_MAX_CLASS - SIZE_CLASS_NUM_TINY + 1;
            size_t log2_ceil = log2Floor(pow2Ceil(size));
            return (log2_ceil < log2_tiny_min ? (size_t(1) << log2_tiny_min) : (size_t(1) << log2_ceil));
        }
    }

    if (size <= SIZE_CLASS_SMALL_MAX_CLASS || !largeSizeClassesDisabled())
        return sizeToUsableSizeComputeUsingDelta(size);

    /// With large size classes disabled, the usize of a large allocation is the size rounded up to a multiple of
    /// PAGE to minimize the memory overhead.
    size_t usable_size = pageCeiling(size);
    ALLOCATOR_ASSERT(usable_size - size < PAGE);
    return usable_size;
}

/// jemalloc: sz_s2u_lookup
ALLOCATOR_ALWAYS_INLINE size_t sizeToUsableSizeLookup(size_t size)
{
    ALLOCATOR_ASSERT(size < SIZE_CLASS_LARGE_MIN_CLASS);
    size_t result = indexToSizeLookup(sizeToIndexLookup(size));
    ALLOCATOR_ASSERT(result == sizeToUsableSizeCompute(size));
    return result;
}

/// Usable size that would result from allocating an object with the specified size; 0 if too large.
/// jemalloc: sz_s2u
ALLOCATOR_ALWAYS_INLINE size_t sizeToUsableSize(size_t size)
{
    if (ALLOCATOR_LIKELY(size <= SIZE_CLASS_LOOKUP_MAX_CLASS))
        return sizeToUsableSizeLookup(size);
    return sizeToUsableSizeCompute(size);
}

/// Usable size that would result from allocating an object with the specified size and alignment (a power of
/// two); 0 on overflow. The result is not checked against SIZE_CLASS_LARGE_MAX_CLASS. jemalloc: sz_sa2u
ALLOCATOR_ALWAYS_INLINE size_t alignedSizeToUsableSize(size_t size, size_t alignment)
{
    size_t usable_size;

    ALLOCATOR_ASSERT(alignment != 0 && ((alignment - 1) & alignment) == 0);

    /// Try for a small size class.
    if (size <= SIZE_CLASS_SMALL_MAX_CLASS && alignment <= PAGE)
    {
        /// Round size up to the nearest multiple of alignment. Every small size class object is aligned at the
        /// smallest power of two that is non-zero in the base two representation of the size.
        usable_size = sizeToUsableSize(alignmentCeiling(size, alignment));
        if (usable_size < SIZE_CLASS_LARGE_MIN_CLASS)
            return usable_size;
    }

    /// Large size class. Beware of overflow.

    if (ALLOCATOR_UNLIKELY(alignment > SIZE_CLASS_LARGE_MAX_CLASS))
        return 0;

    /// Make sure result is a large size class.
    if (size <= SIZE_CLASS_LARGE_MIN_CLASS)
        usable_size = SIZE_CLASS_LARGE_MIN_CLASS;
    else
    {
        usable_size = sizeToUsableSize(size);
        if (usable_size < size)
        {
            /// size_t overflow.
            return 0;
        }
    }

    /// Calculate the multi-page mapping that large_palloc() would need in order to guarantee the alignment.
    if (usable_size + large_pad + pageCeiling(alignment) - PAGE < usable_size)
    {
        /// size_t overflow.
        return 0;
    }
    return usable_size;
}

/// Whether an allocation of this (usable) size is served from a slab (unless it is sampled for profiling).
/// jemalloc: sz_can_use_slab
ALLOCATOR_ALWAYS_INLINE bool canUseSlab(size_t size)
{
    return size <= SIZE_CLASS_SMALL_MAX_CLASS;
}

/// jemalloc: sz_psz_quantize_floor. `size` is page aligned and > 0.
size_t pageSizeQuantizeFloor(size_t size);

/// jemalloc: sz_psz_quantize_ceil. `size` is page aligned and > 0.
size_t pageSizeQuantizeCeil(size_t size);

}

/// --- Region index division (div.h) ----------------------------------------------------------------------------

/// Computes the index of a region in a slab, given its offset relative to the slab base: for n = i * d, returns i
/// by multiplication with magic = ceil(2^32 / d). Requires n < 2^32 and d > 1.
/// jemalloc: div_info_t (without the `JEMALLOC_DEBUG`-only divisor field)
struct DivisionInfo
{
    uint32_t magic;

    /// jemalloc: div_init
    constexpr void init(size_t d)
    {
        /// Nonsensical.
        ALLOCATOR_ASSERT(d != 0);
        /// This would make the value of magic too high to fit into a uint32_t.
        ALLOCATOR_ASSERT(d != 1);

        uint64_t two_to_k = uint64_t(1) << 32;
        uint32_t m = uint32_t(two_to_k / d);

        /// We want magic = ceil(2^k / d), but C gives us floor; increment unless the result was exact.
        if (two_to_k % d != 0)
            ++m;
        magic = m;
    }

    /// jemalloc: div_compute
    ALLOCATOR_ALWAYS_INLINE size_t compute(size_t n) const
    {
        ALLOCATOR_ASSERT(n <= uint32_t(-1));
        return size_t((uint64_t(n) * uint64_t(magic)) >> 32);
    }
};

static_assert(sizeof(DivisionInfo) == 4);

/// --- Bin metadata (bin_info.h, bin_types.h) --------------------------------------------------------------------

/// jemalloc: BIN_SHARDS_MAX (1 << EDATA_BITS_BINSHARD_WIDTH), N_BIN_SHARDS_DEFAULT
inline constexpr unsigned BIN_SHARDS_MAX = 1u << 6;
inline constexpr unsigned NUM_BIN_SHARDS_DEFAULT = 1;

/// Read-only information associated with each small size class (shared by all arenas). A slab consists of `num_regions`
/// regions of `region_size` bytes back to back from its base, without a header.
/// jemalloc: bin_info_t
struct BinInfo
{
    /// Size of regions in a slab for this bin's size class.
    size_t region_size;
    /// Total size of a slab for this bin's size class.
    size_t slab_size;
    /// Total number of regions in a slab for this bin's size class.
    uint32_t num_regions;
    /// Number of sharded bins in each arena for this size class.
    uint32_t num_shards;
    /// Metadata used to manipulate bitmaps for slabs associated with this bin.
    BitmapInfo bitmap_info;
};

namespace detail
{

/// jemalloc: bin_infos_init
constexpr void binInfosInit(const SizeClassData & size_class_data, const unsigned * bin_shard_sizes, BinInfo * infos)
{
    for (unsigned i = 0; i < SIZE_CLASS_NUM_BINS; ++i)
    {
        BinInfo & bin_info = infos[i];
        const SizeClass & size_class = size_class_data.size_class[i];
        bin_info.region_size = (size_t(1) << size_class.log2_base) + (size_t(size_class.num_delta) << size_class.log2_delta);
        bin_info.slab_size = size_t(size_class.pages << LOG2_PAGE);
        bin_info.num_regions = uint32_t(bin_info.slab_size / bin_info.region_size);
        bin_info.num_shards = bin_shard_sizes[i];
        bin_info.bitmap_info = bitmapInfoInitializer(bin_info.num_regions);
    }
}

consteval std::array<BinInfo, SIZE_CLASS_NUM_BINS> makeDefaultBinInfos()
{
    unsigned shards[SIZE_CLASS_NUM_BINS];
    for (auto & s : shards)
        s = NUM_BIN_SHARDS_DEFAULT;
    std::array<BinInfo, SIZE_CLASS_NUM_BINS> infos{};
    binInfosInit(default_size_class_data, shards, infos.data());
    return infos;
}

}

/// The default bin metadata (before the `slab_sizes` and `bin_shards` options are applied).
inline constexpr std::array<BinInfo, SIZE_CLASS_NUM_BINS> default_bin_infos = detail::makeDefaultBinInfos();

/// jemalloc: bin_infos. Constant-initialized with the defaults; recomputed by `binInfoBoot`, read-only afterwards.
extern constinit std::array<BinInfo, SIZE_CLASS_NUM_BINS> bin_infos;

/// jemalloc: bin_info_boot
void binInfoBoot(const SizeClassData & size_class_data, const unsigned * bin_shard_sizes);

/// Sets the number of shards of the small classes with sizes in [start_size, end_size].
/// Returns true on error (`num_shards` is 0 or greater than BIN_SHARDS_MAX). jemalloc: bin_update_shard_size
bool binUpdateShardSize(unsigned * bin_shard_sizes, size_t start_size, size_t end_size, size_t num_shards);

/// Loads the default number of shards. jemalloc: bin_shard_sizes_boot
void binShardSizesBoot(unsigned * bin_shard_sizes);

}
