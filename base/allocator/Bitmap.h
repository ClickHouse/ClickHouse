#pragma once

/// The slab region bitmap (jemalloc: `bitmap.h`, `bitmap.c`).
///
/// Bits are inverted with regard to the external interface: a physical 1 bit means "free" (unset), a physical 0 bit
/// means "allocated" (set). `bitmapGet` returns true iff the logical bit is set (allocated). Logical bit `j` lives in
/// group `j >> 6`, physical bit `j & 63`.
///
/// Two layouts, selected at compile time from `LOG2_BITMAP_MAX_BITS` exactly like `BITMAP_USE_TREE`:
/// - flat (`LOG2_BITMAP_MAX_BITS - 6 <= 3`, i.e. LOG2_PAGE=12): an array of groups scanned linearly;
/// - tree (LOG2_PAGE=14/16): level 0 holds the bits, every upper level holds one bit per group of the level below,
///   set iff that group is non-zero (has a free bit). The root group is the last group.
///
/// Both layouts are available as `BitmapInfoImpl<false>` / `BitmapInfoImpl<true>` (for testing); the allocator uses
/// `BitmapInfo`, which is the one selected for the configured page size.

#include <allocator/Common.h>
#include <allocator/SizeClassConstants.h>

namespace jemalloc
{

using bitmap_t = unsigned long;
inline constexpr unsigned LOG2_SIZEOF_BITMAP = 3;
static_assert(sizeof(bitmap_t) == (size_t(1) << LOG2_SIZEOF_BITMAP));

/// Maximum bitmap bit count is 2^LOG2_BITMAP_MAX_BITS: determined by the maximum regions per slab, or by the number of
/// extent size classes, whichever is larger.
inline constexpr unsigned LOG2_BITMAP_MAX_BITS = SIZE_CLASS_LOG2_SLAB_MAX_REGIONS > log2CeilConst(SIZE_CLASS_NUM_SIZES)
    ? SIZE_CLASS_LOG2_SLAB_MAX_REGIONS
    : log2CeilConst(SIZE_CLASS_NUM_SIZES);
inline constexpr size_t BITMAP_MAX_BITS = size_t(1) << LOG2_BITMAP_MAX_BITS;

/// Number of bits per group.
inline constexpr unsigned LOG2_BITMAP_GROUP_NUM_BITS = LOG2_SIZEOF_BITMAP + 3;
inline constexpr unsigned BITMAP_GROUP_NUM_BITS = 1u << LOG2_BITMAP_GROUP_NUM_BITS;
inline constexpr unsigned BITMAP_GROUP_NUM_BITS_MASK = BITMAP_GROUP_NUM_BITS - 1;

/// If a brute force linear search would have to call ffs more than 2^3 times, use a tree instead.
inline constexpr bool BITMAP_USE_TREE = int(LOG2_BITMAP_MAX_BITS) - int(LOG2_BITMAP_GROUP_NUM_BITS) > 3;

/// Maximum number of levels of a tree bitmap (hard-coded in jemalloc to the largest supported by the macros).
inline constexpr unsigned BITMAP_MAX_LEVELS = 5;

/// Number of groups required to store a given number of bits (BITMAP_BITS2GROUPS).
constexpr size_t bitmapBitsToGroups(size_t num_bits)
{
    return (num_bits + BITMAP_GROUP_NUM_BITS_MASK) >> LOG2_BITMAP_GROUP_NUM_BITS;
}

namespace detail
{

/// BITMAP_GROUPS_L<level>: number of groups at a particular level for a given number of bits.
constexpr size_t bitmapGroupsAtLevel(size_t num_bits, unsigned level)
{
    size_t groups = bitmapBitsToGroups(num_bits);
    for (unsigned i = 0; i < level; ++i)
        groups = bitmapBitsToGroups(groups);
    return groups;
}

/// BITMAP_GROUPS_<n>_LEVEL: total number of groups assuming `num_levels` levels.
constexpr size_t bitmapGroupsForLevels(size_t num_bits, unsigned num_levels)
{
    size_t total = 0;
    for (unsigned i = 0; i < num_levels; ++i)
        total += bitmapGroupsAtLevel(num_bits, i);
    return total;
}

consteval size_t bitmapGroupsMax()
{
    if constexpr (BITMAP_USE_TREE)
    {
        static_assert(LOG2_BITMAP_MAX_BITS <= LOG2_BITMAP_GROUP_NUM_BITS * 5, "Unsupported bitmap size");
        unsigned num_levels = 1;
        while (LOG2_BITMAP_MAX_BITS > LOG2_BITMAP_GROUP_NUM_BITS * num_levels)
            ++num_levels;
        return bitmapGroupsForLevels(BITMAP_MAX_BITS, num_levels);
    }
    else
        return bitmapBitsToGroups(BITMAP_MAX_BITS);
}

}

/// Maximum number of groups required to support LOG2_BITMAP_MAX_BITS.
inline constexpr size_t BITMAP_GROUPS_MAX = detail::bitmapGroupsMax();

/// jemalloc: bitmap_level_t
struct BitmapLevel
{
    /// Offset of this level's groups within the array of groups.
    size_t group_offset;
};

/// jemalloc: bitmap_info_t
template <bool UseTree>
struct BitmapInfoImpl;

/// The flat layout.
template <>
struct BitmapInfoImpl<false>
{
    /// Logical number of bits in the bitmap.
    size_t num_bits;
    /// Number of groups necessary for nbits.
    size_t num_groups;
};

/// The tree layout.
template <>
struct BitmapInfoImpl<true>
{
    /// Logical number of bits in the bitmap (stored at the bottom level).
    size_t num_bits;
    /// Number of levels necessary for nbits.
    unsigned num_levels;
    /// Only the first (nlevels+1) elements are used, and levels are ordered bottom to top (the bottom level is
    /// stored in levels[0]).
    BitmapLevel levels[BITMAP_MAX_LEVELS + 1];
};

using BitmapInfo = BitmapInfoImpl<BITMAP_USE_TREE>;

static_assert(sizeof(BitmapInfoImpl<false>) == 16);
static_assert(sizeof(BitmapInfoImpl<true>) == 64);

/// jemalloc: BITMAP_INFO_INITIALIZER. Usable at compile time and at run time (with the run time `nbits` the
/// same as jemalloc's use of the macro in `bin_infos_init`). Fills all `BITMAP_MAX_LEVELS + 1` levels.
template <bool UseTree = BITMAP_USE_TREE>
constexpr BitmapInfoImpl<UseTree> bitmapInfoInitializer(size_t num_bits)
{
    if constexpr (UseTree)
    {
        BitmapInfoImpl<true> info{};
        info.num_bits = num_bits;
        size_t l[BITMAP_MAX_LEVELS];
        for (unsigned i = 0; i < BITMAP_MAX_LEVELS; ++i)
            l[i] = detail::bitmapGroupsAtLevel(num_bits, i);
        info.num_levels = unsigned(l[0] > l[1]) + unsigned(l[1] > l[2]) + unsigned(l[2] > l[3]) + unsigned(l[3] > l[4]) + 1;
        info.levels[0].group_offset = 0;
        size_t sum = 0;
        for (unsigned i = 0; i < BITMAP_MAX_LEVELS; ++i)
        {
            sum += l[i];
            info.levels[i + 1].group_offset = sum;
        }
        return info;
    }
    else
    {
        return BitmapInfoImpl<false>{num_bits, bitmapBitsToGroups(num_bits)};
    }
}

/// jemalloc: bitmap_info_init. Only the first (nlevels+1) levels are written. (Unused by the allocator itself.)
template <bool UseTree>
void bitmapInfoInit(BitmapInfoImpl<UseTree> & info, size_t num_bits);

/// jemalloc: bitmap_info_ngroups
template <bool UseTree>
constexpr size_t bitmapInfoNumGroups(const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
        return info.levels[info.num_levels].group_offset;
    else
        return info.num_groups;
}

/// jemalloc: bitmap_size. Size of the bitmap in bytes.
template <bool UseTree>
constexpr size_t bitmapSize(const BitmapInfoImpl<UseTree> & info)
{
    return bitmapInfoNumGroups(info) << LOG2_SIZEOF_BITMAP;
}

/// jemalloc: bitmap_init. `fill = true` makes all bits set (allocated); otherwise all are unset (free).
template <bool UseTree>
void bitmapInit(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, bool fill);

/// jemalloc: bitmap_full
template <bool UseTree>
ALLOCATOR_ALWAYS_INLINE bool bitmapFull(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
    {
        size_t root_group_offset = info.levels[info.num_levels].group_offset - 1;
        bitmap_t rg = bitmap[root_group_offset];
        /// The bitmap is full iff the root group is 0.
        return rg == 0;
    }
    else
    {
        for (size_t i = 0; i < info.num_groups; ++i)
        {
            if (bitmap[i] != 0)
                return false;
        }
        return true;
    }
}

/// jemalloc: bitmap_get. True iff the logical bit is set (the region is allocated).
template <bool UseTree>
ALLOCATOR_ALWAYS_INLINE bool bitmapGet(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    ALLOCATOR_ASSERT(bit < info.num_bits);
    (void)info;
    size_t group_offset = bit >> LOG2_BITMAP_GROUP_NUM_BITS;
    bitmap_t g = bitmap[group_offset];
    return !(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK)));
}

/// jemalloc: bitmap_set. Sets the logical bit (marks the region allocated); it must be unset.
template <bool UseTree>
ALLOCATOR_ALWAYS_INLINE void bitmapSet(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    ALLOCATOR_ASSERT(bit < info.num_bits);
    ALLOCATOR_ASSERT(!bitmapGet(bitmap, info, bit));
    size_t group_offset = bit >> LOG2_BITMAP_GROUP_NUM_BITS;
    bitmap_t * group_ptr = &bitmap[group_offset];
    bitmap_t g = *group_ptr;
    ALLOCATOR_ASSERT(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK)));
    g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK);
    *group_ptr = g;
    ALLOCATOR_ASSERT(bitmapGet(bitmap, info, bit));
    if constexpr (UseTree)
    {
        /// Propagate group state transitions up the tree.
        if (g == 0)
        {
            for (unsigned i = 1; i < info.num_levels; ++i)
            {
                bit = group_offset;
                group_offset = bit >> LOG2_BITMAP_GROUP_NUM_BITS;
                group_ptr = &bitmap[info.levels[i].group_offset + group_offset];
                g = *group_ptr;
                ALLOCATOR_ASSERT(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK)));
                g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK);
                *group_ptr = g;
                if (g != 0)
                    break;
            }
        }
    }
}

/// jemalloc: bitmap_ffu. Find the first unset (free) bit >= `min_bit`; returns `nbits` if there is none.
/// Includes the upstream fix `ef8e512e` (no out-of-range load in the flat variant). Unused by the allocator itself.
template <bool UseTree>
inline size_t bitmapFindFirstUnset(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t min_bit)
{
    ALLOCATOR_ASSERT(min_bit < info.num_bits);

    if constexpr (UseTree)
    {
        /// jemalloc recurses (a tail call) to restart from the next sibling; this is the same as a loop.
        while (true)
        {
            bool restart = false;
            size_t bit = 0;
            for (unsigned level = info.num_levels; level--;)
            {
                size_t log2_bits_per_group = LOG2_BITMAP_GROUP_NUM_BITS * (level + 1);
                bitmap_t group = bitmap[info.levels[level].group_offset + (bit >> log2_bits_per_group)];
                unsigned group_num_mask
                    = unsigned(((min_bit > bit) ? (min_bit - bit) : 0) >> (log2_bits_per_group - LOG2_BITMAP_GROUP_NUM_BITS));
                ALLOCATOR_ASSERT(group_num_mask <= BITMAP_GROUP_NUM_BITS);
                bitmap_t group_mask = ~((1LU << group_num_mask) - 1);
                bitmap_t group_masked = group & group_mask;
                if (group_masked == 0LU)
                {
                    if (group == 0LU)
                        return info.num_bits;
                    /// min_bit was preceded by one or more unset bits in this group, but there are no other unset
                    /// bits in this group. Try again starting at the first bit of the next sibling. This will
                    /// recurse at most once per non-root level.
                    size_t sibling_base = bit + (size_t(1) << log2_bits_per_group);
                    ALLOCATOR_ASSERT(sibling_base > min_bit);
                    ALLOCATOR_ASSERT(sibling_base > bit);
                    if (sibling_base >= info.num_bits)
                        return info.num_bits;
                    min_bit = sibling_base;
                    restart = true;
                    break;
                }
                bit += size_t(findFirstSet(group_masked)) << (log2_bits_per_group - LOG2_BITMAP_GROUP_NUM_BITS);
            }
            if (restart)
                continue;
            ALLOCATOR_ASSERT(bit >= min_bit);
            ALLOCATOR_ASSERT(bit < info.num_bits);
            return bit;
        }
    }
    else
    {
        size_t i = min_bit >> LOG2_BITMAP_GROUP_NUM_BITS;
        bitmap_t g = bitmap[i] & ~((1LU << (min_bit & BITMAP_GROUP_NUM_BITS_MASK)) - 1);
        while (true)
        {
            if (g != 0)
            {
                size_t bit = findFirstSet(g);
                return (i << LOG2_BITMAP_GROUP_NUM_BITS) + bit;
            }
            ++i;
            if (i >= info.num_groups)
                break;
            g = bitmap[i];
        }
        return info.num_bits;
    }
}

/// jemalloc: bitmap_sfu. Set the first unset bit: allocates and returns the lowest free index.
/// The bitmap must not be full.
template <bool UseTree>
ALLOCATOR_ALWAYS_INLINE size_t bitmapSetFirstUnset(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info)
{
    ALLOCATOR_ASSERT(!bitmapFull(bitmap, info));

    size_t bit;
    if constexpr (UseTree)
    {
        unsigned i = info.num_levels - 1;
        bitmap_t g = bitmap[info.levels[i].group_offset];
        bit = findFirstSet(g);
        while (i > 0)
        {
            --i;
            g = bitmap[info.levels[i].group_offset + bit];
            bit = (bit << LOG2_BITMAP_GROUP_NUM_BITS) + findFirstSet(g);
        }
    }
    else
    {
        size_t i = 0;
        bitmap_t g = bitmap[0];
        while (g == 0)
        {
            ++i;
            g = bitmap[i];
        }
        bit = (i << LOG2_BITMAP_GROUP_NUM_BITS) + findFirstSet(g);
    }
    bitmapSet(bitmap, info, bit);
    return bit;
}

/// jemalloc: bitmap_unset. Unsets the logical bit (marks the region free); it must be set.
template <bool UseTree>
ALLOCATOR_ALWAYS_INLINE void bitmapUnset(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    ALLOCATOR_ASSERT(bit < info.num_bits);
    ALLOCATOR_ASSERT(bitmapGet(bitmap, info, bit));
    size_t group_offset = bit >> LOG2_BITMAP_GROUP_NUM_BITS;
    bitmap_t * group_ptr = &bitmap[group_offset];
    bitmap_t g = *group_ptr;
    bool propagate = (g == 0);
    ALLOCATOR_ASSERT((g & (bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK))) == 0);
    g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK);
    *group_ptr = g;
    ALLOCATOR_ASSERT(!bitmapGet(bitmap, info, bit));
    if constexpr (UseTree)
    {
        /// Propagate group state transitions up the tree.
        if (propagate)
        {
            for (unsigned i = 1; i < info.num_levels; ++i)
            {
                bit = group_offset;
                group_offset = bit >> LOG2_BITMAP_GROUP_NUM_BITS;
                group_ptr = &bitmap[info.levels[i].group_offset + group_offset];
                g = *group_ptr;
                propagate = (g == 0);
                ALLOCATOR_ASSERT((g & (bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK))) == 0);
                g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NUM_BITS_MASK);
                *group_ptr = g;
                if (!propagate)
                    break;
            }
        }
    }
    else
        (void)propagate;
}

}
