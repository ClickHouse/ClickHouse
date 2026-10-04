#pragma once

/// The slab region bitmap (jemalloc: `bitmap.h`, `bitmap.c`).
///
/// Bits are inverted with regard to the external interface: a physical 1 bit means "free" (unset), a physical 0 bit
/// means "allocated" (set). `bitmapGet` returns true iff the logical bit is set (allocated). Logical bit `j` lives in
/// group `j >> 6`, physical bit `j & 63`.
///
/// Two layouts, selected at compile time from `LG_BITMAP_MAXBITS` exactly like `BITMAP_USE_TREE`:
/// - flat (`LG_BITMAP_MAXBITS - 6 <= 3`, i.e. LG_PAGE=12): an array of groups scanned linearly;
/// - tree (LG_PAGE=14/16): level 0 holds the bits, every upper level holds one bit per group of the level below,
///   set iff that group is non-zero (has a free bit). The root group is the last group.
///
/// Both layouts are available as `BitmapInfoImpl<false>` / `BitmapInfoImpl<true>` (for testing); the allocator uses
/// `BitmapInfo`, which is the one selected for the configured page size.

#include <allocator/Common.h>
#include <allocator/SizeClassConstants.h>

namespace jemalloc
{

using bitmap_t = unsigned long;
inline constexpr unsigned LG_SIZEOF_BITMAP = 3;
static_assert(sizeof(bitmap_t) == (size_t(1) << LG_SIZEOF_BITMAP));

/// Maximum bitmap bit count is 2^LG_BITMAP_MAXBITS: determined by the maximum regions per slab, or by the number of
/// extent size classes, whichever is larger.
inline constexpr unsigned LG_BITMAP_MAXBITS
    = SC_LG_SLAB_MAXREGS > lgCeilConst(SC_NSIZES) ? SC_LG_SLAB_MAXREGS : lgCeilConst(SC_NSIZES);
inline constexpr size_t BITMAP_MAXBITS = size_t(1) << LG_BITMAP_MAXBITS;

/// Number of bits per group.
inline constexpr unsigned LG_BITMAP_GROUP_NBITS = LG_SIZEOF_BITMAP + 3;
inline constexpr unsigned BITMAP_GROUP_NBITS = 1u << LG_BITMAP_GROUP_NBITS;
inline constexpr unsigned BITMAP_GROUP_NBITS_MASK = BITMAP_GROUP_NBITS - 1;

/// If a brute force linear search would have to call ffs more than 2^3 times, use a tree instead.
inline constexpr bool BITMAP_USE_TREE = int(LG_BITMAP_MAXBITS) - int(LG_BITMAP_GROUP_NBITS) > 3;

/// Maximum number of levels of a tree bitmap (hard-coded in jemalloc to the largest supported by the macros).
inline constexpr unsigned BITMAP_MAX_LEVELS = 5;

/// Number of groups required to store a given number of bits (BITMAP_BITS2GROUPS).
constexpr size_t bitmapBitsToGroups(size_t nbits)
{
    return (nbits + BITMAP_GROUP_NBITS_MASK) >> LG_BITMAP_GROUP_NBITS;
}

namespace detail
{

/// BITMAP_GROUPS_L<level>: number of groups at a particular level for a given number of bits.
constexpr size_t bitmapGroupsAtLevel(size_t nbits, unsigned level)
{
    size_t groups = bitmapBitsToGroups(nbits);
    for (unsigned i = 0; i < level; ++i)
        groups = bitmapBitsToGroups(groups);
    return groups;
}

/// BITMAP_GROUPS_<n>_LEVEL: total number of groups assuming `nlevels` levels.
constexpr size_t bitmapGroupsForLevels(size_t nbits, unsigned nlevels)
{
    size_t total = 0;
    for (unsigned i = 0; i < nlevels; ++i)
        total += bitmapGroupsAtLevel(nbits, i);
    return total;
}

consteval size_t bitmapGroupsMax()
{
    if constexpr (BITMAP_USE_TREE)
    {
        static_assert(LG_BITMAP_MAXBITS <= LG_BITMAP_GROUP_NBITS * 5, "Unsupported bitmap size");
        unsigned nlevels = 1;
        while (LG_BITMAP_MAXBITS > LG_BITMAP_GROUP_NBITS * nlevels)
            ++nlevels;
        return bitmapGroupsForLevels(BITMAP_MAXBITS, nlevels);
    }
    else
        return bitmapBitsToGroups(BITMAP_MAXBITS);
}

}

/// Maximum number of groups required to support LG_BITMAP_MAXBITS.
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
    size_t nbits;
    /// Number of groups necessary for nbits.
    size_t ngroups;
};

/// The tree layout.
template <>
struct BitmapInfoImpl<true>
{
    /// Logical number of bits in the bitmap (stored at the bottom level).
    size_t nbits;
    /// Number of levels necessary for nbits.
    unsigned nlevels;
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
constexpr BitmapInfoImpl<UseTree> bitmapInfoInitializer(size_t nbits)
{
    if constexpr (UseTree)
    {
        BitmapInfoImpl<true> info{};
        info.nbits = nbits;
        size_t l[BITMAP_MAX_LEVELS];
        for (unsigned i = 0; i < BITMAP_MAX_LEVELS; ++i)
            l[i] = detail::bitmapGroupsAtLevel(nbits, i);
        info.nlevels = unsigned(l[0] > l[1]) + unsigned(l[1] > l[2]) + unsigned(l[2] > l[3]) + unsigned(l[3] > l[4]) + 1;
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
        return BitmapInfoImpl<false>{nbits, bitmapBitsToGroups(nbits)};
    }
}

/// jemalloc: bitmap_info_init. Only the first (nlevels+1) levels are written. (Unused by the allocator itself.)
template <bool UseTree>
void bitmapInfoInit(BitmapInfoImpl<UseTree> & info, size_t nbits);

/// jemalloc: bitmap_info_ngroups
template <bool UseTree>
constexpr size_t bitmapInfoNumGroups(const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
        return info.levels[info.nlevels].group_offset;
    else
        return info.ngroups;
}

/// jemalloc: bitmap_size. Size of the bitmap in bytes.
template <bool UseTree>
constexpr size_t bitmapSize(const BitmapInfoImpl<UseTree> & info)
{
    return bitmapInfoNumGroups(info) << LG_SIZEOF_BITMAP;
}

/// jemalloc: bitmap_init. `fill = true` makes all bits set (allocated); otherwise all are unset (free).
template <bool UseTree>
void bitmapInit(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, bool fill);

/// jemalloc: bitmap_full
template <bool UseTree>
JE_ALWAYS_INLINE bool bitmapFull(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info)
{
    if constexpr (UseTree)
    {
        size_t rgoff = info.levels[info.nlevels].group_offset - 1;
        bitmap_t rg = bitmap[rgoff];
        /// The bitmap is full iff the root group is 0.
        return rg == 0;
    }
    else
    {
        for (size_t i = 0; i < info.ngroups; ++i)
        {
            if (bitmap[i] != 0)
                return false;
        }
        return true;
    }
}

/// jemalloc: bitmap_get. True iff the logical bit is set (the region is allocated).
template <bool UseTree>
JE_ALWAYS_INLINE bool bitmapGet(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    JE_ASSERT(bit < info.nbits);
    (void)info;
    size_t goff = bit >> LG_BITMAP_GROUP_NBITS;
    bitmap_t g = bitmap[goff];
    return !(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK)));
}

/// jemalloc: bitmap_set. Sets the logical bit (marks the region allocated); it must be unset.
template <bool UseTree>
JE_ALWAYS_INLINE void bitmapSet(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    JE_ASSERT(bit < info.nbits);
    JE_ASSERT(!bitmapGet(bitmap, info, bit));
    size_t goff = bit >> LG_BITMAP_GROUP_NBITS;
    bitmap_t * gp = &bitmap[goff];
    bitmap_t g = *gp;
    JE_ASSERT(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK)));
    g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK);
    *gp = g;
    JE_ASSERT(bitmapGet(bitmap, info, bit));
    if constexpr (UseTree)
    {
        /// Propagate group state transitions up the tree.
        if (g == 0)
        {
            for (unsigned i = 1; i < info.nlevels; ++i)
            {
                bit = goff;
                goff = bit >> LG_BITMAP_GROUP_NBITS;
                gp = &bitmap[info.levels[i].group_offset + goff];
                g = *gp;
                JE_ASSERT(g & (bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK)));
                g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK);
                *gp = g;
                if (g != 0)
                    break;
            }
        }
    }
}

/// jemalloc: bitmap_ffu. Find the first unset (free) bit >= `min_bit`; returns `nbits` if there is none.
/// Includes the upstream fix `ef8e512e` (no out-of-range load in the flat variant). Unused by the allocator itself.
template <bool UseTree>
inline size_t bitmapFfu(const bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t min_bit)
{
    JE_ASSERT(min_bit < info.nbits);

    if constexpr (UseTree)
    {
        /// jemalloc recurses (a tail call) to restart from the next sibling; this is the same as a loop.
        while (true)
        {
            bool restart = false;
            size_t bit = 0;
            for (unsigned level = info.nlevels; level--;)
            {
                size_t lg_bits_per_group = LG_BITMAP_GROUP_NBITS * (level + 1);
                bitmap_t group = bitmap[info.levels[level].group_offset + (bit >> lg_bits_per_group)];
                unsigned group_nmask
                    = unsigned(((min_bit > bit) ? (min_bit - bit) : 0) >> (lg_bits_per_group - LG_BITMAP_GROUP_NBITS));
                JE_ASSERT(group_nmask <= BITMAP_GROUP_NBITS);
                bitmap_t group_mask = ~((1LU << group_nmask) - 1);
                bitmap_t group_masked = group & group_mask;
                if (group_masked == 0LU)
                {
                    if (group == 0LU)
                        return info.nbits;
                    /// min_bit was preceded by one or more unset bits in this group, but there are no other unset
                    /// bits in this group. Try again starting at the first bit of the next sibling. This will
                    /// recurse at most once per non-root level.
                    size_t sib_base = bit + (size_t(1) << lg_bits_per_group);
                    JE_ASSERT(sib_base > min_bit);
                    JE_ASSERT(sib_base > bit);
                    if (sib_base >= info.nbits)
                        return info.nbits;
                    min_bit = sib_base;
                    restart = true;
                    break;
                }
                bit += size_t(ffs(group_masked)) << (lg_bits_per_group - LG_BITMAP_GROUP_NBITS);
            }
            if (restart)
                continue;
            JE_ASSERT(bit >= min_bit);
            JE_ASSERT(bit < info.nbits);
            return bit;
        }
    }
    else
    {
        size_t i = min_bit >> LG_BITMAP_GROUP_NBITS;
        bitmap_t g = bitmap[i] & ~((1LU << (min_bit & BITMAP_GROUP_NBITS_MASK)) - 1);
        while (true)
        {
            if (g != 0)
            {
                size_t bit = ffs(g);
                return (i << LG_BITMAP_GROUP_NBITS) + bit;
            }
            ++i;
            if (i >= info.ngroups)
                break;
            g = bitmap[i];
        }
        return info.nbits;
    }
}

/// jemalloc: bitmap_sfu. Set the first unset bit: allocates and returns the lowest free index.
/// The bitmap must not be full.
template <bool UseTree>
JE_ALWAYS_INLINE size_t bitmapSfu(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info)
{
    JE_ASSERT(!bitmapFull(bitmap, info));

    size_t bit;
    if constexpr (UseTree)
    {
        unsigned i = info.nlevels - 1;
        bitmap_t g = bitmap[info.levels[i].group_offset];
        bit = ffs(g);
        while (i > 0)
        {
            --i;
            g = bitmap[info.levels[i].group_offset + bit];
            bit = (bit << LG_BITMAP_GROUP_NBITS) + ffs(g);
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
        bit = (i << LG_BITMAP_GROUP_NBITS) + ffs(g);
    }
    bitmapSet(bitmap, info, bit);
    return bit;
}

/// jemalloc: bitmap_unset. Unsets the logical bit (marks the region free); it must be set.
template <bool UseTree>
JE_ALWAYS_INLINE void bitmapUnset(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, size_t bit)
{
    JE_ASSERT(bit < info.nbits);
    JE_ASSERT(bitmapGet(bitmap, info, bit));
    size_t goff = bit >> LG_BITMAP_GROUP_NBITS;
    bitmap_t * gp = &bitmap[goff];
    bitmap_t g = *gp;
    bool propagate = (g == 0);
    JE_ASSERT((g & (bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK))) == 0);
    g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK);
    *gp = g;
    JE_ASSERT(!bitmapGet(bitmap, info, bit));
    if constexpr (UseTree)
    {
        /// Propagate group state transitions up the tree.
        if (propagate)
        {
            for (unsigned i = 1; i < info.nlevels; ++i)
            {
                bit = goff;
                goff = bit >> LG_BITMAP_GROUP_NBITS;
                gp = &bitmap[info.levels[i].group_offset + goff];
                g = *gp;
                propagate = (g == 0);
                JE_ASSERT((g & (bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK))) == 0);
                g ^= bitmap_t(1) << (bit & BITMAP_GROUP_NBITS_MASK);
                *gp = g;
                if (!propagate)
                    break;
            }
        }
    }
    else
        (void)propagate;
}

}
