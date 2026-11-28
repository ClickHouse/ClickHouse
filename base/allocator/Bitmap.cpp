#include <allocator/Bitmap.h>

#include <cstring>

namespace jemalloc
{

/// jemalloc: bitmap_info_init
template <bool UseTree>
void bitmapInfoInit(BitmapInfoImpl<UseTree> & info, size_t num_bits)
{
    ALLOCATOR_ASSERT(num_bits > 0);
    ALLOCATOR_ASSERT(num_bits <= (size_t(1) << LOG2_BITMAP_MAX_BITS));

    if constexpr (UseTree)
    {
        /// Compute the number of groups necessary to store nbits bits, and progressively work upward through the
        /// levels until reaching a level that requires only one group.
        unsigned i;
        info.levels[0].group_offset = 0;
        size_t group_count = bitmapBitsToGroups(num_bits);
        for (i = 1; group_count > 1; ++i)
        {
            ALLOCATOR_ASSERT(i < BITMAP_MAX_LEVELS);
            info.levels[i].group_offset = info.levels[i - 1].group_offset + group_count;
            group_count = bitmapBitsToGroups(group_count);
        }
        info.levels[i].group_offset = info.levels[i - 1].group_offset + group_count;
        ALLOCATOR_ASSERT(!BITMAP_USE_TREE || info.levels[i].group_offset <= BITMAP_GROUPS_MAX);
        info.num_levels = i;
        info.num_bits = num_bits;
    }
    else
    {
        info.num_groups = bitmapBitsToGroups(num_bits);
        info.num_bits = num_bits;
    }
}

/// jemalloc: bitmap_init
template <bool UseTree>
void bitmapInit(bitmap_t * bitmap, const BitmapInfoImpl<UseTree> & info, bool fill)
{
    /// Bits are actually inverted with regard to the external bitmap interface.

    if (fill)
    {
        /// The "filled" bitmap starts out with all 0 bits.
        std::memset(bitmap, 0, bitmapSize(info));
        return;
    }

    /// The "empty" bitmap starts out with all 1 bits, except for trailing unused bits (if any). Each group uses bit
    /// 0 to correspond to the first logical bit in the group, so extra bits are the most significant bits of the
    /// last group.
    std::memset(bitmap, 0xffU, bitmapSize(info));
    size_t extra = (BITMAP_GROUP_NUM_BITS - (info.num_bits & BITMAP_GROUP_NUM_BITS_MASK)) & BITMAP_GROUP_NUM_BITS_MASK;

    if constexpr (UseTree)
    {
        if (extra != 0)
            bitmap[info.levels[1].group_offset - 1] >>= extra;
        for (unsigned i = 1; i < info.num_levels; ++i)
        {
            size_t group_count = info.levels[i].group_offset - info.levels[i - 1].group_offset;
            extra = (BITMAP_GROUP_NUM_BITS - (group_count & BITMAP_GROUP_NUM_BITS_MASK)) & BITMAP_GROUP_NUM_BITS_MASK;
            if (extra != 0)
                bitmap[info.levels[i + 1].group_offset - 1] >>= extra;
        }
    }
    else
    {
        if (extra != 0)
            bitmap[info.num_groups - 1] >>= extra;
    }
}

template void bitmapInfoInit<false>(BitmapInfoImpl<false> &, size_t);
template void bitmapInfoInit<true>(BitmapInfoImpl<true> &, size_t);
template void bitmapInit<false>(bitmap_t *, const BitmapInfoImpl<false> &, bool);
template void bitmapInit<true>(bitmap_t *, const BitmapInfoImpl<true> &, bool);

}
