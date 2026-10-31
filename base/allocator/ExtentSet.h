#pragma once

/// A quantized collection of extents with a built-in LRU queue (jemalloc: `eset.h`, `src/eset.c`), and the flat
/// bitmap it uses to find non-empty bins (jemalloc: `fb.h`).
///
/// The set is not thread-safe; synchronization must be done externally (by the owning `ExtentCache` mutex) for
/// mutating operations. The exception is the stats counters (and `num_pages`), which may be read without locking:
/// they are relaxed atomics that writers update with a load followed by a store (writers hold the mutex).

#include <allocator/Common.h>
#include <allocator/Extent.h>
#include <allocator/SizeClasses.h>

#include <atomic>
#include <climits>
#include <cstring>
#include <sys/types.h>

namespace jemalloc
{

/// --- Flat bitmap (fb.h) ---------------------------------------------------------------------------------------------

/// jemalloc: fb_group_t
using FlatBitmapGroup = unsigned long;

/// jemalloc: FB_GROUP_BITS
inline constexpr size_t FLAT_BITMAP_GROUP_BITS = sizeof(FlatBitmapGroup) * CHAR_BIT;

/// jemalloc: FB_NGROUPS
constexpr size_t flatBitmapNumGroups(size_t num_bits)
{
    return num_bits / FLAT_BITMAP_GROUP_BITS + (num_bits % FLAT_BITMAP_GROUP_BITS == 0 ? 0 : 1);
}

/// The flat bitmap: a larger API than `Bitmap` (backwards searches, searching for both set and unset bits), at the
/// cost of slower operations for very large bitmaps. Initialized flat bitmaps start at all-zeros (all bits unset).
/// The number of bits is a compile-time constant (eset is the only user; HPA is dropped).
template <size_t num_bits>
struct FlatBitmap
{
    static constexpr size_t num_groups = flatBitmapNumGroups(num_bits);

    FlatBitmapGroup groups[num_groups];

    /// jemalloc: fb_init
    void init() { std::memset(groups, 0, sizeof(groups)); }

    /// jemalloc: fb_empty
    bool empty() const
    {
        for (size_t i = 0; i < num_groups; ++i)
            if (groups[i] != 0)
                return false;
        return true;
    }

    /// jemalloc: fb_full
    bool full() const
    {
        size_t trailing_bits = num_bits % FLAT_BITMAP_GROUP_BITS;
        size_t limit = (trailing_bits == 0 ? num_groups : num_groups - 1);
        for (size_t i = 0; i < limit; ++i)
            if (groups[i] != ~FlatBitmapGroup(0))
                return false;
        if (trailing_bits == 0)
            return true;
        return groups[num_groups - 1] == (FlatBitmapGroup(1) << trailing_bits) - 1;
    }

    /// jemalloc: fb_get
    ALLOCATOR_ALWAYS_INLINE bool get(size_t bit) const
    {
        ALLOCATOR_ASSERT(bit < num_bits);
        size_t group_idx = bit / FLAT_BITMAP_GROUP_BITS;
        size_t bit_idx = bit % FLAT_BITMAP_GROUP_BITS;
        return bool(groups[group_idx] & (FlatBitmapGroup(1) << bit_idx));
    }

    /// jemalloc: fb_set
    ALLOCATOR_ALWAYS_INLINE void set(size_t bit)
    {
        ALLOCATOR_ASSERT(bit < num_bits);
        size_t group_idx = bit / FLAT_BITMAP_GROUP_BITS;
        size_t bit_idx = bit % FLAT_BITMAP_GROUP_BITS;
        groups[group_idx] |= (FlatBitmapGroup(1) << bit_idx);
    }

    /// jemalloc: fb_unset
    ALLOCATOR_ALWAYS_INLINE void unset(size_t bit)
    {
        ALLOCATOR_ASSERT(bit < num_bits);
        size_t group_idx = bit / FLAT_BITMAP_GROUP_BITS;
        size_t bit_idx = bit % FLAT_BITMAP_GROUP_BITS;
        groups[group_idx] &= ~(FlatBitmapGroup(1) << bit_idx);
    }

    /// Sets the `count` bits starting at position `start`. Must not have a 0 count.
    /// jemalloc: fb_set_range
    void setRange(size_t start, size_t count)
    {
        visit(start, count, [](FlatBitmapGroup * flat_bitmap, FlatBitmapGroup mask) { *flat_bitmap |= mask; });
    }

    /// Unsets the `count` bits starting at position `start`. Must not have a 0 count.
    /// jemalloc: fb_unset_range
    void unsetRange(size_t start, size_t count)
    {
        visit(start, count, [](FlatBitmapGroup * flat_bitmap, FlatBitmapGroup mask) { *flat_bitmap &= ~mask; });
    }

    /// The number of set bits in the range of length `count` starting at `start`.
    /// jemalloc: fb_scount
    size_t setCount(size_t start, size_t count) const
    {
        size_t result = 0;
        const_cast<FlatBitmap *>(this)->visit(
            start,
            count,
            [&result](FlatBitmapGroup * flat_bitmap, FlatBitmapGroup mask) { result += size_t(popcount(*flat_bitmap & mask)); });
        return result;
    }

    /// The number of unset bits in the range of length `count` starting at `start`.
    /// jemalloc: fb_ucount
    size_t unsetCount(size_t start, size_t count) const { return count - setCount(start, count); }

    /// The first set bit with an index >= `min_bit`, or `num_bits` if there is none.
    /// jemalloc: fb_ffs
    ALLOCATOR_ALWAYS_INLINE size_t findFirstSet(size_t min_bit) const
    {
        return size_t(findImpl(min_bit, /* val */ true, /* forward */ true));
    }

    /// The first unset bit with an index >= `min_bit`, or `num_bits` if there is none.
    /// jemalloc: fb_ffu
    size_t findFirstUnset(size_t min_bit) const { return size_t(findImpl(min_bit, /* val */ false, /* forward */ true)); }

    /// The last set bit with an index <= `max_bit`, or -1 if there is none.
    /// jemalloc: fb_fls
    ssize_t findLastSet(size_t max_bit) const { return findImpl(max_bit, /* val */ true, /* forward */ false); }

    /// The last unset bit with an index <= `max_bit`, or -1 if there is none.
    /// jemalloc: fb_flu
    ssize_t findLastUnset(size_t max_bit) const { return findImpl(max_bit, /* val */ false, /* forward */ false); }

    /// Tries to find the next contiguous sequence of set bits with a first index >= `start`. If one exists, puts the
    /// earliest bit of the range in `*result_begin`, its length in `*r_len`, and returns true. Otherwise returns false
    /// (without touching the outputs).
    /// jemalloc: fb_srange_iter
    bool setRangeIterate(size_t start, size_t * result_begin, size_t * r_len) const
    {
        return iterateRangeImpl(start, result_begin, r_len, /* val */ true, /* forward */ true);
    }

    /// The same, but searches backwards from `start` (the position returned is still the earliest bit in the range).
    /// jemalloc: fb_srange_riter
    bool setRangeReverseIterate(size_t start, size_t * result_begin, size_t * r_len) const
    {
        return iterateRangeImpl(start, result_begin, r_len, /* val */ true, /* forward */ false);
    }

    /// jemalloc: fb_urange_iter
    bool unsetRangeIterate(size_t start, size_t * result_begin, size_t * r_len) const
    {
        return iterateRangeImpl(start, result_begin, r_len, /* val */ false, /* forward */ true);
    }

    /// jemalloc: fb_urange_riter
    bool unsetRangeReverseIterate(size_t start, size_t * result_begin, size_t * r_len) const
    {
        return iterateRangeImpl(start, result_begin, r_len, /* val */ false, /* forward */ false);
    }

    /// jemalloc: fb_srange_longest
    size_t setRangeLongest() const { return rangeLongestImpl(/* val */ true); }

    /// jemalloc: fb_urange_longest
    size_t unsetRangeLongest() const { return rangeLongestImpl(/* val */ false); }

    /// jemalloc: fb_bit_and
    static void bitAnd(FlatBitmap & dst, const FlatBitmap & src1, const FlatBitmap & src2)
    {
        for (size_t i = 0; i < num_groups; ++i)
            dst.groups[i] = src1.groups[i] & src2.groups[i];
    }

    /// jemalloc: fb_bit_or
    static void bitOr(FlatBitmap & dst, const FlatBitmap & src1, const FlatBitmap & src2)
    {
        for (size_t i = 0; i < num_groups; ++i)
            dst.groups[i] = src1.groups[i] | src2.groups[i];
    }

    /// jemalloc: fb_bit_not
    static void bitNot(FlatBitmap & dst, const FlatBitmap & src)
    {
        for (size_t i = 0; i < num_groups; ++i)
            dst.groups[i] = ~src.groups[i];
    }

private:
    /// Applies a group visitor to each group in the range (potentially modifying it); the mask indicates which bits
    /// are logically part of the visitation.
    /// jemalloc: fb_visit_impl
    template <typename Visitor>
    ALLOCATOR_ALWAYS_INLINE void visit(size_t start, size_t count, Visitor && visitor)
    {
        ALLOCATOR_ASSERT(count > 0);
        ALLOCATOR_ASSERT(start + count <= num_bits);
        size_t group_idx = start / FLAT_BITMAP_GROUP_BITS;
        size_t start_bit_idx = start % FLAT_BITMAP_GROUP_BITS;
        /// The first group is special; it's the only one we don't start writing to from bit 0.
        size_t first_group_count = (start_bit_idx + count > FLAT_BITMAP_GROUP_BITS ? FLAT_BITMAP_GROUP_BITS - start_bit_idx : count);
        /// The first group, where we touch only the high bits; the middle, where all the bits are the same; the last
        /// group, where we touch only the low bits.
        FlatBitmapGroup mask = ((~FlatBitmapGroup(0)) >> (FLAT_BITMAP_GROUP_BITS - first_group_count)) << start_bit_idx;
        visitor(&groups[group_idx], mask);

        count -= first_group_count;
        ++group_idx;
        while (count > FLAT_BITMAP_GROUP_BITS)
        {
            visitor(&groups[group_idx], ~FlatBitmapGroup(0));
            count -= FLAT_BITMAP_GROUP_BITS;
            ++group_idx;
        }
        if (count != 0)
        {
            mask = (~FlatBitmapGroup(0)) >> (FLAT_BITMAP_GROUP_BITS - count);
            visitor(&groups[group_idx], mask);
        }
    }

    /// Finds the first bit at position >= `start` (or <= `start` going backwards) with the value `value`. Returns
    /// `num_bits` (forward) or -1 (backward) if no such bit exists.
    /// jemalloc: fb_find_impl
    ALLOCATOR_ALWAYS_INLINE ssize_t findImpl(size_t start, bool value, bool forward) const
    {
        ALLOCATOR_ASSERT(start < num_bits);
        ssize_t group_idx = ssize_t(start / FLAT_BITMAP_GROUP_BITS);
        size_t bit_idx = start % FLAT_BITMAP_GROUP_BITS;

        FlatBitmapGroup maybe_invert = (value ? 0 : FlatBitmapGroup(-1));

        FlatBitmapGroup group = groups[group_idx];
        group ^= maybe_invert;
        if (forward)
        {
            /// Only keep ones in bits bit_ind and above.
            group &= ~((1LU << bit_idx) - 1);
        }
        else
        {
            /// Only keep ones in bits bit_ind and below. (1 << (bit_ind + 1)) - 1 would shift by an invalid amount if
            /// bit_ind is FLAT_BITMAP_GROUP_BITS - 1.
            group &= ((2LU << bit_idx) - 1);
        }
        ssize_t group_idx_bound = forward ? ssize_t(num_groups) : -1;
        while (group == 0)
        {
            group_idx += forward ? 1 : -1;
            if (group_idx == group_idx_bound)
                return forward ? ssize_t(num_bits) : ssize_t(-1);
            group = groups[group_idx];
            group ^= maybe_invert;
        }
        ALLOCATOR_ASSERT(group != 0);
        size_t bit = forward ? jemalloc::findFirstSet(group) : jemalloc::findLastSet(group);
        size_t pos = size_t(group_idx) * FLAT_BITMAP_GROUP_BITS + bit;
        /// The high bits of a partially filled last group are zeros, so if we're looking for zeros we don't want to
        /// report an invalid result.
        if (forward && !value && pos > num_bits)
            return ssize_t(num_bits);
        return ssize_t(pos);
    }

    /// Returns whether a range was found.
    /// jemalloc: fb_iter_range_impl
    ALLOCATOR_ALWAYS_INLINE bool iterateRangeImpl(size_t start, size_t * result_begin, size_t * r_len, bool value, bool forward) const
    {
        ALLOCATOR_ASSERT(start < num_bits);
        ssize_t next_range_begin = findImpl(start, value, forward);
        if ((forward && next_range_begin == ssize_t(num_bits)) || (!forward && next_range_begin == ssize_t(-1)))
            return false;
        /// Half open range; the set bits are [begin, end).
        ssize_t next_range_end = findImpl(size_t(next_range_begin), !value, forward);
        if (forward)
        {
            *result_begin = size_t(next_range_begin);
            *r_len = size_t(next_range_end - next_range_begin);
        }
        else
        {
            *result_begin = size_t(next_range_end + 1);
            *r_len = size_t(next_range_begin - next_range_end);
        }
        return true;
    }

    /// jemalloc: fb_range_longest_impl
    size_t rangeLongestImpl(bool value) const
    {
        size_t begin = 0;
        size_t longest_len = 0;
        size_t len = 0;
        while (begin < num_bits && iterateRangeImpl(begin, &begin, &len, value, /* forward */ true))
        {
            if (len > longest_len)
                longest_len = len;
            begin += len;
        }
        return longest_len;
    }
};

/// --- Extent set (eset.h) --------------------------------------------------------------------------------------------

/// One bin per page size class, plus one for extents larger than `SIZE_CLASS_LARGE_MAX_CLASS`.
/// jemalloc: ESET_NPSIZES
inline constexpr unsigned EXTENT_SET_NUM_PAGE_SIZES = SIZE_CLASS_NUM_PAGE_SIZES + 1;

/// jemalloc: eset_bin_t
struct ExtentSetBin
{
    ExtentHeap heap;
    /// We do first-fit across multiple size classes. If we compared against the min element in each heap directly,
    /// we'd take a cache miss per extent we looked at. If we co-locate the summaries, we only take a miss on the
    /// extent we're actually going to return (which is inevitable anyways). Filled in when the bin goes from empty to
    /// non-empty.
    ExtentComparisonSummary heap_min;
};

/// jemalloc: eset_bin_stats_t
struct ExtentSetBinStats
{
    std::atomic<size_t> num_extents;
    std::atomic<size_t> num_bytes;
};

/// jemalloc: eset_t
class ExtentSet
{
public:
    constexpr ExtentSet() = default;

    ExtentSet(const ExtentSet &) = delete;
    ExtentSet & operator=(const ExtentSet &) = delete;

    /// jemalloc: eset_init
    void init(ExtentState state_);

    /// jemalloc: eset_npages_get
    ALLOCATOR_ALWAYS_INLINE size_t numPagesGet() const { return num_pages.load(std::memory_order_relaxed); }

    /// The number of extents in the given page size index.
    /// jemalloc: eset_nextents_get
    ALLOCATOR_ALWAYS_INLINE size_t numExtentsGet(PageSizeClassIdx page_size_class_idx) const
    {
        return bin_stats[page_size_class_idx].num_extents.load(std::memory_order_relaxed);
    }

    /// The sum total bytes of the extents in the given page size index.
    /// jemalloc: eset_nbytes_get
    ALLOCATOR_ALWAYS_INLINE size_t numBytesGet(PageSizeClassIdx page_size_class_idx) const
    {
        return bin_stats[page_size_class_idx].num_bytes.load(std::memory_order_relaxed);
    }

    /// jemalloc: eset_insert
    void insert(Extent * extent);

    /// jemalloc: eset_remove
    void remove(Extent * extent);

    /// Select an extent from this set of the given size and alignment. Returns null if no such item could be found.
    /// jemalloc: eset_fit
    Extent * fit(size_t extent_size, size_t alignment, bool exact_only, unsigned log2_max_fit);

    /// The LRU-first extent (oldest insertion), or null.
    ALLOCATOR_ALWAYS_INLINE Extent * lruFirst() const { return lru.first(); }

    /// Bitmap for which set bits correspond to non-empty heaps.
    FlatBitmap<EXTENT_SET_NUM_PAGE_SIZES> bitmap;
    /// Quantized per size class heaps of extents.
    ExtentSetBin bins[EXTENT_SET_NUM_PAGE_SIZES];
    ExtentSetBinStats bin_stats[EXTENT_SET_NUM_PAGE_SIZES];
    /// LRU of all extents in heaps.
    ExtentListInactive lru;
    /// Page sum for all extents in heaps.
    std::atomic<size_t> num_pages{0};
    /// A duplication of the data in the containing ecache. Used only for assertions on the states of the passed-in
    /// extents.
    ExtentState state = extent_state_active;

private:
    /// jemalloc: eset_stats_add
    ALLOCATOR_ALWAYS_INLINE void statsAdd(PageSizeClassIdx page_size_class_idx, size_t size)
    {
        size_t current = bin_stats[page_size_class_idx].num_extents.load(std::memory_order_relaxed);
        bin_stats[page_size_class_idx].num_extents.store(current + 1, std::memory_order_relaxed);
        current = bin_stats[page_size_class_idx].num_bytes.load(std::memory_order_relaxed);
        bin_stats[page_size_class_idx].num_bytes.store(current + size, std::memory_order_relaxed);
    }

    /// jemalloc: eset_stats_sub
    ALLOCATOR_ALWAYS_INLINE void statsSub(PageSizeClassIdx page_size_class_idx, size_t size)
    {
        size_t current = bin_stats[page_size_class_idx].num_extents.load(std::memory_order_relaxed);
        bin_stats[page_size_class_idx].num_extents.store(current - 1, std::memory_order_relaxed);
        current = bin_stats[page_size_class_idx].num_bytes.load(std::memory_order_relaxed);
        bin_stats[page_size_class_idx].num_bytes.store(current - size, std::memory_order_relaxed);
    }

    /// jemalloc: eset_enumerate_alignment_search
    Extent * enumerateAlignmentSearch(size_t size, PageSizeClassIdx bin_idx, size_t alignment);

    /// jemalloc: eset_enumerate_search
    Extent * enumerateSearch(size_t size, PageSizeClassIdx bin_idx, bool exact_only, ExtentComparisonSummary * result_summary);

    /// Find an extent with size [min_size, max_size) to satisfy the alignment requirement. For each size, try only
    /// the first extent in the heap.
    /// jemalloc: eset_fit_alignment
    Extent * fitAlignment(size_t min_size, size_t max_size, size_t alignment);

    /// Do first-fit extent selection, i.e. select the oldest/lowest extent that is large enough.
    /// jemalloc: eset_first_fit
    Extent * firstFit(size_t size, bool exact_only, unsigned log2_max_fit);
};

/// Measured from the C build (the size is observable through `stats.metadata`: three `ecache_t` per arena).
static_assert(sizeof(ExtentSet) == (LOG2_PAGE == 12 ? 9656 : (LOG2_PAGE == 14 ? 9264 : 8880)));

}
