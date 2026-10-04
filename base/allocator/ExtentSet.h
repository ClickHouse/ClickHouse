#pragma once

/// A quantized collection of extents with a built-in LRU queue (jemalloc: `eset.h`, `src/eset.c`), and the flat
/// bitmap it uses to find non-empty bins (jemalloc: `fb.h`).
///
/// The set is not thread-safe; synchronization must be done externally (by the owning `ExtentCache` mutex) for
/// mutating operations. The exception is the stats counters (and `npages`), which may be read without locking:
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
using fb_group_t = unsigned long;

/// jemalloc: FB_GROUP_BITS
inline constexpr size_t FB_GROUP_BITS = sizeof(fb_group_t) * CHAR_BIT;

/// jemalloc: FB_NGROUPS
constexpr size_t fbNgroups(size_t nbits)
{
    return nbits / FB_GROUP_BITS + (nbits % FB_GROUP_BITS == 0 ? 0 : 1);
}

/// The flat bitmap: a larger API than `Bitmap` (backwards searches, searching for both set and unset bits), at the
/// cost of slower operations for very large bitmaps. Initialized flat bitmaps start at all-zeros (all bits unset).
/// The number of bits is a compile-time constant (eset is the only user; HPA is dropped).
template <size_t nbits>
struct FlatBitmap
{
    static constexpr size_t ngroups = fbNgroups(nbits);

    fb_group_t groups[ngroups];

    /// jemalloc: fb_init
    void init() { std::memset(groups, 0, sizeof(groups)); }

    /// jemalloc: fb_empty
    bool empty() const
    {
        for (size_t i = 0; i < ngroups; ++i)
            if (groups[i] != 0)
                return false;
        return true;
    }

    /// jemalloc: fb_full
    bool full() const
    {
        size_t trailing_bits = nbits % FB_GROUP_BITS;
        size_t limit = (trailing_bits == 0 ? ngroups : ngroups - 1);
        for (size_t i = 0; i < limit; ++i)
            if (groups[i] != ~fb_group_t(0))
                return false;
        if (trailing_bits == 0)
            return true;
        return groups[ngroups - 1] == (fb_group_t(1) << trailing_bits) - 1;
    }

    /// jemalloc: fb_get
    JE_ALWAYS_INLINE bool get(size_t bit) const
    {
        JE_ASSERT(bit < nbits);
        size_t group_ind = bit / FB_GROUP_BITS;
        size_t bit_ind = bit % FB_GROUP_BITS;
        return bool(groups[group_ind] & (fb_group_t(1) << bit_ind));
    }

    /// jemalloc: fb_set
    JE_ALWAYS_INLINE void set(size_t bit)
    {
        JE_ASSERT(bit < nbits);
        size_t group_ind = bit / FB_GROUP_BITS;
        size_t bit_ind = bit % FB_GROUP_BITS;
        groups[group_ind] |= (fb_group_t(1) << bit_ind);
    }

    /// jemalloc: fb_unset
    JE_ALWAYS_INLINE void unset(size_t bit)
    {
        JE_ASSERT(bit < nbits);
        size_t group_ind = bit / FB_GROUP_BITS;
        size_t bit_ind = bit % FB_GROUP_BITS;
        groups[group_ind] &= ~(fb_group_t(1) << bit_ind);
    }

    /// Sets the `cnt` bits starting at position `start`. Must not have a 0 count.
    /// jemalloc: fb_set_range
    void setRange(size_t start, size_t cnt)
    {
        visit(start, cnt, [](fb_group_t * fb, fb_group_t mask) { *fb |= mask; });
    }

    /// Unsets the `cnt` bits starting at position `start`. Must not have a 0 count.
    /// jemalloc: fb_unset_range
    void unsetRange(size_t start, size_t cnt)
    {
        visit(start, cnt, [](fb_group_t * fb, fb_group_t mask) { *fb &= ~mask; });
    }

    /// The number of set bits in the range of length `cnt` starting at `start`.
    /// jemalloc: fb_scount
    size_t scount(size_t start, size_t cnt) const
    {
        size_t result = 0;
        const_cast<FlatBitmap *>(this)->visit(
            start, cnt, [&result](fb_group_t * fb, fb_group_t mask) { result += size_t(popcount(*fb & mask)); });
        return result;
    }

    /// The number of unset bits in the range of length `cnt` starting at `start`.
    /// jemalloc: fb_ucount
    size_t ucount(size_t start, size_t cnt) const { return cnt - scount(start, cnt); }

    /// The first set bit with an index >= `min_bit`, or `nbits` if there is none.
    /// jemalloc: fb_ffs
    JE_ALWAYS_INLINE size_t ffs(size_t min_bit) const { return size_t(findImpl(min_bit, /* val */ true, /* forward */ true)); }

    /// The first unset bit with an index >= `min_bit`, or `nbits` if there is none.
    /// jemalloc: fb_ffu
    size_t ffu(size_t min_bit) const { return size_t(findImpl(min_bit, /* val */ false, /* forward */ true)); }

    /// The last set bit with an index <= `max_bit`, or -1 if there is none.
    /// jemalloc: fb_fls
    ssize_t fls(size_t max_bit) const { return findImpl(max_bit, /* val */ true, /* forward */ false); }

    /// The last unset bit with an index <= `max_bit`, or -1 if there is none.
    /// jemalloc: fb_flu
    ssize_t flu(size_t max_bit) const { return findImpl(max_bit, /* val */ false, /* forward */ false); }

    /// Tries to find the next contiguous sequence of set bits with a first index >= `start`. If one exists, puts the
    /// earliest bit of the range in `*r_begin`, its length in `*r_len`, and returns true. Otherwise returns false
    /// (without touching the outputs).
    /// jemalloc: fb_srange_iter
    bool srangeIter(size_t start, size_t * r_begin, size_t * r_len) const
    {
        return iterRangeImpl(start, r_begin, r_len, /* val */ true, /* forward */ true);
    }

    /// The same, but searches backwards from `start` (the position returned is still the earliest bit in the range).
    /// jemalloc: fb_srange_riter
    bool srangeRiter(size_t start, size_t * r_begin, size_t * r_len) const
    {
        return iterRangeImpl(start, r_begin, r_len, /* val */ true, /* forward */ false);
    }

    /// jemalloc: fb_urange_iter
    bool urangeIter(size_t start, size_t * r_begin, size_t * r_len) const
    {
        return iterRangeImpl(start, r_begin, r_len, /* val */ false, /* forward */ true);
    }

    /// jemalloc: fb_urange_riter
    bool urangeRiter(size_t start, size_t * r_begin, size_t * r_len) const
    {
        return iterRangeImpl(start, r_begin, r_len, /* val */ false, /* forward */ false);
    }

    /// jemalloc: fb_srange_longest
    size_t srangeLongest() const { return rangeLongestImpl(/* val */ true); }

    /// jemalloc: fb_urange_longest
    size_t urangeLongest() const { return rangeLongestImpl(/* val */ false); }

    /// jemalloc: fb_bit_and
    static void bitAnd(FlatBitmap & dst, const FlatBitmap & src1, const FlatBitmap & src2)
    {
        for (size_t i = 0; i < ngroups; ++i)
            dst.groups[i] = src1.groups[i] & src2.groups[i];
    }

    /// jemalloc: fb_bit_or
    static void bitOr(FlatBitmap & dst, const FlatBitmap & src1, const FlatBitmap & src2)
    {
        for (size_t i = 0; i < ngroups; ++i)
            dst.groups[i] = src1.groups[i] | src2.groups[i];
    }

    /// jemalloc: fb_bit_not
    static void bitNot(FlatBitmap & dst, const FlatBitmap & src)
    {
        for (size_t i = 0; i < ngroups; ++i)
            dst.groups[i] = ~src.groups[i];
    }

private:
    /// Applies a group visitor to each group in the range (potentially modifying it); the mask indicates which bits
    /// are logically part of the visitation.
    /// jemalloc: fb_visit_impl
    template <typename Visitor>
    JE_ALWAYS_INLINE void visit(size_t start, size_t cnt, Visitor && visitor)
    {
        JE_ASSERT(cnt > 0);
        JE_ASSERT(start + cnt <= nbits);
        size_t group_ind = start / FB_GROUP_BITS;
        size_t start_bit_ind = start % FB_GROUP_BITS;
        /// The first group is special; it's the only one we don't start writing to from bit 0.
        size_t first_group_cnt = (start_bit_ind + cnt > FB_GROUP_BITS ? FB_GROUP_BITS - start_bit_ind : cnt);
        /// The first group, where we touch only the high bits; the middle, where all the bits are the same; the last
        /// group, where we touch only the low bits.
        fb_group_t mask = ((~fb_group_t(0)) >> (FB_GROUP_BITS - first_group_cnt)) << start_bit_ind;
        visitor(&groups[group_ind], mask);

        cnt -= first_group_cnt;
        ++group_ind;
        while (cnt > FB_GROUP_BITS)
        {
            visitor(&groups[group_ind], ~fb_group_t(0));
            cnt -= FB_GROUP_BITS;
            ++group_ind;
        }
        if (cnt != 0)
        {
            mask = (~fb_group_t(0)) >> (FB_GROUP_BITS - cnt);
            visitor(&groups[group_ind], mask);
        }
    }

    /// Finds the first bit at position >= `start` (or <= `start` going backwards) with the value `val`. Returns
    /// `nbits` (forward) or -1 (backward) if no such bit exists.
    /// jemalloc: fb_find_impl
    JE_ALWAYS_INLINE ssize_t findImpl(size_t start, bool val, bool forward) const
    {
        JE_ASSERT(start < nbits);
        ssize_t group_ind = ssize_t(start / FB_GROUP_BITS);
        size_t bit_ind = start % FB_GROUP_BITS;

        fb_group_t maybe_invert = (val ? 0 : fb_group_t(-1));

        fb_group_t group = groups[group_ind];
        group ^= maybe_invert;
        if (forward)
        {
            /// Only keep ones in bits bit_ind and above.
            group &= ~((1LU << bit_ind) - 1);
        }
        else
        {
            /// Only keep ones in bits bit_ind and below. (1 << (bit_ind + 1)) - 1 would shift by an invalid amount if
            /// bit_ind is FB_GROUP_BITS - 1.
            group &= ((2LU << bit_ind) - 1);
        }
        ssize_t group_ind_bound = forward ? ssize_t(ngroups) : -1;
        while (group == 0)
        {
            group_ind += forward ? 1 : -1;
            if (group_ind == group_ind_bound)
                return forward ? ssize_t(nbits) : ssize_t(-1);
            group = groups[group_ind];
            group ^= maybe_invert;
        }
        JE_ASSERT(group != 0);
        size_t bit = forward ? jemalloc::ffs(group) : jemalloc::fls(group);
        size_t pos = size_t(group_ind) * FB_GROUP_BITS + bit;
        /// The high bits of a partially filled last group are zeros, so if we're looking for zeros we don't want to
        /// report an invalid result.
        if (forward && !val && pos > nbits)
            return ssize_t(nbits);
        return ssize_t(pos);
    }

    /// Returns whether a range was found.
    /// jemalloc: fb_iter_range_impl
    JE_ALWAYS_INLINE bool iterRangeImpl(size_t start, size_t * r_begin, size_t * r_len, bool val, bool forward) const
    {
        JE_ASSERT(start < nbits);
        ssize_t next_range_begin = findImpl(start, val, forward);
        if ((forward && next_range_begin == ssize_t(nbits)) || (!forward && next_range_begin == ssize_t(-1)))
            return false;
        /// Half open range; the set bits are [begin, end).
        ssize_t next_range_end = findImpl(size_t(next_range_begin), !val, forward);
        if (forward)
        {
            *r_begin = size_t(next_range_begin);
            *r_len = size_t(next_range_end - next_range_begin);
        }
        else
        {
            *r_begin = size_t(next_range_end + 1);
            *r_len = size_t(next_range_begin - next_range_end);
        }
        return true;
    }

    /// jemalloc: fb_range_longest_impl
    size_t rangeLongestImpl(bool val) const
    {
        size_t begin = 0;
        size_t longest_len = 0;
        size_t len = 0;
        while (begin < nbits && iterRangeImpl(begin, &begin, &len, val, /* forward */ true))
        {
            if (len > longest_len)
                longest_len = len;
            begin += len;
        }
        return longest_len;
    }
};

/// --- Extent set (eset.h) --------------------------------------------------------------------------------------------

/// One bin per page size class, plus one for extents larger than `SC_LARGE_MAXCLASS`.
/// jemalloc: ESET_NPSIZES
inline constexpr unsigned ESET_NPSIZES = SC_NPSIZES + 1;

/// jemalloc: eset_bin_t
struct ExtentSetBin
{
    ExtentHeap heap;
    /// We do first-fit across multiple size classes. If we compared against the min element in each heap directly,
    /// we'd take a cache miss per extent we looked at. If we co-locate the summaries, we only take a miss on the
    /// extent we're actually going to return (which is inevitable anyways). Filled in when the bin goes from empty to
    /// non-empty.
    ExtentCmpSummary heap_min;
};

/// jemalloc: eset_bin_stats_t
struct ExtentSetBinStats
{
    std::atomic<size_t> nextents;
    std::atomic<size_t> nbytes;
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
    JE_ALWAYS_INLINE size_t npagesGet() const { return npages.load(std::memory_order_relaxed); }

    /// The number of extents in the given page size index.
    /// jemalloc: eset_nextents_get
    JE_ALWAYS_INLINE size_t nextentsGet(pszind_t pind) const { return bin_stats[pind].nextents.load(std::memory_order_relaxed); }

    /// The sum total bytes of the extents in the given page size index.
    /// jemalloc: eset_nbytes_get
    JE_ALWAYS_INLINE size_t nbytesGet(pszind_t pind) const { return bin_stats[pind].nbytes.load(std::memory_order_relaxed); }

    /// jemalloc: eset_insert
    void insert(Extent * edata);

    /// jemalloc: eset_remove
    void remove(Extent * edata);

    /// Select an extent from this set of the given size and alignment. Returns null if no such item could be found.
    /// jemalloc: eset_fit
    Extent * fit(size_t esize, size_t alignment, bool exact_only, unsigned lg_max_fit);

    /// The LRU-first extent (oldest insertion), or null.
    JE_ALWAYS_INLINE Extent * lruFirst() const { return lru.first(); }

    /// Bitmap for which set bits correspond to non-empty heaps.
    FlatBitmap<ESET_NPSIZES> bitmap;
    /// Quantized per size class heaps of extents.
    ExtentSetBin bins[ESET_NPSIZES];
    ExtentSetBinStats bin_stats[ESET_NPSIZES];
    /// LRU of all extents in heaps.
    ExtentListInactive lru;
    /// Page sum for all extents in heaps.
    std::atomic<size_t> npages{0};
    /// A duplication of the data in the containing ecache. Used only for assertions on the states of the passed-in
    /// extents.
    ExtentState state = extent_state_active;

private:
    /// jemalloc: eset_stats_add
    JE_ALWAYS_INLINE void statsAdd(pszind_t pind, size_t sz)
    {
        size_t cur = bin_stats[pind].nextents.load(std::memory_order_relaxed);
        bin_stats[pind].nextents.store(cur + 1, std::memory_order_relaxed);
        cur = bin_stats[pind].nbytes.load(std::memory_order_relaxed);
        bin_stats[pind].nbytes.store(cur + sz, std::memory_order_relaxed);
    }

    /// jemalloc: eset_stats_sub
    JE_ALWAYS_INLINE void statsSub(pszind_t pind, size_t sz)
    {
        size_t cur = bin_stats[pind].nextents.load(std::memory_order_relaxed);
        bin_stats[pind].nextents.store(cur - 1, std::memory_order_relaxed);
        cur = bin_stats[pind].nbytes.load(std::memory_order_relaxed);
        bin_stats[pind].nbytes.store(cur - sz, std::memory_order_relaxed);
    }

    /// jemalloc: eset_enumerate_alignment_search
    Extent * enumerateAlignmentSearch(size_t size, pszind_t bin_ind, size_t alignment);

    /// jemalloc: eset_enumerate_search
    Extent * enumerateSearch(size_t size, pszind_t bin_ind, bool exact_only, ExtentCmpSummary * ret_summ);

    /// Find an extent with size [min_size, max_size) to satisfy the alignment requirement. For each size, try only
    /// the first extent in the heap.
    /// jemalloc: eset_fit_alignment
    Extent * fitAlignment(size_t min_size, size_t max_size, size_t alignment);

    /// Do first-fit extent selection, i.e. select the oldest/lowest extent that is large enough.
    /// jemalloc: eset_first_fit
    Extent * firstFit(size_t size, bool exact_only, unsigned lg_max_fit);
};

/// Measured from the C build (the size is observable through `stats.metadata`: three `ecache_t` per arena).
static_assert(sizeof(ExtentSet) == (LG_PAGE == 12 ? 9656 : (LG_PAGE == 14 ? 9264 : 8880)));

}
