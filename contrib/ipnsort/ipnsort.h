// ipnsort - C++ port of the Rust ipnsort (Instruction-Parallel-Network unstable sort)
// by Lukas Bergdoll & Orson Peters. Original: https://github.com/Voultapher/sort-research-rs
// Dual MIT / Apache-2.0 (same as the original).
//
// Unstable, in-place, O(n log n) worst-case (pattern-defeating quicksort with a heapsort
// fallback, glidesort pivot selection and sorting-network small-sort).
//
// API: ipnsort::sort(T* first, T* last, Less less)  where  less(a, b) -> bool  (a < b).
//
// Performance-critical paths (sorting networks, branchless lomuto-cyclic partition,
// branchless bidirectional merge) are used for trivially-copyable T. Other types use a
// correct move-based path (insertion sort + Hoare-cyclic partition + heapsort).

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <type_traits>
#include <utility>

namespace ipnsort
{
namespace detail
{

inline constexpr std::size_t SMALL_SORT_FALLBACK_THRESHOLD = 16;
inline constexpr std::size_t SMALL_SORT_GENERAL_THRESHOLD = 32;
inline constexpr std::size_t SMALL_SORT_GENERAL_SCRATCH_LEN = SMALL_SORT_GENERAL_THRESHOLD + 16;
inline constexpr std::size_t SMALL_SORT_NETWORK_THRESHOLD = 32;
inline constexpr std::size_t MAX_STACK_ARRAY_SIZE = 4096;
inline constexpr std::size_t MAX_BRANCHLESS_PARTITION_SIZE = 96;

template <typename T>
inline constexpr bool is_copy_v = std::is_trivially_copyable_v<T>;

/// Uninitialized single-element storage; lets scratch slots work for trivially-copyable types
/// that are not default-constructible (e.g. a deleted default constructor). Access `.value`.
template <typename T>
union Uninit
{
    T value;
    Uninit() {}  // NOLINT(modernize-use-equals-default)
    ~Uninit() {}  // NOLINT(modernize-use-equals-default)
};

/// Bitwise relocate one element (trivially-copyable bit move).
template <typename T>
inline void bit_copy(T * dst, const T * src) noexcept
{
    std::memcpy(static_cast<void *>(dst), static_cast<const void *>(src), sizeof(T));
}

// ---- floor(log2) -------------------------------------------------------------------------

inline std::uint32_t ilog2(std::size_t x) noexcept
{
    std::uint32_t r = 0;
    while (x >>= 1)
        ++r;
    return r;
}

// ---- small-sort selection ----------------------------------------------------------------

template <typename T>
constexpr bool has_efficient_in_place_swap() noexcept
{
    return sizeof(T) <= sizeof(std::uint64_t);
}

enum class SmallSortKind { Fallback, General, Network };

template <typename T>
constexpr SmallSortKind choose_small_sort() noexcept
{
#if defined(IPNSORT_FORCE_FALLBACK)
    return SmallSortKind::Fallback;
#elif defined(IPNSORT_FORCE_GENERAL)
    if constexpr (!is_copy_v<T>) return SmallSortKind::Fallback; else return SmallSortKind::General;
#else
    if constexpr (!is_copy_v<T>)
        return SmallSortKind::Fallback;
    else if (has_efficient_in_place_swap<T>() && sizeof(T) * SMALL_SORT_NETWORK_THRESHOLD <= MAX_STACK_ARRAY_SIZE)
        return SmallSortKind::Network;
    else if (sizeof(T) * SMALL_SORT_GENERAL_SCRATCH_LEN <= MAX_STACK_ARRAY_SIZE)
        return SmallSortKind::General;
    else
        return SmallSortKind::Fallback;
#endif
}

template <typename T>
constexpr std::size_t small_sort_threshold() noexcept
{
    switch (choose_small_sort<T>())
    {
        case SmallSortKind::Network: return SMALL_SORT_NETWORK_THRESHOLD;
        case SmallSortKind::General: return SMALL_SORT_GENERAL_THRESHOLD;
        case SmallSortKind::Fallback: return SMALL_SORT_FALLBACK_THRESHOLD;
    }
    return SMALL_SORT_FALLBACK_THRESHOLD;
}

// ---- insertion sort (move-based; works for all types) ------------------------------------

/// Sorts [begin, tail] assuming [begin, tail) is sorted. begin < tail.
template <typename T, typename F>
inline void insert_tail(T * begin, T * tail, F & is_less)
{
    if (!is_less(*tail, *(tail - 1)))
        return;

    T tmp = std::move(*tail);
    T * gap = tail;
    T * sift = tail - 1;
    while (true)
    {
        *gap = std::move(*sift);
        gap = sift;
        if (sift == begin)
            break;
        --sift;
        if (!is_less(tmp, *sift))
            break;
    }
    *gap = std::move(tmp);
}

/// Sort v[0..len] assuming v[0..offset] is already sorted.
template <typename T, typename F>
inline void insertion_sort_shift_left(T * v, std::size_t len, std::size_t offset, F & is_less)
{
    if (offset == 0 || offset > len)
        return;
    for (T * tail = v + offset; tail != v + len; ++tail)
        insert_tail(v, tail, is_less);
}

// ---- find existing run -------------------------------------------------------------------

template <typename T, typename F>
inline std::size_t find_existing_run(const T * v, std::size_t len, bool & strictly_descending, F & is_less)
{
    strictly_descending = false;
    if (len < 2)
        return len;

    std::size_t run_len = 2;
    strictly_descending = is_less(v[1], v[0]);
    if (strictly_descending)
        while (run_len < len && is_less(v[run_len], v[run_len - 1]))
            ++run_len;
    else
        while (run_len < len && !is_less(v[run_len], v[run_len - 1]))
            ++run_len;
    return run_len;
}

// ---- pivot selection (glidesort pseudomedian) --------------------------------------------

inline constexpr std::size_t PSEUDO_MEDIAN_REC_THRESHOLD = 64;

template <typename T, typename F>
inline const T * median3(const T * a, const T * b, const T * c, F & is_less)
{
    bool x = is_less(*a, *b);
    bool y = is_less(*a, *c);
    if (x == y)
    {
        bool z = is_less(*b, *c);
        return (z ^ x) ? c : b;
    }
    return a;
}

template <typename T, typename F>
inline const T * median3_rec(const T * a, const T * b, const T * c, std::size_t n, F & is_less)
{
    if (n * 8 >= PSEUDO_MEDIAN_REC_THRESHOLD)
    {
        std::size_t n8 = n / 8;
        a = median3_rec(a, a + n8 * 4, a + n8 * 7, n8, is_less);
        b = median3_rec(b, b + n8 * 4, b + n8 * 7, n8, is_less);
        c = median3_rec(c, c + n8 * 4, c + n8 * 7, n8, is_less);
    }
    return median3(a, b, c, is_less);
}

template <typename T, typename F>
inline std::size_t choose_pivot(T * v, std::size_t len, F & is_less)
{
    std::size_t len_div_8 = len / 8;
    const T * a = v;
    const T * b = v + len_div_8 * 4;
    const T * c = v + len_div_8 * 7;
    const T * res = (len < PSEUDO_MEDIAN_REC_THRESHOLD)
        ? median3(a, b, c, is_less)
        : median3_rec(a, b, c, len_div_8, is_less);
    return static_cast<std::size_t>(res - v);
}

// ---- heapsort ----------------------------------------------------------------------------

template <typename T, typename F>
inline void sift_down(T * v, std::size_t len, std::size_t node, F & is_less)
{
    while (true)
    {
        std::size_t child = 2 * node + 1;
        if (child >= len)
            break;
        if (child + 1 < len)
            child += static_cast<std::size_t>(is_less(v[child], v[child + 1]));
        if (!is_less(v[node], v[child]))
            break;
        std::swap(v[node], v[child]);
        node = child;
    }
}

template <typename T, typename F>
inline void heapsort(T * v, std::size_t len, F & is_less)
{
    for (std::size_t i = len + len / 2; i-- > 0;)
    {
        std::size_t sift_idx;
        if (i >= len)
            sift_idx = i - len;
        else
        {
            std::swap(v[0], v[i]);
            sift_idx = 0;
        }
        sift_down(v, i < len ? i : len, sift_idx, is_less);
    }
}

// ============================ trivially-copyable fast paths ===============================

template <typename T, typename F>
inline void swap_if_less(T * base, std::size_t a_pos, std::size_t b_pos, F & is_less)
{
    T * va = base + a_pos;
    T * vb = base + b_pos;
    bool should_swap = is_less(*vb, *va);
    T * left_swap = should_swap ? vb : va;
    T * right_swap = should_swap ? va : vb;
    Uninit<T> tmp;
    bit_copy(&tmp.value, right_swap);
    // In the no-swap case `left_swap == va`, so this copies an element onto itself. That is
    // overlapping (in fact identical) storage, where `std::memcpy` is undefined, so use an
    // overlap-safe move here, matching the original's `ptr::copy` (as opposed to the
    // non-overlapping `bit_copy` used everywhere else). For a fixed `sizeof(T)` the compiler
    // lowers this to the same load/store as `memcpy`, so the small-sort network stays branchless.
    std::memmove(static_cast<void *>(va), static_cast<const void *>(left_swap), sizeof(T));
    bit_copy(vb, &tmp.value);
}

template <typename T, typename F>
inline void sort9_optimal(T * v, F & is_less)
{
    swap_if_less(v, 0, 3, is_less); swap_if_less(v, 1, 7, is_less); swap_if_less(v, 2, 5, is_less);
    swap_if_less(v, 4, 8, is_less); swap_if_less(v, 0, 7, is_less); swap_if_less(v, 2, 4, is_less);
    swap_if_less(v, 3, 8, is_less); swap_if_less(v, 5, 6, is_less); swap_if_less(v, 0, 2, is_less);
    swap_if_less(v, 1, 3, is_less); swap_if_less(v, 4, 5, is_less); swap_if_less(v, 7, 8, is_less);
    swap_if_less(v, 1, 4, is_less); swap_if_less(v, 3, 6, is_less); swap_if_less(v, 5, 7, is_less);
    swap_if_less(v, 0, 1, is_less); swap_if_less(v, 2, 4, is_less); swap_if_less(v, 3, 5, is_less);
    swap_if_less(v, 6, 8, is_less); swap_if_less(v, 2, 3, is_less); swap_if_less(v, 4, 5, is_less);
    swap_if_less(v, 6, 7, is_less); swap_if_less(v, 1, 2, is_less); swap_if_less(v, 3, 4, is_less);
    swap_if_less(v, 5, 6, is_less);
}

template <typename T, typename F>
inline void sort13_optimal(T * v, F & is_less)
{
    swap_if_less(v, 0, 12, is_less); swap_if_less(v, 1, 10, is_less); swap_if_less(v, 2, 9, is_less);
    swap_if_less(v, 3, 7, is_less); swap_if_less(v, 5, 11, is_less); swap_if_less(v, 6, 8, is_less);
    swap_if_less(v, 1, 6, is_less); swap_if_less(v, 2, 3, is_less); swap_if_less(v, 4, 11, is_less);
    swap_if_less(v, 7, 9, is_less); swap_if_less(v, 8, 10, is_less); swap_if_less(v, 0, 4, is_less);
    swap_if_less(v, 1, 2, is_less); swap_if_less(v, 3, 6, is_less); swap_if_less(v, 7, 8, is_less);
    swap_if_less(v, 9, 10, is_less); swap_if_less(v, 11, 12, is_less); swap_if_less(v, 4, 6, is_less);
    swap_if_less(v, 5, 9, is_less); swap_if_less(v, 8, 11, is_less); swap_if_less(v, 10, 12, is_less);
    swap_if_less(v, 0, 5, is_less); swap_if_less(v, 3, 8, is_less); swap_if_less(v, 4, 7, is_less);
    swap_if_less(v, 6, 11, is_less); swap_if_less(v, 9, 10, is_less); swap_if_less(v, 0, 1, is_less);
    swap_if_less(v, 2, 5, is_less); swap_if_less(v, 6, 9, is_less); swap_if_less(v, 7, 8, is_less);
    swap_if_less(v, 10, 11, is_less); swap_if_less(v, 1, 3, is_less); swap_if_less(v, 2, 4, is_less);
    swap_if_less(v, 5, 6, is_less); swap_if_less(v, 9, 10, is_less); swap_if_less(v, 1, 2, is_less);
    swap_if_less(v, 3, 4, is_less); swap_if_less(v, 5, 7, is_less); swap_if_less(v, 6, 8, is_less);
    swap_if_less(v, 2, 3, is_less); swap_if_less(v, 4, 5, is_less); swap_if_less(v, 6, 7, is_less);
    swap_if_less(v, 8, 9, is_less); swap_if_less(v, 3, 4, is_less); swap_if_less(v, 5, 6, is_less);
}

template <typename T>
inline const T * select_ptr(bool cond, const T * a, const T * b) { return cond ? a : b; }

/// Stable sort of 4 elements from v_base into dst[0..4]. v_base valid for 4 reads, dst for 4 writes.
template <typename T, typename F>
inline void sort4_stable(const T * v_base, T * dst, F & is_less)
{
    bool c1 = is_less(v_base[1], v_base[0]);
    bool c2 = is_less(v_base[3], v_base[2]);
    const T * a = v_base + static_cast<std::size_t>(c1);
    const T * b = v_base + static_cast<std::size_t>(!c1);
    const T * c = v_base + 2 + static_cast<std::size_t>(c2);
    const T * d = v_base + 2 + static_cast<std::size_t>(!c2);

    bool c3 = is_less(*c, *a);
    bool c4 = is_less(*d, *b);
    const T * mn = select_ptr(c3, c, a);
    const T * mx = select_ptr(c4, b, d);
    const T * ul = select_ptr(c3, a, select_ptr(c4, c, b));
    const T * ur = select_ptr(c4, d, select_ptr(c3, b, c));

    bool c5 = is_less(*ur, *ul);
    const T * lo = select_ptr(c5, ur, ul);
    const T * hi = select_ptr(c5, ul, ur);

    bit_copy(dst, mn);
    bit_copy(dst + 1, lo);
    bit_copy(dst + 2, hi);
    bit_copy(dst + 3, mx);
}

template <typename T, typename F>
inline void merge_up(const T *& left_src, const T *& right_src, T *& dst, F & is_less)
{
    bool is_l = !is_less(*right_src, *left_src);
    const T * src = is_l ? left_src : right_src;
    bit_copy(dst, src);
    right_src += static_cast<std::size_t>(!is_l);
    left_src += static_cast<std::size_t>(is_l);
    dst += 1;
}

template <typename T, typename F>
inline void merge_down(const T *& left_src, const T *& right_src, T *& dst, F & is_less)
{
    bool is_l = !is_less(*right_src, *left_src);
    const T * src = is_l ? right_src : left_src;
    bit_copy(dst, src);
    right_src -= static_cast<std::size_t>(is_l);
    left_src -= static_cast<std::size_t>(!is_l);
    dst -= 1;
}

/// Merge src[0..len/2] and src[len/2..len] into dst. dst valid for len writes, no alias, len>=2.
template <typename T, typename F>
inline void bidirectional_merge(const T * src, std::size_t len, T * dst, F & is_less)
{
    std::size_t len_div_2 = len / 2;
    const T * left = src;
    const T * right = src + len_div_2;
    T * d = dst;
    const T * left_rev = src + len_div_2 - 1;
    const T * right_rev = src + len - 1;
    T * dst_rev = dst + len - 1;

    for (std::size_t i = 0; i < len_div_2; ++i)
    {
        merge_up(left, right, d, is_less);
        merge_down(left_rev, right_rev, dst_rev, is_less);
    }

    const T * left_end = left_rev + 1;
    const T * right_end = right_rev + 1;
    if (len % 2 != 0)
    {
        bool left_nonempty = left < left_end;
        const T * last_src = left_nonempty ? left : right;
        bit_copy(d, last_src);
        left += static_cast<std::size_t>(left_nonempty);
        right += static_cast<std::size_t>(!left_nonempty);
    }
    // If (left != left_end || right != right_end) the comparator is not a strict weak ordering;
    // dst is a valid permutation regardless, so we don't abort (matching pdqsort tolerance).
    (void)left_end; (void)right_end;
}

template <typename T, typename F>
inline void sort8_stable(T * v_base, T * dst, T * scratch_base, F & is_less)
{
    sort4_stable(v_base, scratch_base, is_less);
    sort4_stable(v_base + 4, scratch_base + 4, is_less);
    bidirectional_merge(scratch_base, 8, dst, is_less);
}

/// Restores dst from src on scope exit unless dismissed (panic/exception safety).
template <typename T>
struct CopyOnDrop
{
    const T * src;
    T * dst;
    std::size_t len;
    bool armed = true;
    ~CopyOnDrop()
    {
        if (armed)
            std::memmove(static_cast<void *>(dst), static_cast<const void *>(src), len * sizeof(T));
    }
};

template <typename T, typename F>
inline void small_sort_general(T * v, std::size_t len, F & is_less)
{
    if (len < 2)
        return;

    alignas(T) std::byte storage[sizeof(T) * SMALL_SORT_GENERAL_SCRATCH_LEN];
    T * scratch_base = reinterpret_cast<T *>(storage);
    std::size_t len_div_2 = len / 2;

    std::size_t presorted_len;
    if (sizeof(T) <= 16 && len >= 16)
    {
        sort8_stable(v, scratch_base, scratch_base + len, is_less);
        sort8_stable(v + len_div_2, scratch_base + len_div_2, scratch_base + len + 8, is_less);
        presorted_len = 8;
    }
    else if (len >= 8)
    {
        sort4_stable(v, scratch_base, is_less);
        sort4_stable(v + len_div_2, scratch_base + len_div_2, is_less);
        presorted_len = 4;
    }
    else
    {
        bit_copy(scratch_base, v);
        bit_copy(scratch_base + len_div_2, v + len_div_2);
        presorted_len = 1;
    }

    for (std::size_t off : {std::size_t(0), len_div_2})
    {
        const T * src = v + off;
        T * dst = scratch_base + off;
        std::size_t desired_len = (off == 0) ? len_div_2 : (len - len_div_2);
        for (std::size_t i = presorted_len; i < desired_len; ++i)
        {
            bit_copy(dst + i, src + i);
            insert_tail(dst, dst + i, is_less);
        }
    }

    CopyOnDrop<T> guard{scratch_base, v, len};
    bidirectional_merge(scratch_base, len, v, is_less);
    guard.armed = false;
}

template <typename T, typename F>
inline void small_sort_network(T * v, std::size_t len, F & is_less)
{
    if (len < 2)
        return;

    alignas(T) std::byte storage[sizeof(T) * SMALL_SORT_NETWORK_THRESHOLD];
    T * scratch_base = reinterpret_cast<T *>(storage);

    std::size_t len_div_2 = len / 2;
    bool no_merge = len < 18;
    T * region = v;
    std::size_t region_len = no_merge ? len : len_div_2;

    while (true)
    {
        std::size_t presorted_len;
        if (region_len >= 13)
        {
            sort13_optimal(region, is_less);
            presorted_len = 13;
        }
        else if (region_len >= 9)
        {
            sort9_optimal(region, is_less);
            presorted_len = 9;
        }
        else
            presorted_len = 1;

        insertion_sort_shift_left(region, region_len, presorted_len, is_less);

        if (no_merge)
            return;
        if (region != v)
            break;
        region = v + len_div_2;
        region_len = len - len_div_2;
    }

    bidirectional_merge(v, len, scratch_base, is_less);
    std::memcpy(static_cast<void *>(v), static_cast<const void *>(scratch_base), len * sizeof(T));
}

// ============================ partition ===================================================

template <typename T>
struct GapGuard  // restores the saved hole value on exception
{
    T * pos;
    T * value;
    bool armed = true;
    ~GapGuard()
    {
        if (armed)
            bit_copy(pos, value);
    }
};

/// Branchless Lomuto cyclic partition (trivially-copyable, size <= 96). v[0..len], pivot separate.
template <typename T, typename F>
inline std::size_t partition_lomuto_branchless_cyclic(T * v_base, std::size_t len, const T * pivot, F & is_less)
{
    if (len == 0)
        return 0;

    Uninit<T> gap_value;
    bit_copy(&gap_value.value, v_base);

    std::size_t num_lt = 0;
    T * right = v_base + 1;
    GapGuard<T> gap{v_base, &gap_value.value};

    auto loop_body = [&]()
    {
        bool right_is_lt = is_less(*right, *pivot);
        T * left = v_base + num_lt;
        std::memmove(static_cast<void *>(gap.pos), static_cast<const void *>(left), sizeof(T));
        bit_copy(left, right);
        gap.pos = right;
        num_lt += static_cast<std::size_t>(right_is_lt);
        right += 1;
    };

#ifndef IPNSORT_UNROLL
#define IPNSORT_UNROLL 2
#endif
    constexpr std::size_t unroll_len = (sizeof(T) <= 16) ? IPNSORT_UNROLL : 1;
    T * unroll_end = v_base + (len - (unroll_len - 1));
    while (right < unroll_end)
    {
        for (std::size_t u = 0; u < unroll_len; ++u)
            loop_body();
    }

    T * end = v_base + len;
    while (true)
    {
        bool is_done = (right == end);
        if (is_done)
            right = gap.value;
        loop_body();
        if (is_done)
        {
            gap.armed = false;
            break;
        }
    }
    return num_lt;
}

/// Block Lomuto cyclic partition: the expensive comparator is evaluated over a block in an
/// independent (ILP-friendly) pre-pass; the serial cyclic rearrangement then consumes the
/// precomputed predicates with only cheap moves. This decouples the comparator's latency from
/// the loop-carried `num_lt` dependency, matching block-quicksort throughput for expensive
/// (e.g. indirect / cache-missing) comparators while keeping the branchless single-pass speed.
template <typename T, typename F>
inline std::size_t partition_lomuto_block(T * v_base, std::size_t len, const T * pivot, F & is_less)
{
    if (len == 0)
        return 0;

#ifndef IPN_BLOCK
#define IPN_BLOCK 64
#endif
    constexpr std::size_t B = IPN_BLOCK;
    Uninit<T> gap_value;
    bit_copy(&gap_value.value, v_base);

    std::size_t num_lt = 0;
    GapGuard<T> gap{v_base, &gap_value.value};

    // Apply one precomputed step of the cyclic permutation. `r` points at the current element.
    auto apply = [&](bool right_is_lt, T * r)
    {
        T * left = v_base + num_lt;
        std::memmove(static_cast<void *>(gap.pos), static_cast<const void *>(left), sizeof(T));
        bit_copy(left, r);
        gap.pos = r;
        num_lt += static_cast<std::size_t>(right_is_lt);
    };

    unsigned char pred[B];
    T * right = v_base + 1;
    T * end = v_base + len;

    while (right < end)
    {
        std::size_t blk = static_cast<std::size_t>(end - right);
        if (blk > B)
            blk = B;
        // Independent comparator evaluations (high ILP / MLP).
        for (std::size_t j = 0; j < blk; ++j)
            pred[j] = static_cast<unsigned char>(is_less(right[j], *pivot));
        // Serial rearrangement using precomputed predicates (cheap moves only).
        for (std::size_t j = 0; j < blk; ++j)
            apply(pred[j] != 0, right + j);
        right += blk;
    }

    // Final step: feed the saved gap value as the last element (matches the cyclic cleanup).
    bool gap_is_lt = is_less(gap_value.value, *pivot);
    apply(gap_is_lt, &gap_value.value);
    gap.armed = false;

    return num_lt;
}

/// Hoare branchy cyclic partition. Works for any movable T (move-based). v[0..len], pivot separate.
template <typename T, typename F>
inline std::size_t partition_hoare_branchy_cyclic(T * v_base, std::size_t len, const T * pivot, F & is_less)
{
    if (len == 0)
        return 0;

    T * left = v_base;
    T * right = v_base + len;

    bool have_gap = false;
    Uninit<T> gap_value;          // engaged (move-constructed) only while have_gap is true
    T * gap_pos = nullptr;

    /// If `is_less` throws while the gap is engaged, the in-transit element saved in `gap_value`
    /// must be moved back into the hole and destroyed on unwind. Otherwise the `Uninit` union's
    /// trivial destructor never runs `~T`, leaking the element and leaving the range one value
    /// short — a regression versus the `pdqsort` path this replaces for non-trivially-copyable
    /// `::sort`. This mirrors `GapGuard` used by the trivially-copyable block partition.
    struct GapRestorer
    {
        bool & have_gap;
        Uninit<T> & gap_value;
        T * & gap_pos;
        bool armed = true;
        ~GapRestorer()
        {
            if (armed && have_gap)
            {
                *gap_pos = std::move(gap_value.value);
                gap_value.value.~T();
            }
        }
    } gap_restorer{have_gap, gap_value, gap_pos};

    while (true)
    {
        while (left < right && is_less(*left, *pivot))
            ++left;
        do
        {
            --right;
            if (left >= right || is_less(*right, *pivot))
                break;
        } while (true);
        if (left >= right)
            break;

        if (!have_gap)
        {
            ::new (static_cast<void *>(&gap_value.value)) T(std::move(*left));
            gap_pos = right;
            have_gap = true;
        }
        else
        {
            *gap_pos = std::move(*left);
            gap_pos = right;
        }
        *left = std::move(*right);
        ++left;
    }

    if (have_gap)
    {
        gap_restorer.armed = false;
        *gap_pos = std::move(gap_value.value);
        gap_value.value.~T();
    }

    return static_cast<std::size_t>(left - v_base);
}

template <typename T, typename F>
inline std::size_t partition_impl(T * v_base, std::size_t len, const T * pivot, F & is_less)
{
    if constexpr (is_copy_v<T> && sizeof(T) <= MAX_BRANCHLESS_PARTITION_SIZE)
    {
#ifdef IPNSORT_SINGLEPASS_PARTITION
        return partition_lomuto_branchless_cyclic(v_base, len, pivot, is_less);
#else
        return partition_lomuto_block(v_base, len, pivot, is_less);
#endif
    }
    else
        return partition_hoare_branchy_cyclic(v_base, len, pivot, is_less);
}

/// Partitions v[0..len] around v[pivot_pos]; returns number of elements < pivot. Pivot ends there.
template <typename T, typename F>
inline std::size_t partition(T * v, std::size_t len, std::size_t pivot_pos, F & is_less)
{
    if (len == 0)
        return 0;
    std::swap(v[0], v[pivot_pos]);
    std::size_t num_lt = partition_impl(v + 1, len - 1, v, is_less);
    std::swap(v[0], v[num_lt]);
    return num_lt;
}

// ---- small_sort dispatch -----------------------------------------------------------------

template <typename T, typename F>
inline void small_sort(T * v, std::size_t len, F & is_less)
{
    constexpr SmallSortKind kind = choose_small_sort<T>();
    if constexpr (kind == SmallSortKind::Network)
        small_sort_network(v, len, is_less);
    else if constexpr (kind == SmallSortKind::General)
        small_sort_general(v, len, is_less);
    else
    {
        if (len >= 2)
            insertion_sort_shift_left(v, len, 1, is_less);
    }
}

// ---- quicksort driver --------------------------------------------------------------------

template <typename T, typename F>
inline void quicksort(T * v, std::size_t len, const T * ancestor_pivot, std::uint32_t limit, F & is_less)
{
    constexpr std::size_t threshold = small_sort_threshold<T>();
    while (true)
    {
        if (len <= threshold)
        {
            small_sort(v, len, is_less);
            return;
        }
        if (limit == 0)
        {
            heapsort(v, len, is_less);
            return;
        }
        --limit;

        std::size_t pivot_pos = choose_pivot(v, len, is_less);

        if (ancestor_pivot)
        {
            if (!is_less(*ancestor_pivot, v[pivot_pos]))
            {
                // Partition into (== pivot) and (> pivot): use !is_less(b,a) as the predicate.
                auto ge_pred = [&](const T & a, const T & b) { return !is_less(b, a); };
                std::size_t num_lt = partition(v, len, pivot_pos, ge_pred);
                v += num_lt + 1;
                len -= num_lt + 1;
                ancestor_pivot = nullptr;
                continue;
            }
        }

        std::size_t num_lt = partition(v, len, pivot_pos, is_less);

        T * left = v;
        std::size_t left_len = num_lt;
        const T * pivot = v + num_lt;
        T * right = v + num_lt + 1;
        std::size_t right_len = len - num_lt - 1;

        quicksort(left, left_len, ancestor_pivot, limit, is_less);

        v = right;
        len = right_len;
        ancestor_pivot = pivot;
    }
}

template <typename T, typename F>
inline void ipnsort_impl(T * v, std::size_t len, F & is_less)
{
    bool was_reversed = false;
    std::size_t run_len = find_existing_run(v, len, was_reversed, is_less);
    if (run_len == len)
    {
        if (was_reversed)
        {
            T * lo = v;
            T * hi = v + len - 1;
            while (lo < hi)
                std::swap(*lo++, *hi--);
        }
        return;
    }
    std::uint32_t limit = 2 * ilog2(len | 1);
    quicksort(v, len, static_cast<const T *>(nullptr), limit, is_less);
}

} // namespace detail

constexpr std::size_t MAX_LEN_ALWAYS_INSERTION_SORT = 20;

template <typename T, typename Less>
inline void sort(T * first, T * last, Less less)
{
    std::size_t len = static_cast<std::size_t>(last - first);
    if (len < 2)
        return;
    if (len <= MAX_LEN_ALWAYS_INSERTION_SORT)
    {
        detail::insertion_sort_shift_left(first, len, 1, less);
        return;
    }
    detail::ipnsort_impl(first, len, less);
}

template <typename T>
inline void sort(T * first, T * last)
{
    sort(first, last, [](const T & a, const T & b) { return a < b; });
}

} // namespace ipnsort
