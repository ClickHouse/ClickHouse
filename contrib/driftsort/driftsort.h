// driftsort - C++ port of the Rust driftsort (stable adaptive sort) by Orson Peters &
// Lukas Bergdoll. Original: https://github.com/Voultapher/driftsort  (dual MIT / Apache-2.0).
//
// Stable, O(n log n) worst-case. Powersort merge-tree over detected runs, with a stable
// scratch-based quicksort for unsorted runs and a branchless bidirectional-merge small-sort.
// Uses up to ~n/2 extra memory (stack buffer for small inputs, heap otherwise).
//
// API: driftsort::sort(T* first, T* last, less)  where less(a,b)->bool.
//
// The full algorithm runs for trivially-copyable T (it relocates elements bitwise via memcpy,
// matching the Rust original). Non-trivially-copyable T falls back to std::stable_sort.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <type_traits>
#include <utility>

namespace driftsort
{
namespace detail
{

inline constexpr std::size_t SMALL_SORT_THRESHOLD = 32;
inline constexpr std::size_t MIN_SMALL_SORT_SCRATCH_LEN = SMALL_SORT_THRESHOLD + 17;
inline constexpr std::size_t MAX_FULL_ALLOC_BYTES = 8'000'000;

template <typename T>
inline void bit_copy(T * dst, const T * src) noexcept
{
    std::memcpy(static_cast<void *>(dst), static_cast<const void *>(src), sizeof(T));
}
template <typename T>
inline void bit_copy_n(T * dst, const T * src, std::size_t n) noexcept
{
    std::memmove(static_cast<void *>(dst), static_cast<const void *>(src), n * sizeof(T));
}

inline std::uint32_t ilog2(std::size_t x) noexcept
{
    std::uint32_t r = 0;
    while (x >>= 1) ++r;
    return r;
}

/// Uninitialized single-element storage; lets scratch slots work for trivially-copyable types
/// that are not default-constructible (e.g. a deleted default constructor). Access `.value`.
template <typename T>
union Uninit
{
    T value;
    Uninit() {}  // NOLINT(modernize-use-equals-default)
    ~Uninit() {}  // NOLINT(modernize-use-equals-default)
};

// ---- pivot (identical to ipnsort) --------------------------------------------------------
inline constexpr std::size_t PSEUDO_MEDIAN_REC_THRESHOLD = 64;

template <typename T, typename F>
inline const T * median3(const T * a, const T * b, const T * c, F & is_less)
{
    bool x = is_less(*a, *b), y = is_less(*a, *c);
    if (x == y) { bool z = is_less(*b, *c); return (z ^ x) ? c : b; }
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
inline std::size_t choose_pivot(const T * v, std::size_t len, F & is_less)
{
    std::size_t d8 = len / 8;
    const T * a = v, * b = v + d8 * 4, * c = v + d8 * 7;
    const T * r = (len < PSEUDO_MEDIAN_REC_THRESHOLD) ? median3(a, b, c, is_less)
                                                      : median3_rec(a, b, c, d8, is_less);
    return static_cast<std::size_t>(r - v);
}

// ---- run detection -----------------------------------------------------------------------
template <typename T, typename F>
inline std::size_t find_existing_run(const T * v, std::size_t len, bool & descending, F & is_less)
{
    descending = false;
    if (len < 2) return len;
    std::size_t run = 2;
    descending = is_less(v[1], v[0]);
    if (descending) while (run < len && is_less(v[run], v[run - 1])) ++run;
    else            while (run < len && !is_less(v[run], v[run - 1])) ++run;
    return run;
}

template <typename T>
inline void reverse_range(T * v, std::size_t len)
{
    T * lo = v; T * hi = v + len - 1;
    while (lo < hi) { Uninit<T> t; bit_copy(&t.value, lo); bit_copy(lo, hi); bit_copy(hi, &t.value); ++lo; --hi; }
}

// ---- small sort (stable; bidirectional merge) --------------------------------------------
template <typename T, typename F>
inline void insert_tail(T * begin, T * tail, F & is_less)
{
    if (!is_less(*tail, *(tail - 1))) return;
    Uninit<T> tmp; bit_copy(&tmp.value, tail);
    T * gap = tail; T * sift = tail - 1;
    while (true)
    {
        bit_copy(gap, sift); gap = sift;
        if (sift == begin) break;
        --sift;
        if (!is_less(tmp.value, *sift)) break;
    }
    bit_copy(gap, &tmp.value);
}
template <typename T, typename F>
inline void insertion_sort_shift_left(T * v, std::size_t len, std::size_t offset, F & is_less)
{
    if (offset == 0 || offset > len) return;
    for (T * tail = v + offset; tail != v + len; ++tail) insert_tail(v, tail, is_less);
}

template <typename T> inline const T * sel(bool c, const T * a, const T * b) { return c ? a : b; }

template <typename T, typename F>
inline void sort4_stable(const T * v, T * dst, F & is_less)
{
    bool c1 = is_less(v[1], v[0]);
    bool c2 = is_less(v[3], v[2]);
    const T * a = v + std::size_t(c1);
    const T * b = v + std::size_t(!c1);
    const T * c = v + 2 + std::size_t(c2);
    const T * d = v + 2 + std::size_t(!c2);
    bool c3 = is_less(*c, *a);
    bool c4 = is_less(*d, *b);
    const T * mn = sel(c3, c, a);
    const T * mx = sel(c4, b, d);
    const T * ul = sel(c3, a, sel(c4, c, b));
    const T * ur = sel(c4, d, sel(c3, b, c));
    bool c5 = is_less(*ur, *ul);
    const T * lo = sel(c5, ur, ul);
    const T * hi = sel(c5, ul, ur);
    bit_copy(dst, mn); bit_copy(dst + 1, lo); bit_copy(dst + 2, hi); bit_copy(dst + 3, mx);
}

template <typename T, typename F>
inline void merge_up(const T *& l, const T *& r, T *& d, F & is_less)
{
    bool is_l = !is_less(*r, *l);
    bit_copy(d, is_l ? l : r);
    r += std::size_t(!is_l); l += std::size_t(is_l); d += 1;
}
template <typename T, typename F>
inline void merge_down(const T *& l, const T *& r, T *& d, F & is_less)
{
    bool is_l = !is_less(*r, *l);
    bit_copy(d, is_l ? r : l);
    r -= std::size_t(is_l); l -= std::size_t(!is_l); d -= 1;
}
template <typename T, typename F>
inline void bidirectional_merge(const T * src, std::size_t len, T * dst, F & is_less)
{
    std::size_t h = len / 2;
    const T * left = src; const T * right = src + h; T * d = dst;
    const T * left_rev = src + h - 1; const T * right_rev = src + len - 1; T * dst_rev = dst + len - 1;
    for (std::size_t i = 0; i < h; ++i)
    {
        merge_up(left, right, d, is_less);
        merge_down(left_rev, right_rev, dst_rev, is_less);
    }
    const T * left_end = left_rev + 1;
    if (len % 2 != 0)
    {
        bool left_nonempty = left < left_end;
        bit_copy(d, left_nonempty ? left : right);
        left += std::size_t(left_nonempty); right += std::size_t(!left_nonempty);
    }
}

template <typename T> struct CopyOnDrop
{
    const T * src; T * dst; std::size_t len; bool armed = true;
    ~CopyOnDrop() { if (armed) std::memmove(static_cast<void *>(dst), static_cast<const void *>(src), len * sizeof(T)); }
};

template <typename T, typename F>
inline void sort_small_general(T * v, std::size_t len, T * scratch_base, F & is_less)
{
    if (len < 2) return;
    std::size_t h = len / 2;

    std::size_t presorted_len;
    if (sizeof(T) <= 16 && len >= 16)
    {
        sort4_stable(v, scratch_base, is_less);
        sort4_stable(v + 4, scratch_base + 4, is_less);
        bidirectional_merge(scratch_base, 8, scratch_base + len, is_less);     // sort8 of v[0..8] -> scratch[len..len+8]
        bit_copy_n(scratch_base, scratch_base + len, 8);
        sort4_stable(v + h, scratch_base + h, is_less);
        sort4_stable(v + h + 4, scratch_base + h + 4, is_less);
        bidirectional_merge(scratch_base + h, 8, scratch_base + len + 8, is_less);
        bit_copy_n(scratch_base + h, scratch_base + len + 8, 8);
        presorted_len = 8;
    }
    else if (len >= 8)
    {
        sort4_stable(v, scratch_base, is_less);
        sort4_stable(v + h, scratch_base + h, is_less);
        presorted_len = 4;
    }
    else
    {
        bit_copy(scratch_base, v);
        bit_copy(scratch_base + h, v + h);
        presorted_len = 1;
    }

    for (std::size_t off : {std::size_t(0), h})
    {
        const T * src = v + off;
        T * dst = scratch_base + off;
        std::size_t desired = (off == 0) ? h : (len - h);
        for (std::size_t i = presorted_len; i < desired; ++i)
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
inline void sort_small(T * v, std::size_t len, T * scratch_base, F & is_less)
{
    sort_small_general(v, len, scratch_base, is_less);
}

// ---- stable scratch partition ------------------------------------------------------------
// Writes elements < pivot to the left of scratch (in order) and >= pivot to the right in
// reverse, then copies them back; returns the number of elements < pivot.
template <typename T, typename F>
inline std::size_t stable_partition(T * v, std::size_t len, T * scratch_base,
                                     std::size_t pivot_pos, bool pivot_goes_left, F & is_less)
{
    const T * pivot = v + pivot_pos;
    std::size_t num_left = 0;
    const T * scan = v;
    T * scratch_rev = scratch_base + len;

    auto partition_one = [&](bool towards_left)
    {
        scratch_rev -= 1;
        T * dst_base = towards_left ? scratch_base : scratch_rev;
        T * dst = dst_base + num_left;
        bit_copy(dst, scan);
        num_left += std::size_t(towards_left);
        scan += 1;
    };

    std::size_t loop_end_pos = pivot_pos;
    while (true)
    {
        if constexpr (sizeof(T) <= 16)
        {
            const T * unroll_end = v + (loop_end_pos >= 3 ? loop_end_pos - 3 : 0);
            while (scan < unroll_end)
            {
                partition_one(is_less(*scan, *pivot));
                partition_one(is_less(*scan, *pivot));
                partition_one(is_less(*scan, *pivot));
                partition_one(is_less(*scan, *pivot));
            }
        }
        const T * loop_end = v + loop_end_pos;
        while (scan < loop_end)
            partition_one(is_less(*scan, *pivot));

        if (loop_end_pos == len) break;
        partition_one(pivot_goes_left);   // place the pivot without comparing it to itself
        loop_end_pos = len;
    }

    bit_copy_n(v, scratch_base, num_left);
    for (std::size_t i = 0; i < len - num_left; ++i)
        bit_copy(v + num_left + i, scratch_base + (len - 1 - i));
    return num_left;
}

// ---- physical merge (MergeState) ---------------------------------------------------------
template <typename T, typename F>
inline void merge(T * v, std::size_t len, T * buf, std::size_t mid, F & is_less)
{
    if (mid == 0 || mid >= len) return;
    std::size_t left_len = mid, right_len = len - mid;

    T * v_base = v, * v_mid = v + mid, * v_end = v + len;
    bool left_is_shorter = left_len <= right_len;
    T * save_base = left_is_shorter ? v_base : v_mid;
    std::size_t save_len = left_is_shorter ? left_len : right_len;
    bit_copy_n(buf, save_base, save_len);

    // MergeState: on scope exit copies [start,end) -> dst (finishing the shorter run / panic safety).
    struct MergeState
    {
        T * start; T * end; T * dst;
        ~MergeState() { std::memmove(static_cast<void *>(dst), static_cast<const void *>(start),
                                     static_cast<std::size_t>(end - start) * sizeof(T)); }
    } st{buf, buf + save_len, save_base};

    if (left_is_shorter)
    {
        T * right = v_mid; const T * right_end = v_end;
        while (st.start != st.end && static_cast<const T *>(right) != right_end)
        {
            bool consume_left = !is_less(*right, *st.start);
            bit_copy(st.dst, consume_left ? st.start : right);
            st.start += std::size_t(consume_left);
            right += std::size_t(!consume_left);
            st.dst += 1;
        }
    }
    else
    {
        const T * left_end = v_base; const T * right_end = buf; T * out = v_end;
        while (true)
        {
            T * left = st.dst - 1;
            T * right = st.end - 1;
            out -= 1;
            bool consume_left = is_less(*right, *left);
            bit_copy(out, consume_left ? left : right);
            st.dst = left + std::size_t(!consume_left);
            st.end = right + std::size_t(consume_left);
            if (static_cast<const T *>(st.dst) == left_end || static_cast<const T *>(st.end) == right_end)
                break;
        }
    }
}

// ---- driver --------------------------------------------------------------------------------
template <typename T, typename F>
void stable_quicksort(T * v, std::size_t len, T * scratch, std::size_t scratch_len,
                      std::uint32_t limit, const T * left_ancestor_pivot, F & is_less);

template <typename T, typename F>
void drift_sort(T * v, std::size_t len, T * scratch, std::size_t scratch_len, bool eager_sort, F & is_less);

inline std::uint64_t merge_tree_scale_factor(std::size_t n)
{
    return ((std::uint64_t(1) << 62) + std::uint64_t(n) - 1) / std::uint64_t(n);
}
inline std::uint8_t merge_tree_depth(std::size_t left, std::size_t mid, std::size_t right, std::uint64_t f)
{
    std::uint64_t x = std::uint64_t(left) + std::uint64_t(mid);
    std::uint64_t y = std::uint64_t(mid) + std::uint64_t(right);
    std::uint64_t z = (f * x) ^ (f * y);
    return z == 0 ? 64 : static_cast<std::uint8_t>(__builtin_clzll(z));
}
inline std::size_t sqrt_approx(std::size_t n)
{
    std::uint32_t il = ilog2(n | 1);
    std::uint32_t shift = (1 + il) / 2;
    return ((std::size_t(1) << shift) + (n >> shift)) / 2;
}

// Packs run length and sorted flag.
struct Run { std::size_t v; static Run sorted(std::size_t l){return {(l<<1)|1};} static Run unsorted(std::size_t l){return {l<<1};}
             bool is_sorted() const { return v & 1; } std::size_t len() const { return v >> 1; } };

template <typename T, typename F>
inline Run create_run(T * v, std::size_t len, T * scratch, std::size_t scratch_len,
                      std::size_t min_good_run_len, bool eager_sort, F & is_less)
{
    if (len >= min_good_run_len)
    {
        bool was_reversed = false;
        std::size_t run_len = find_existing_run(v, len, was_reversed, is_less);
        if (run_len >= min_good_run_len)
        {
            if (was_reversed) reverse_range(v, run_len);
            return Run::sorted(run_len);
        }
    }
    if (eager_sort)
    {
        std::size_t eager = std::min(SMALL_SORT_THRESHOLD, len);
        stable_quicksort(v, eager, scratch, scratch_len, 0, static_cast<const T *>(nullptr), is_less);
        return Run::sorted(eager);
    }
    return Run::unsorted(std::min(min_good_run_len, len));
}

template <typename T, typename F>
inline Run logical_merge(T * v, std::size_t len, T * scratch, std::size_t scratch_len,
                         Run left, Run right, F & is_less)
{
    bool can_fit = len <= scratch_len;
    if (!can_fit || left.is_sorted() || right.is_sorted())
    {
        if (!left.is_sorted())
            stable_quicksort(v, left.len(), scratch, scratch_len, 2 * ilog2(left.len() | 1), static_cast<const T *>(nullptr), is_less);
        if (!right.is_sorted())
            stable_quicksort(v + left.len(), len - left.len(), scratch, scratch_len, 2 * ilog2((len - left.len()) | 1), static_cast<const T *>(nullptr), is_less);
        merge(v, len, scratch, left.len(), is_less);
        return Run::sorted(len);
    }
    return Run::unsorted(len);
}

template <typename T, typename F>
void drift_sort(T * v, std::size_t len, T * scratch, std::size_t scratch_len, bool eager_sort, F & is_less)
{
    if (len < 2) return;
    std::uint64_t scale_factor = merge_tree_scale_factor(len);

    constexpr std::size_t MIN_SQRT_RUN_LEN = 64;
    std::size_t min_good_run_len = (len <= MIN_SQRT_RUN_LEN * MIN_SQRT_RUN_LEN)
        ? std::min(len - len / 2, MIN_SQRT_RUN_LEN) : sqrt_approx(len);

    std::size_t stack_len = 0;
    Run runs[66];
    std::uint8_t desired_depths[66];

    std::size_t scan_idx = 0;
    Run prev_run = Run::sorted(0);
    while (true)
    {
        Run next_run; std::uint8_t desired_depth;
        if (scan_idx < len)
        {
            next_run = create_run(v + scan_idx, len - scan_idx, scratch, scratch_len, min_good_run_len, eager_sort, is_less);
            desired_depth = merge_tree_depth(scan_idx - prev_run.len(), scan_idx, scan_idx + next_run.len(), scale_factor);
        }
        else { next_run = Run::sorted(0); desired_depth = 0; }

        while (stack_len > 1 && desired_depths[stack_len - 1] >= desired_depth)
        {
            Run left = runs[stack_len - 1];
            std::size_t merged_len = left.len() + prev_run.len();
            std::size_t merge_start = scan_idx - merged_len;
            prev_run = logical_merge(v + merge_start, merged_len, scratch, scratch_len, left, prev_run, is_less);
            stack_len -= 1;
        }
        runs[stack_len] = prev_run;
        desired_depths[stack_len] = desired_depth;
        stack_len += 1;

        if (scan_idx >= len) break;
        scan_idx += next_run.len();
        prev_run = next_run;
    }

    if (!prev_run.is_sorted())
        stable_quicksort(v, len, scratch, scratch_len, 2 * ilog2(len | 1), static_cast<const T *>(nullptr), is_less);
}

template <typename T, typename F>
void stable_quicksort(T * v, std::size_t len, T * scratch, std::size_t scratch_len,
                      std::uint32_t limit, const T * left_ancestor_pivot, F & is_less)
{
    while (true)
    {
        if (len <= SMALL_SORT_THRESHOLD)
        {
            sort_small(v, len, scratch, is_less);
            return;
        }
        if (limit == 0)
        {
            drift_sort(v, len, scratch, scratch_len, true, is_less);
            return;
        }
        --limit;

        std::size_t pivot_pos = choose_pivot(v, len, is_less);
        Uninit<T> pivot_copy; bit_copy(&pivot_copy.value, v + pivot_pos);

        bool perform_equal = false;
        if (left_ancestor_pivot)
            perform_equal = !is_less(*left_ancestor_pivot, v[pivot_pos]);

        std::size_t left_len = 0;
        if (!perform_equal)
        {
            left_len = stable_partition(v, len, scratch, pivot_pos, false, is_less);
            perform_equal = (left_len == 0);
        }
        if (perform_equal)
        {
            auto ge = [&](const T & a, const T & b) { return !is_less(b, a); };
            std::size_t mid_eq = stable_partition(v, len, scratch, pivot_pos, true, ge);
            v += mid_eq; len -= mid_eq; left_ancestor_pivot = nullptr;
            continue;
        }

        // Recurse right, loop left. The ancestor pivot for the right side is our pivot value.
        stable_quicksort(v + left_len, len - left_len, scratch, scratch_len, limit, &pivot_copy.value, is_less);
        len = left_len;
    }
}

} // namespace detail

constexpr std::size_t MAX_LEN_ALWAYS_INSERTION_SORT = 20;

template <typename T, typename Less>
inline void sort(T * first, T * last, Less less)
{
    std::size_t len = static_cast<std::size_t>(last - first);
    if (len < 2) return;

    if constexpr (!std::is_trivially_copyable_v<T>)
    {
        std::stable_sort(first, last, less);
        return;
    }
    else
    {
        if (len <= MAX_LEN_ALWAYS_INSERTION_SORT)
        {
            detail::insertion_sort_shift_left(first, len, 1, less);
            return;
        }

        std::size_t max_full_alloc = detail::MAX_FULL_ALLOC_BYTES / sizeof(T);
        std::size_t alloc_len = std::max(std::max(len - len / 2, std::min(len, max_full_alloc)),
                                         detail::MIN_SMALL_SORT_SCRATCH_LEN);

        // Small inputs: 4 KiB stack scratch; otherwise heap.
        alignas(T) std::byte stack_storage[4096];
        std::size_t stack_cap = 4096 / sizeof(T);
        T * scratch;

        // The scratch is reinterpreted as `T *` and accessed as `T &`, so the heap allocation must
        // satisfy `alignof(T)`. Plain `new std::byte[]` only guarantees the default new alignment,
        // which is insufficient for over-aligned `T` (e.g. `alignas(64)`), so use aligned
        // `operator new` / `operator delete` with a matching deleter.
        struct AlignedDelete
        {
            void operator()(std::byte * p) const noexcept
            {
                ::operator delete[](static_cast<void *>(p), std::align_val_t{alignof(T)});
            }
        };
        std::unique_ptr<std::byte[], AlignedDelete> heap;
        if (stack_cap >= alloc_len)
            scratch = reinterpret_cast<T *>(stack_storage);
        else
        {
            heap.reset(static_cast<std::byte *>(::operator new[](alloc_len * sizeof(T), std::align_val_t{alignof(T)})));
            scratch = reinterpret_cast<T *>(heap.get());
        }

        bool eager = len <= detail::SMALL_SORT_THRESHOLD * 2;
        detail::drift_sort(first, len, scratch, alloc_len, eager, less);
    }
}

template <typename T>
inline void sort(T * first, T * last)
{
    sort(first, last, [](const T & a, const T & b) { return a < b; });
}

} // namespace driftsort
