#pragma once

#include <pdqsort.h>
#include <blqs.h>

#include <iterator>
#include <memory>

#ifndef NDEBUG

#include <pcg_random.hpp>
#include <base/defines.h>
#include <base/getThreadId.h>

/** Same as libcxx std::__debug_less. Just without dependency on private part of standard library.
  * Check that Comparator induce strict weak ordering.
  */
template <typename Comparator>
class DebugLessComparator
{
public:
    constexpr DebugLessComparator(Comparator & cmp_) // NOLINT(google-explicit-constructor)
        : cmp(cmp_)
    {}

    template <typename LhsType, typename RhsType>
    constexpr bool operator()(const LhsType & lhs, const RhsType & rhs)
    {
        bool lhs_less_than_rhs = cmp(lhs, rhs);
        if (lhs_less_than_rhs)
            chassert(!cmp(rhs, lhs));

        return lhs_less_than_rhs;
    }

    template <typename LhsType, typename RhsType>
    constexpr bool operator()(LhsType & lhs, RhsType & rhs)
    {
        bool lhs_less_than_rhs = cmp(lhs, rhs);
        if (lhs_less_than_rhs)
            chassert(!cmp(rhs, lhs));

        return lhs_less_than_rhs;
    }

private:
    Comparator & cmp;
};

template <typename Comparator>
using ComparatorWrapper = DebugLessComparator<Comparator>;

template <typename RandomIt>
void shuffle(RandomIt first, RandomIt last)
{
    static thread_local pcg64 rng(getThreadId());
    std::shuffle(first, last, rng);
}

#else

template <typename Comparator>
using ComparatorWrapper = Comparator;

#endif

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wimplicit-int-float-conversion"
#include <miniselect/floyd_rivest_select.h>

template <typename RandomIt, typename Compare>
void nth_element(RandomIt first, RandomIt nth, RandomIt last, Compare compare)
{
#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ComparatorWrapper<Compare> compare_wrapper = compare;
    ::miniselect::floyd_rivest_select(first, nth, last, compare_wrapper);

#ifndef NDEBUG
    ::shuffle(first, nth);

    if (nth != last)
        ::shuffle(nth + 1, last);
#endif
}

template <typename RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;

    ::nth_element(first, nth, last, comparator());
}

template <typename RandomIt, typename Compare>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
{
#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ComparatorWrapper<Compare> compare_wrapper = compare;
    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare_wrapper);

#ifndef NDEBUG
    ::shuffle(middle, last);
#endif
}

template <typename RandomIt>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;

    ::partial_sort(first, middle, last, comparator());
}

#pragma clang diagnostic pop

/** Branchless quicksort (blqsort) outperforms pdqsort by a wide margin on unpatterned,
  * high-cardinality data, which is the common case reaching this function. blqsort requires
  * raw pointers, so we convert the (always contiguous) iterators with `std::to_address`.
  *
  * Unlike pdqsort, blqsort has no pattern-defeating fast paths: it is much slower than pdqsort
  * on already-sorted, reverse-sorted and nearly-sorted inputs. pdqsort handled those patterns
  * internally on every `::sort` call, so to preserve that behavior we run pdqsort's pattern
  * pre-pass (`pdqsort_try_sort`, the same one behind `trySort`) before falling back to blqsort.
  * The pre-pass also sorts small ranges with insertion sort and returns. On unpatterned data it
  * gives up after a few partitioning iterations, which costs about 1% — negligible next to the
  * several-fold speedup blqsort gives on that data. Without this pre-pass, sorts that reach
  * `::sort` directly (e.g. `ColumnDecimal<Decimal128>::getPermutation`, which has no radix /
  * trySort step) regress by an order of magnitude on patterned inputs.
  */
template <typename RandomIt, typename Compare>
void sort(RandomIt first, RandomIt last, Compare compare)
{
#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ComparatorWrapper<Compare> compare_wrapper = compare;

    /// blqsort needs raw pointers. Contiguous iterators (raw pointers, PODArray and vector
    /// iterators) provide them via `std::to_address`; the rare non-contiguous random-access
    /// iterators (e.g. reverse iterators over contiguous storage) keep using pdqsort.
    if constexpr (std::contiguous_iterator<RandomIt>)
    {
        auto * first_ptr = std::to_address(first);
        auto * last_ptr = std::to_address(last);
        if (::pdqsort_try_sort(first_ptr, last_ptr, compare_wrapper))
            return;
        ::blqs::sort(first_ptr, last_ptr, compare_wrapper);
    }
    else
        ::pdqsort(first, last, compare_wrapper);
}

template <typename RandomIt>
void sort(RandomIt first, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;
    ::sort(first, last, comparator());
}

/** Try to fast sort elements for common sorting patterns:
  * 1. If elements are already sorted.
  * 2. If elements are already almost sorted.
  * 3. If elements are already sorted in reverse order.
  *
  * Returns true if fast sort was performed or elements were already sorted, false otherwise.
  */
template <typename RandomIt, typename Compare>
bool trySort(RandomIt first, RandomIt last, Compare compare)
{
#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ComparatorWrapper<Compare> compare_wrapper = compare;
    return ::pdqsort_try_sort(first, last, compare_wrapper);
}

template <typename RandomIt>
bool trySort(RandomIt first, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;
    return ::trySort(first, last, comparator());
}
