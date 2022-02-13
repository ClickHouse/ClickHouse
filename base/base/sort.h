#pragma once

#include <pdqsort.h>

#ifndef NDEBUG

#include <pcg_random.hpp>
#include <base/getThreadId.h>

/** Same as libcxx std::__debug_less. Just without dependency on private part of standard library.
  * Check that Comparator induce strict weak ordering.
  */
template <typename Comparator>
class DebugLessComparator
{
public:
    constexpr DebugLessComparator(Comparator & cmp_)
        : cmp(cmp_)
    {}

    template <typename LhsType, typename RhsType>
    constexpr bool operator()(const LhsType & lhs, const RhsType & rhs)
    {
        bool lhs_less_than_rhs = cmp(lhs, rhs);
        if (lhs_less_than_rhs)
            assert(!cmp(rhs, lhs));

        return lhs_less_than_rhs;
    }

    template <typename LhsType, typename RhsType>
    constexpr bool operator()(LhsType & lhs, RhsType & rhs)
    {
        bool lhs_less_than_rhs = cmp(lhs, rhs);
        if (lhs_less_than_rhs)
            assert(!cmp(rhs, lhs));

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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <miniselect/floyd_rivest_select.h>

template <typename RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;

    comparator compare;
    ComparatorWrapper<comparator> compare_wrapper = compare;

#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ::miniselect::floyd_rivest_select(first, nth, last, compare_wrapper);

#ifndef NDEBUG
    ::shuffle(first, nth);

    if (nth != last)
        ::shuffle(nth + 1, last);
#endif
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

#pragma GCC diagnostic pop

template <typename RandomIt, typename Compare>
void sort(RandomIt first, RandomIt last, Compare compare)
{
#ifndef NDEBUG
    ::shuffle(first, last);
#endif

    ComparatorWrapper<Compare> compare_wrapper = compare;
    ::pdqsort(first, last, compare_wrapper);
}

template <typename RandomIt>
void sort(RandomIt first, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;
    ::sort(first, last, comparator());
}
