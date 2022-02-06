#pragma once

#include <pdqsort.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <miniselect/floyd_rivest_select.h>

template <typename RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
    ::miniselect::floyd_rivest_select(first, nth, last);
}

template <typename RandomIt>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
{
    ::miniselect::floyd_rivest_partial_sort(first, middle, last);
}

template <typename RandomIt, typename Compare>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
{
    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare);
}

#pragma GCC diagnostic pop

template <typename RandomIt, typename Compare>
void sort(RandomIt first, RandomIt last, Compare compare)
{
    ::pdqsort(first, last, compare);
}

template <typename RandomIt>
void sort(RandomIt first, RandomIt last)
{
    using value_type = typename std::iterator_traits<RandomIt>::value_type;
    using comparator = std::less<value_type>;
    ::pdqsort(first, last, comparator());
}
