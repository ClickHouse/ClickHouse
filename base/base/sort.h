#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

#include <miniselect/floyd_rivest_select.h>

template <class RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
    ::miniselect::floyd_rivest_select(first, nth, last);
}

template <class RandomIt>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
{
    ::miniselect::floyd_rivest_partial_sort(first, middle, last);
}

template <class RandomIt, class Compare>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
{
    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare);
}

#pragma GCC diagnostic pop
