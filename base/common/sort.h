#pragma once

#if !defined(ARCADIA_BUILD)
#    include <miniselect/floyd_rivest_select.h>  // Y_IGNORE
#else
#    include <algorithm>
#endif

template <class RandomIt>
void nth_element(RandomIt first, RandomIt nth, RandomIt last)
{
#if !defined(ARCADIA_BUILD)
    ::miniselect::floyd_rivest_select(first, nth, last);
#else
    ::std::nth_element(first, nth, last);
#endif
}

template <class RandomIt>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last)
{
#if !defined(ARCADIA_BUILD)
    ::miniselect::floyd_rivest_partial_sort(first, middle, last);
#else
    ::std::partial_sort(first, middle, last);
#endif
}

template <class RandomIt, class Compare>
void partial_sort(RandomIt first, RandomIt middle, RandomIt last, Compare compare)
{
#if !defined(ARCADIA_BUILD)
    ::miniselect::floyd_rivest_partial_sort(first, middle, last, compare);
#else
    ::std::partial_sort(first, middle, last, compare);
#endif
}
