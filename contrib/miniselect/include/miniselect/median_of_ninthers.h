/*          Copyright Andrei Alexandrescu, 2016-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
// Adjusted from Alexandrescu paper to support arbitrary comparators.
#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <iterator>
#include <utility>

#include "private/median_common.h"

namespace miniselect {
namespace median_of_ninthers_detail {

template <class Iter, class Compare>
void adaptiveQuickselect(Iter r, size_t n, size_t length, Compare&& comp);

/**
Median of minima
*/
template <class Iter, class Compare>
size_t medianOfMinima(Iter const r, const size_t n, const size_t length,
                      Compare&& comp) {
  assert(length >= 2);
  assert(n * 4 <= length);
  assert(n > 0);
  const size_t subset = n * 2, computeMinOver = (length - subset) / subset;
  assert(computeMinOver > 0);
  for (size_t i = 0, j = subset; i < subset; ++i) {
    const auto limit = j + computeMinOver;
    size_t minIndex = j;
    while (++j < limit)
      if (comp(r[j], r[minIndex])) minIndex = j;
    if (comp(r[minIndex], r[i])) std::swap(r[i], r[minIndex]);
    assert(j < length || i + 1 == subset);
  }
  adaptiveQuickselect(r, n, subset, comp);
  return median_common_detail::expandPartition(r, 0, n, subset, length, comp);
}

/**
Median of maxima
*/
template <class Iter, class Compare>
size_t medianOfMaxima(Iter const r, const size_t n, const size_t length,
                      Compare&& comp) {
  assert(length >= 2);
  assert(n * 4 >= length * 3 && n < length);
  const size_t subset = (length - n) * 2, subsetStart = length - subset,
               computeMaxOver = subsetStart / subset;
  assert(computeMaxOver > 0);
  for (size_t i = subsetStart, j = i - subset * computeMaxOver; i < length;
       ++i) {
    const auto limit = j + computeMaxOver;
    size_t maxIndex = j;
    while (++j < limit)
      if (comp(r[maxIndex], r[j])) maxIndex = j;
    if (comp(r[i], r[maxIndex])) std::swap(r[i], r[maxIndex]);
    assert(j != 0 || i + 1 == length);
  }
  adaptiveQuickselect(r + subsetStart, length - n, subset, comp);
  return median_common_detail::expandPartition(r, subsetStart, n, length,
                                               length, comp);
}

/**
Partitions r[0 .. length] using a pivot of its own choosing. Attempts to pick a
pivot that approximates the median. Returns the position of the pivot.
*/
template <class Iter, class Compare>
size_t medianOfNinthers(Iter const r, const size_t length, Compare&& comp) {
  assert(length >= 12);
  const auto frac = length <= 1024
                        ? length / 12
                        : length <= 128 * 1024 ? length / 64 : length / 1024;
  auto pivot = frac / 2;
  const auto lo = length / 2 - pivot, hi = lo + frac;
  assert(lo >= frac * 4);
  assert(length - hi >= frac * 4);
  assert(lo / 2 >= pivot);
  const auto gap = (length - 9 * frac) / 4;
  auto a = lo - 4 * frac - gap, b = hi + gap;
  for (size_t i = lo; i < hi; ++i, a += 3, b += 3) {
    median_common_detail::ninther(r, a, i - frac, b, a + 1, i, b + 1, a + 2,
                                  i + frac, b + 2, comp);
  }

  adaptiveQuickselect(r + lo, pivot, frac, comp);
  return median_common_detail::expandPartition(r, lo, lo + pivot, hi, length,
                                               comp);
}

/**
Quickselect driver for medianOfNinthers, medianOfMinima, and medianOfMaxima.
Dispathes to each depending on the relationship between n (the sought order
statistics) and length.
*/
template <class Iter, class Compare>
void adaptiveQuickselect(Iter r, size_t n, size_t length, Compare&& comp) {
  assert(n < length);
  for (;;) {
    // Decide strategy for partitioning
    if (n == 0) {
      // That would be the max
      auto pivot = n;
      for (++n; n < length; ++n)
        if (comp(r[n], r[pivot])) pivot = n;
      std::swap(r[0], r[pivot]);
      return;
    }
    if (n + 1 == length) {
      // That would be the min
      auto pivot = 0;
      for (n = 1; n < length; ++n)
        if (comp(r[pivot], r[n])) pivot = n;
      std::swap(r[pivot], r[length - 1]);
      return;
    }
    assert(n < length);
    size_t pivot;
    if (length <= 16)
      pivot = median_common_detail::pivotPartition(r, n, length, comp) - r;
    else if (n * 6 <= length)
      pivot = medianOfMinima(r, n, length, comp);
    else if (n * 6 >= length * 5)
      pivot = medianOfMaxima(r, n, length, comp);
    else
      pivot = medianOfNinthers(r, length, comp);

    // See how the pivot fares
    if (pivot == n) {
      return;
    }
    if (pivot > n) {
      length = pivot;
    } else {
      ++pivot;
      r += pivot;
      length -= pivot;
      n -= pivot;
    }
  }
}

}  // namespace median_of_ninthers_detail

template <class Iter, class Compare>
inline void median_of_ninthers_select(Iter begin, Iter mid, Iter end,
                                      Compare comp) {
  if (mid == end) return;
  using CompType = typename median_common_detail::CompareRefType<Compare>::type;

  median_of_ninthers_detail::adaptiveQuickselect<Iter, CompType>(
      begin, mid - begin, end - begin, comp);
}

template <class Iter>
inline void median_of_ninthers_select(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  median_of_ninthers_select(begin, mid, end, std::less<T>());
}

template <class Iter, class Compare>
inline void median_of_ninthers_sort(Iter begin, Iter mid, Iter end,
                                    Compare comp) {
  if (begin == mid) return;
  using CompType = typename median_common_detail::CompareRefType<Compare>::type;

  median_of_ninthers_detail::adaptiveQuickselect<Iter, CompType>(
      begin, mid - begin - 1, end - begin, comp);
  std::sort<Iter, CompType>(begin, mid, comp);
}

template <class Iter>
inline void median_of_ninthers_sort(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  median_of_ninthers_sort(begin, mid, end, std::less<T>());
}

}  // namespace miniselect
