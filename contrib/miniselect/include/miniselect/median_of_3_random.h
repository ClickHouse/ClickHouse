/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <algorithm>
#include <cassert>
#include <iterator>
#include <random>
#include <utility>

#include "private/median_common.h"

namespace miniselect {
namespace median_of_3_random_detail {

template <class Iter, class Compare>
static inline Iter partition(Iter r, Iter end, Compare&& comp) {
  typedef typename std::iterator_traits<Iter>::difference_type T;
  const T len = end - r;
  assert(len >= 3);
  static std::mt19937_64 gen(1);
  std::uniform_int_distribution<T> dis(0, len - 1);
  T x = dis(gen);
  T y = dis(gen);
  T z = dis(gen);
  return median_common_detail::pivotPartition(
      r, median_common_detail::medianIndex(r, x, y, z, comp), len, comp);
}

}  // namespace median_of_3_random_detail

template <class Iter, class Compare>
inline void median_of_3_random_select(Iter begin, Iter mid, Iter end,
                                      Compare comp) {
  if (mid == end) return;
  using CompType = typename floyd_rivest_detail::CompareRefType<Compare>::type;

  median_common_detail::quickselect<
      Iter, CompType, &median_of_3_random_detail::partition<Iter, CompType>>(
      begin, mid, end, comp);
}

template <class Iter>
inline void median_of_3_random_select(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  median_of_3_random_select(begin, mid, end, std::less<T>());
}

template <class Iter, class Compare>
inline void median_of_3_random_sort(Iter begin, Iter mid, Iter end,
                                    Compare comp) {
  if (begin == mid) return;
  using CompType = typename floyd_rivest_detail::CompareRefType<Compare>::type;
  median_common_detail::quickselect<
      Iter, CompType, &median_of_3_random_detail::partition<Iter, CompType>>(
      begin, mid - 1, end, comp);
  std::sort<Iter, CompType>(begin, mid, comp);
}

template <class Iter>
inline void median_of_3_random_sort(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  median_of_3_random_sort(begin, mid, end, std::less<T>());
}

}  // namespace miniselect
