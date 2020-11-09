/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <functional>
#include <iterator>
#include <type_traits>
#include <utility>

namespace miniselect {
namespace floyd_rivest_detail {

template <class Compare>
struct CompareRefType {
  // Pass the comparator by lvalue reference. Or in debug mode, using a
  // debugging wrapper that stores a reference.
  using type = typename std::add_lvalue_reference<Compare>::type;
};

template <class Iter, class Compare, class Diff>
inline void floyd_rivest_select_loop(Iter begin, Diff left, Diff right, Diff k,
                                     Compare comp) {
  while (right > left) {
    Diff size = right - left;
    if (size > 600) {
      Diff n = right - left + 1;
      Diff i = k - left + 1;
      double z = log(n);
      double s = 0.5 * exp(2 * z / 3);
      double sd = 0.5 * sqrt(z * s * (n - s) / n);
      if (i < n / 2) {
        sd *= -1.0;
      }
      Diff newLeft = std::max(left, (Diff)(k - i * s / n + sd));
      Diff newRight = std::min(right, (Diff)(k + (n - i) * s / n + sd));
      floyd_rivest_select_loop<Iter, Compare, Diff>(begin, newLeft, newRight, k,
                                                    comp);
    }
    Diff i = left;
    Diff j = right;
    std::swap(begin[left], begin[k]);
    const bool to_swap = comp(begin[left], begin[right]);
    if (to_swap) {
      std::swap(begin[left], begin[right]);
    }
    // Make sure that non copyable types compile.
    const auto& t = to_swap ? begin[left] : begin[right];
    while (i < j) {
      std::swap(begin[i], begin[j]);
      i++;
      j--;
      while (comp(begin[i], t)) {
        i++;
      }
      while (comp(t, begin[j])) {
        j--;
      }
    }

    if (to_swap) {
      std::swap(begin[left], begin[j]);
    } else {
      j++;
      std::swap(begin[right], begin[j]);
    }

    if (j <= k) {
      left = j + 1;
    }
    if (k <= j) {
      right = j - 1;
    }
  }
}

}  // namespace floyd_rivest_detail

template <class Iter, class Compare>
inline void floyd_rivest_partial_sort(Iter begin, Iter mid, Iter end,
                                      Compare comp) {
  if (begin == end) return;
  if (begin == mid) return;
  using CompType = typename floyd_rivest_detail::CompareRefType<Compare>::type;

  floyd_rivest_detail::floyd_rivest_select_loop<
      Iter, CompType, typename std::iterator_traits<Iter>::difference_type>(
      begin, 0, end - begin - 1, mid - begin - 1, comp);
  // std::sort proved to be better than other sorts because of pivoting.
  std::sort<Iter, CompType>(begin, mid, comp);
}

template <class Iter>
inline void floyd_rivest_partial_sort(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  floyd_rivest_partial_sort(begin, mid, end, std::less<T>());
}

template <class Iter, class Compare>
inline void floyd_rivest_select(Iter begin, Iter mid, Iter end, Compare comp) {
  if (mid == end) return;
  using CompType = typename floyd_rivest_detail::CompareRefType<Compare>::type;

  floyd_rivest_detail::floyd_rivest_select_loop<
      Iter, CompType, typename std::iterator_traits<Iter>::difference_type>(
      begin, 0, end - begin - 1, mid - begin, comp);
}

template <class Iter>
inline void floyd_rivest_select(Iter begin, Iter mid, Iter end) {
  typedef typename std::iterator_traits<Iter>::value_type T;
  floyd_rivest_select(begin, mid, end, std::less<T>());
}

}  // namespace miniselect
