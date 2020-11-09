/*          Copyright Danila Kutenin, 2020-.
 * Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *          https://boost.org/LICENSE_1_0.txt)
 */
#pragma once

#include <gtest/gtest.h>

#include <algorithm>

#include "miniselect/floyd_rivest_select.h"
#include "miniselect/median_of_3_random.h"
#include "miniselect/median_of_medians.h"
#include "miniselect/median_of_ninthers.h"
#include "miniselect/pdqselect.h"

namespace miniselect {
namespace algorithms {

struct STD {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    std::partial_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    std::partial_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    std::nth_element(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    std::nth_element(begin, mid, end);
  }
};

struct PDQ {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    pdqpartial_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    pdqpartial_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    pdqselect(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    pdqselect(begin, mid, end);
  }
};

struct PDQBranchless {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    pdqpartial_sort_branchless(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    pdqpartial_sort_branchless(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    pdqselect_branchless(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    pdqselect_branchless(begin, mid, end);
  }
};

struct FloydRivest {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    floyd_rivest_partial_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    floyd_rivest_partial_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    floyd_rivest_select(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    floyd_rivest_select(begin, mid, end);
  }
};

struct MedianOfNinthers {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_ninthers_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    median_of_ninthers_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_ninthers_select(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    median_of_ninthers_select(begin, mid, end);
  }
};

struct MedianOfMedians {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_medians_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    median_of_medians_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_medians_select(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    median_of_medians_select(begin, mid, end);
  }
};

struct MedianOf3Random {
  template <class Iter, class Compare>
  static void Sort(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_3_random_sort(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Sort(Iter begin, Iter mid, Iter end) {
    median_of_3_random_sort(begin, mid, end);
  }

  template <class Iter, class Compare>
  static void Select(Iter begin, Iter mid, Iter end, Compare&& comp) {
    median_of_3_random_select(begin, mid, end, std::move(comp));
  }

  template <class Iter>
  static void Select(Iter begin, Iter mid, Iter end) {
    median_of_3_random_select(begin, mid, end);
  }
};

using All =
    ::testing::Types<STD, PDQ, PDQBranchless, FloydRivest, MedianOfNinthers,
                     MedianOfMedians, MedianOf3Random>;

}  // namespace algorithms
}  // namespace miniselect
