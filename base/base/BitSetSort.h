/** https://github.com/minjaehwang/bitsetsort
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  * Bitset Sort is a variant of quick sort, specifically BlockQuickSort.
  * Bitset Sort uses a carefully written partition function to let the compiler generates
  * SIMD instructions without actually writing SIMD intrinsics in the loop.
  * Bitset Sort is 3.4x faster (or spends 71% less time) than libc++ std::sort when sorting uint64s and 1.58x faster (or spends 37% less time)
  * when sorting std::string.
  * Bitset Sort uses multiple techniques to improve runtime performance of sort. This includes sorting networks,
  * a variant of merge sort called Bitonic Order Merge Sort that is faster for small N, and pattern recognitions.
  */

#pragma clang diagnostic ignored "-Wreserved-identifier"
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"
#pragma clang diagnostic ignored "-Wunused-local-typedef"

#ifndef _LIBCPP___BITSETSORT
#define _LIBCPP___BITSETSORT

#include <algorithm>
#include <cstdint>
#include <iterator>

namespace stdext {  //_LIBCPP_BEGIN_NAMESPACE_STD

namespace __sorting_network {

template <class _RandomAccessIterator, class _Compare>
class __conditional_swap {
  _Compare comp_;

 public:
  _Compare get() const { return comp_; }
  __conditional_swap(_Compare __comp) : comp_(__comp) {}
  inline void operator()(_RandomAccessIterator __x,
                         _RandomAccessIterator __y) const {
    typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type
        value_type;
    bool __result = comp_(*__x, *__y);
    // Expect a compiler would short-circuit the following if-block.
    // 4 * sizeof(size_t) is a magic number. Expect a compiler to use SIMD
    // instruction on them.
    if (_VSTD::is_trivially_copy_constructible<value_type>::value &&
        _VSTD::is_trivially_copy_assignable<value_type>::value &&
        sizeof(value_type) <= 4 * sizeof(size_t)) {
      value_type __min = __result ? _VSTD::move(*__x) : _VSTD::move(*__y);
      *__y = __result ? _VSTD::move(*__y) : _VSTD::move(*__x);
      *__x = _VSTD::move(__min);
    } else {
      if (!__result) {
        _VSTD::iter_swap(__x, __y);
      }
    }
  }
};

template <class _RandomAccessIterator, class _Compare>
class __reverse_conditional_swap {
  _Compare comp_;

 public:
  _Compare get() const { return comp_; }
  __reverse_conditional_swap(_Compare __comp) : comp_(__comp) {}
  inline void operator()(_RandomAccessIterator __x,
                         _RandomAccessIterator __y) const {
    typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type
        value_type;
    bool __result = !comp_(*__x, *__y);
    // Expect a compiler would short-circuit the following if-block.
    if (_VSTD::is_trivially_copy_constructible<value_type>::value &&
        _VSTD::is_trivially_copy_assignable<value_type>::value &&
        sizeof(value_type) <= 4 * sizeof(size_t)) {
      value_type __min = __result ? _VSTD::move(*__x) : _VSTD::move(*__y);
      *__y = __result ? _VSTD::move(*__y) : _VSTD::move(*__x);
      *__x = _VSTD::move(__min);
    } else {
      /** This change is required for ClickHouse.
        * It seems that this is slow branch, and its logic should be identical to fast branch.
        * Logic of fast branch,
        * if (result)
        *   min = x;
        *   y = y;
        *   x = x;
        * else
        *   min = y;
        *   y = x;
        *   x = y;
        *
        * We swap elements only if result is false.
        *
        * Example to reproduce sort bug:
        * int main(int argc, char ** argv)
        * {
        *    (void)(argc);
        *    (void)(argv);
        *
        *    std::vector<std::pair<Int64, Int64>> values = {
        *        {1, 1},
        *        {3, -1},
        *        {2, 1},
        *        {7, -1},
        *        {3, 1},
        *        {999, -1},
        *        {4, 1},
        *        {7, -1},
        *        {5, 1},
        *        {8, -1}
        *    };
        *
        *    ::stdext::bitsetsort(values.begin(), values.end());
        *    bool is_sorted = std::is_sorted(values.begin(), values.end());
        *
        *    std::cout << "Array " << values.size() << " is sorted " << is_sorted << std::endl;
        *
        *    for (auto & value : values)
        *        std::cout << value.first << " " << value.second << std::endl;
        *
        *    return 0;
        * }
        *
        * Output before change:
        * Array 10 is sorted 0
        * 1 1
        * 2 1
        * 3 -1
        * 3 1
        * 4 1
        * 7 -1
        * 7 -1
        * 8 -1
        * 5 1
        * 999 -1
        *
        * After change:
        * Array 10 is sorted 1
        * 1 1
        * 2 1
        * 3 -1
        * 3 1
        * 4 1
        * 5 1
        * 7 -1
        * 7 -1
        * 8 -1
        * 999 -1
        */
      if (!__result) {
        _VSTD::iter_swap(__x, __y);
      }
    }
  }
};

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort2(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 0, __a + 1);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort3(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 1, __a + 2);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 0, __a + 1);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort4(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 0, __a + 1);
  __cond_swap(__a + 2, __a + 3);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 1, __a + 3);
  __cond_swap(__a + 1, __a + 2);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort5(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 0, __a + 1);
  __cond_swap(__a + 3, __a + 4);
  __cond_swap(__a + 2, __a + 4);
  __cond_swap(__a + 2, __a + 3);
  __cond_swap(__a + 0, __a + 3);
  __cond_swap(__a + 1, __a + 4);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 1, __a + 3);
  __cond_swap(__a + 1, __a + 2);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort6(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 1, __a + 2);
  __cond_swap(__a + 4, __a + 5);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 3, __a + 5);
  __cond_swap(__a + 0, __a + 1);
  __cond_swap(__a + 3, __a + 4);
  __cond_swap(__a + 0, __a + 3);
  __cond_swap(__a + 1, __a + 4);
  __cond_swap(__a + 2, __a + 5);
  __cond_swap(__a + 2, __a + 4);
  __cond_swap(__a + 1, __a + 3);
  __cond_swap(__a + 2, __a + 3);
}
template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort7(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 1, __a + 2);
  __cond_swap(__a + 3, __a + 4);
  __cond_swap(__a + 5, __a + 6);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 3, __a + 5);
  __cond_swap(__a + 4, __a + 6);
  __cond_swap(__a + 0, __a + 1);
  __cond_swap(__a + 4, __a + 5);
  __cond_swap(__a + 0, __a + 4);
  __cond_swap(__a + 1, __a + 5);
  __cond_swap(__a + 2, __a + 6);
  __cond_swap(__a + 0, __a + 3);
  __cond_swap(__a + 2, __a + 5);
  __cond_swap(__a + 1, __a + 3);
  __cond_swap(__a + 2, __a + 4);
  __cond_swap(__a + 2, __a + 3);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort8(_RandomAccessIterator __a, _ConditionalSwap __cond_swap) {
  __cond_swap(__a + 0, __a + 1);
  __cond_swap(__a + 2, __a + 3);
  __cond_swap(__a + 4, __a + 5);
  __cond_swap(__a + 6, __a + 7);
  __cond_swap(__a + 0, __a + 2);
  __cond_swap(__a + 1, __a + 3);
  __cond_swap(__a + 4, __a + 6);
  __cond_swap(__a + 5, __a + 7);
  __cond_swap(__a + 1, __a + 2);
  __cond_swap(__a + 5, __a + 6);
  __cond_swap(__a + 0, __a + 4);
  __cond_swap(__a + 1, __a + 5);
  __cond_swap(__a + 2, __a + 6);
  __cond_swap(__a + 3, __a + 7);
  __cond_swap(__a + 1, __a + 4);
  __cond_swap(__a + 3, __a + 6);
  __cond_swap(__a + 2, __a + 4);
  __cond_swap(__a + 3, __a + 5);
  __cond_swap(__a + 3, __a + 4);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort1to8(
    _RandomAccessIterator __a,
    typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type __len,
    _ConditionalSwap __cond_swap) {
  switch (__len) {
    case 0:
    case 1:
      return;
    case 2:
      __sort2(__a, __cond_swap);
      return;
    case 3:
      __sort3(__a, __cond_swap);
      return;
    case 4:
      __sort4(__a, __cond_swap);
      return;
    case 5:
      __sort5(__a, __cond_swap);
      return;
    case 6:
      __sort6(__a, __cond_swap);
      return;
    case 7:
      __sort7(__a, __cond_swap);
      return;
    case 8:
      __sort8(__a, __cond_swap);
      return;
  }
  // ignore
}
template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort3(_RandomAccessIterator __a0, _RandomAccessIterator __a1, _RandomAccessIterator __a2, _ConditionalSwap __cond_swap) {
  __cond_swap(__a1, __a2);
  __cond_swap(__a0, __a2);
  __cond_swap(__a0, __a1);
}

template <class _RandomAccessIterator, class _ConditionalSwap>
void __sort3r(_RandomAccessIterator __a2, _RandomAccessIterator __a1, _RandomAccessIterator __a0, _ConditionalSwap __rev_cond_swap) {
  __rev_cond_swap(__a1, __a2);
  __rev_cond_swap(__a0, __a2);
  __rev_cond_swap(__a0, __a1);
}

}  // namespace __sorting_network

template <class _Compare, class _ForwardIterator>
_ForwardIterator
__median3(_ForwardIterator __x, _ForwardIterator __y, _ForwardIterator __z, _Compare __c)
{
    if (__c(*__x, *__y)) {
      if (__c(*__y, *__z)) {
        return __y;
      }
      // x < y, y >= z
      if (__c(*__x, *__z)) {
        return __z;
      }
      return __x;
    } else {
      // y <= x
      if (__c(*__x, *__z)) {
        // y <= x < z
        return __x;
      }
      // y <= x, z <= x
      if (__c(*__y, *__z)) {
        return __z;
      }
      return __y;
    }
}

namespace __bitonic {
class __detail {
 public:
  _LIBCPP_CONSTEXPR_AFTER_CXX11 static int __batch = 8;
  _LIBCPP_CONSTEXPR_AFTER_CXX11 static int __bitonic_batch = __batch * 2;
  _LIBCPP_CONSTEXPR_AFTER_CXX11 static int __small_sort_max =
      __detail::__bitonic_batch * 2;
};

template <class _RandomAccessIterator, class _ConditionalSwap,
          class _ReverseConditionalSwap>
void __enforce_order(_RandomAccessIterator __first,
                     _RandomAccessIterator __last, _ConditionalSwap __cond_swap,
                     _ReverseConditionalSwap __reverse_cond_swap) {
  _RandomAccessIterator __i = __first;
  while (__i + __detail::__bitonic_batch <= __last) {
    __sorting_network::__sort8(__i, __cond_swap);
    __sorting_network::__sort8(__i + __detail::__batch, __reverse_cond_swap);
    __i += __detail::__bitonic_batch;
  }
  if (__i + __detail::__batch <= __last) {
    __sorting_network::__sort8(__i, __cond_swap);
    __i += __detail::__batch;
    __sorting_network::__sort1to8(__i, __last - __i, __reverse_cond_swap);
  } else {
    __sorting_network::__sort1to8(__i, __last - __i, __cond_swap);
  }
}

class __construct {
 public:
  template <class _T>
  static inline void __op(_T* __result, _T&& __val) {
    new (__result) _T(_VSTD::move(__val));
  }
};

class __move_assign {
 public:
  template <class _T>
  static inline void __op(_T* __result, _T&& __val) {
    *__result = _VSTD::move(__val);
  }
};

template <class _Copy, class _InputIterator, class _OutputIterator,
          class _Compare>
void __forward_merge(_InputIterator __first, _InputIterator __last,
                     _OutputIterator __result, _Compare __comp) {
  --__last;
  typename _VSTD::iterator_traits<_InputIterator>::difference_type __len =
      __last - __first;
  for (; __len > 0; __len--) {
    if (__comp(*__first, *__last)) {
      _Copy::__op(&*__result, _VSTD::move(*__first++));
    } else {
      _Copy::__op(&*__result, _VSTD::move(*__last--));
    }
    __result++;
  }
  _Copy::__op(&*__result, _VSTD::move(*__first));
}

template <class _Copy, class _InputIterator, class _OutputIterator,
          class _Compare>
void __backward_merge(_InputIterator __first, _InputIterator __last,
                      _OutputIterator __result, _Compare __comp) {
  --__last;
  __result += __last - __first;
  typename _VSTD::iterator_traits<_InputIterator>::difference_type __len =
      __last - __first;
  for (; __len > 0; __len--) {
    if (__comp(*__first, *__last)) {
      _Copy::__op(&*__result, _VSTD::move(*__first++));
    } else {
      _Copy::__op(&*__result, _VSTD::move(*__last--));
    }
    __result--;
  }
  _Copy::__op(&*__result, _VSTD::move(*__first));
}

template <class _Copy, class _InputIterator, class _OutputIterator,
          class _Compare>
void __forward_and_backward_merge(_InputIterator __first, _InputIterator __last,
                                  _InputIterator __rlast,
                                  _OutputIterator __result, _Compare __comp) {
  _InputIterator __rfirst = __last;
  __last--;
  __rlast--;
  typename _VSTD::iterator_traits<_InputIterator>::difference_type len =
      __last - __first;
  _OutputIterator __rout = __result + (__rlast - __first);

  for (; len > 0; len--) {
    if (__comp(*__first, *__last)) {
      _Copy::__op(&*__result, _VSTD::move(*__first++));
    } else {
      _Copy::__op(&*__result, _VSTD::move(*__last--));
    }
    __result++;
    if (__comp(*__rfirst, *__rlast)) {
      _Copy::__op(&*__rout, _VSTD::move(*__rfirst++));
    } else {
      _Copy::__op(&*__rout, _VSTD::move(*__rlast--));
    }
    __rout--;
  }
  _Copy::__op(&*__result, _VSTD::move(*__first));
  _Copy::__op(&*__rout, _VSTD::move(*__rfirst));
}

template <class _RandomAccessIterator, class _ConditionalSwap,
          class _ReverseConditionalSwap>
inline bool __small_sort(
    _RandomAccessIterator __first,
    typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type __len,
    typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type* __buff,
    _ConditionalSwap& __cond_swap,
    _ReverseConditionalSwap __reverse_cond_swap) {
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type
      difference_type;
  typedef
      typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type value_type;
  if (__len > __detail::__small_sort_max) {
    return false;
  }
  _RandomAccessIterator __last = __first + __len;
  __enforce_order(__first, __last, __cond_swap, __reverse_cond_swap);
  if (__len <= __detail::__batch) {
    // sorted.
    return true;
  }
  auto __comp = __cond_swap.get();
  if (__len <= __detail::__bitonic_batch) {
    // single bitonic order merge.
    __forward_merge<__bitonic::__construct>(__first, __last, __buff, __comp);
    copy(_VSTD::make_move_iterator(__buff), _VSTD::make_move_iterator(__buff + __len),
         __first);
    for (auto __iter = __buff; __iter < __buff + __len; __iter++) {
      (*__iter).~value_type();
    }
    return true;
  }
  // double bitonic order merge.
  __forward_merge<__construct>(__first, __first + __detail::__bitonic_batch,
                               __buff, __comp);
  __backward_merge<__construct>(__first + __detail::__bitonic_batch, __last,
                                __buff + __detail::__bitonic_batch, __comp);
  __forward_merge<__move_assign>(__buff, __buff + __len, __first, __comp);
  for (auto __iter = __buff; __iter < __buff + __len; __iter++) {
    (*__iter).~value_type();
  }
  return true;
}
}  // namespace __bitonic

namespace __bitsetsort {
struct __64bit_set {
  typedef uint64_t __storage_t;
  _LIBCPP_CONSTEXPR_AFTER_CXX11 static int __block_size = 64;
  static __storage_t __blsr(__storage_t x) {
    // _blsr_u64 can be used here but it did not make any performance
    // difference in practice.
    return x ^ (x & -x);
  }
  static int __clz(__storage_t x) { return __builtin_clzll(x); }
  static int __ctz(__storage_t x) { return __builtin_ctzll(x); }
};

struct __32bit_set {
  typedef uint32_t __storage_t;
  _LIBCPP_CONSTEXPR_AFTER_CXX11 static int __block_size = 32;
  static __storage_t __blsr(__storage_t x) {
    // _blsr_u32 can be used here but it did not make any performance
    // difference in practice.
    return x ^ (x & -x);
  }
  static int __clz(__storage_t x) { return __builtin_clzl(x); }
  static int __ctz(__storage_t x) { return __builtin_ctzl(x); }
};

template <int N>
struct __set_selector {
    typedef __64bit_set __set;
};

template<>
struct __set_selector<4> {
    typedef __32bit_set __set;
};

template <class _Bitset, class _RandomAccessIterator>
inline void __swap_bitmap_pos(_RandomAccessIterator __first,
                              _RandomAccessIterator __last,
                              typename _Bitset::__storage_t& __left_bitset,
                              typename _Bitset::__storage_t& __right_bitset) {
  while (__left_bitset != 0 & __right_bitset != 0) {
    int tz_left = _Bitset::__ctz(__left_bitset);
    __left_bitset = _Bitset::__blsr(__left_bitset);
    int tz_right = _Bitset::__ctz(__right_bitset);
    __right_bitset = _Bitset::__blsr(__right_bitset);
    _VSTD::iter_swap(__first + tz_left, __last - tz_right);
  }
}

template <class _Bitset, class _RandomAccessIterator>
inline void __swap_bitmap(_RandomAccessIterator __first,
                          _RandomAccessIterator __last,
                          typename _Bitset::__storage_t& __left_bitset,
                          typename _Bitset::__storage_t& __right_bitset) {
  if (__left_bitset == 0 || __right_bitset == 0) {
    return;
  }
  int tz_left;
  int tz_right;

  tz_left = _Bitset::__ctz(__left_bitset);
  __left_bitset = _Bitset::__blsr(__left_bitset);

  tz_right = _Bitset::__ctz(__right_bitset);
  __right_bitset = _Bitset::__blsr(__right_bitset);

  _RandomAccessIterator l = __first + tz_left;
  _RandomAccessIterator r = __last - tz_right;
  typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type tmp(
      _VSTD::move(*l));
  *l = _VSTD::move(*r);
  while (__left_bitset != 0 & __right_bitset != 0) {
    tz_left = _Bitset::__ctz(__left_bitset);
    __left_bitset = _Bitset::__blsr(__left_bitset);
    tz_right = _Bitset::__ctz(__right_bitset);
    __right_bitset = _Bitset::__blsr(__right_bitset);

    l = __first + tz_left;
    *r = _VSTD::move(*l);
    r = __last - tz_right;
    *l = _VSTD::move(*r);
  }
  *r = _VSTD::move(tmp);
}

template <class _Bitset, class _RandomAccessIterator, class _Compare>
_VSTD::pair<_RandomAccessIterator, bool> __bitset_partition(
    _RandomAccessIterator __first, _RandomAccessIterator __last,
    _Compare __comp) {
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type
      value_type;
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type
      difference_type;
  typedef typename _Bitset::__storage_t __storage_t;
  _RandomAccessIterator __begin = __first;
  value_type __pivot = _VSTD::move(*__first);

  if (__comp(__pivot, *(__last - 1))) {
    // Guarded.
    while (!__comp(__pivot, *++__first)) {}
  } else {
    while (++__first < __last && !__comp(__pivot, *__first)) {}
  }

  if (__first < __last) {
    // It will be always guarded because __bitset_sort will do the median-of-three before calling this.
    while (__comp(__pivot, *--__last)) {}
  }
  bool __already_partitioned = __first >= __last;
  if (!__already_partitioned) {
      _VSTD::iter_swap(__first, __last);
      ++__first;
  }

  // [__first, __last) - __last is not inclusive. From now one, it uses last minus one to be inclusive on both sides.
  _RandomAccessIterator __lm1 = __last - 1;
  __storage_t __left_bitset = 0;
  __storage_t __right_bitset = 0;

  // Reminder: length = __lm1 - __first + 1.
  while (__lm1 - __first >= 2 * _Bitset::__block_size - 1) {
    if (__left_bitset == 0) {
      // Possible vectorization. With a proper "-march" flag, the following loop
      // will be compiled into a set of SIMD instructions.
      _RandomAccessIterator __iter = __first;
      for (int __j = 0; __j < _Bitset::__block_size;) {
        __left_bitset |= (static_cast<__storage_t>(__comp(__pivot, *__iter)) << __j);
        __j++;
        __iter++;
      }
    }

    if (__right_bitset == 0) {
      // Possible vectorization. With a proper "-march" flag, the following loop
      // will be compiled into a set of SIMD instructions.
      _RandomAccessIterator __iter = __lm1;
      for (int __j = 0; __j < _Bitset::__block_size;) {
        __right_bitset |=
            (static_cast<__storage_t>(!__comp(__pivot, *__iter)) << __j);
        __j++;
        __iter--;
      }
    }

    __swap_bitmap_pos<_Bitset>(__first, __lm1, __left_bitset, __right_bitset);
    __first += (__left_bitset == 0) ? _Bitset::__block_size : 0;
    __lm1 -= (__right_bitset == 0) ? _Bitset::__block_size : 0;
  }
  // Now, we have a less-than a block on each side.
  difference_type __remaining_len = __lm1 - __first + 1;
  difference_type __l_size;
  difference_type __r_size;
  if (__left_bitset == 0 && __right_bitset == 0) {
    __l_size = __remaining_len / 2;
    __r_size = __remaining_len - __l_size;
  } else if (__left_bitset == 0) {
    // We know at least one side is a full block.
    __l_size = __remaining_len - _Bitset::__block_size;
    __r_size = _Bitset::__block_size;
  } else {  // if (right == 0)
    __l_size = _Bitset::__block_size;
    __r_size = __remaining_len - _Bitset::__block_size;
  }
  if (__left_bitset == 0) {
    _RandomAccessIterator __iter = __first;
    for (int j = 0; j < __l_size; j++) {
      __left_bitset |=
          (static_cast<__storage_t>(__comp(__pivot, *(__iter))) << j);
      __iter++;
    }
  }
  if (__right_bitset == 0) {
    _RandomAccessIterator __iter = __lm1;
    for (int j = 0; j < __r_size; j++) {
      __right_bitset |=
          (static_cast<__storage_t>(!__comp(__pivot, *(__iter))) << j);
      --__iter;
    }
  }
  __swap_bitmap_pos<_Bitset>(__first, __lm1, __left_bitset, __right_bitset);
  __first += (__left_bitset == 0) ? __l_size : 0;
  __lm1 -= (__right_bitset == 0) ? __r_size : 0;

  if (__left_bitset) {
    // Swap within the right side.
    int __tz_left;

    // Need to find set positions in the reverse order.
    while (__left_bitset != 0) {
      __tz_left = _Bitset::__block_size - 1 - _Bitset::__clz(__left_bitset);
      __left_bitset &= (static_cast<__storage_t>(1) << __tz_left) - 1;
      _VSTD::iter_swap(__first + __tz_left, __lm1--);
    }
    __first = __lm1 + 1;
  } else if (__right_bitset) {
    // Swap within the left side.
    int __tz_right;
    // Need to find set positions in the reverse order.
    while (__right_bitset != 0) {
      __tz_right = _Bitset::__block_size - 1 - _Bitset::__clz(__right_bitset);
      __right_bitset &= (static_cast<__storage_t>(1) << __tz_right) - 1;
      _VSTD::iter_swap(__lm1 - __tz_right, __first++);
    }
  }

  _RandomAccessIterator __pivot_pos = __first - 1;
  *__begin = _VSTD::move(*__pivot_pos);
  *__pivot_pos = _VSTD::move(__pivot);
  return _VSTD::make_pair(__pivot_pos, __already_partitioned);
}

template <class _RandomAccessIterator, class _Compare>
inline bool __partial_insertion_sort(_RandomAccessIterator __first,
                                     _RandomAccessIterator __last,
                                     _Compare __comp) {
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type
      value_type;
  if (__first == __last) return true;

  const unsigned __limit = 8;
  unsigned __count = 0;
  _RandomAccessIterator __j = __first;
  for (_RandomAccessIterator __i = __j + 1; __i != __last; ++__i) {
    if (__comp(*__i, *__j)) {
      value_type __t(_VSTD::move(*__i));
      _RandomAccessIterator __k = __j;
      __j = __i;
      do {
        *__j = _VSTD::move(*__k);
        __j = __k;
      } while (__j != __first && __comp(__t, *--__k));
      *__j = _VSTD::move(__t);
      if (++__count == __limit) return ++__i == __last;
    }
    __j = __i;
  }
  return true;
}

template <class _Compare, class _RandomAccessIterator>
void __bitsetsort_loop(
    _RandomAccessIterator __first, _RandomAccessIterator __last,
    _Compare __comp,
    typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type* __buff,
    typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type __limit) {
  _LIBCPP_CONSTEXPR_AFTER_CXX11 int __ninther_threshold = 128;
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type
      difference_type;
  typedef
  typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type value_type;
  __sorting_network::__conditional_swap<_RandomAccessIterator, _Compare>
      __cond_swap(__comp);
  __sorting_network::__reverse_conditional_swap<_RandomAccessIterator, _Compare>
      __reverse_cond_swap(__comp);
  while (true) {
    if (__limit == 0) {
      // Fallback to heap sort as Introsort suggests.
      _VSTD::make_heap(__first, __last, __comp);
      _VSTD::sort_heap(__first, __last, __comp);
      return;
    }
    __limit--;
    difference_type __len = __last - __first;
    if (__len <= __bitonic::__detail::__batch) {
      __sorting_network::__sort1to8(__first, __len, __cond_swap);
      return;
    } else if (__len <= 32) {
      __bitonic::__small_sort(__first, __len, __buff, __cond_swap,
                               __reverse_cond_swap);
      // __bitonic::__sort9to32(__first, __len, __buff, __cond_swap,
      //                           __reverse_cond_swap);
      return;
    }
    difference_type __half_len = __len / 2;
    if (__len > __ninther_threshold) {
      __sorting_network::__sort3(__first, __first + __half_len, __last - 1, __cond_swap);
      __sorting_network::__sort3(__first + 1, __first + (__half_len - 1), __last - 2, __cond_swap);
      __sorting_network::__sort3(__first + 2, __first + (__half_len + 1), __last - 3, __cond_swap);
      __sorting_network::__sort3(__first + (__half_len - 1), __first + __half_len,
              __first + (__half_len + 1), __cond_swap);
      _VSTD::iter_swap(__first, __first + __half_len);
    } else {
      __sorting_network::__sort3(__first + __half_len, __first, __last - 1, __cond_swap);
    }
    auto __ret = __bitset_partition<__64bit_set>(__first, __last, __comp);
    if (__ret.second) {
      bool __left = __partial_insertion_sort(__first, __ret.first, __comp);
      if (__partial_insertion_sort(__ret.first + 1, __last, __comp)) {
        if (__left) return;
        __last = __ret.first;
        continue;
      } else {
        if (__left) {
          __first = ++__ret.first;
          continue;
        }
      }
    }

    // Sort smaller range with recursive call and larger with tail recursion
    // elimination.
    if (__ret.first - __first < __last - __ret.first) {
      __bitsetsort_loop<_Compare>(__first, __ret.first, __comp, __buff, __limit);
      __first = ++__ret.first;
    } else {
      __bitsetsort_loop<_Compare>(__ret.first + 1, __last, __comp, __buff, __limit);
      __last = __ret.first;
    }
  }
}

template<typename _Number>
inline _LIBCPP_INLINE_VISIBILITY _Number __log2i(_Number __n) {
    _Number __log2 = 0;
    while (__n > 1) {
      __log2++;
      __n >>= 1;
    }
    return __log2;
}


template <class _Compare, class _RandomAccessIterator>
inline _LIBCPP_INLINE_VISIBILITY void __bitsetsort_internal(
    _RandomAccessIterator __first, _RandomAccessIterator __last,
    _Compare __comp) {
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type
      value_type;
  typedef typename _VSTD::iterator_traits<_RandomAccessIterator>::difference_type
      difference_type;
  typename _VSTD::aligned_storage<sizeof(value_type), alignof(value_type)>::type
      __buff[__bitonic::__detail::__small_sort_max];

  // 2*log2 comes from Introsort https://reviews.llvm.org/D36423.
  difference_type __depth_limit = 2 * __log2i(__last - __first);
  __bitsetsort_loop(__first, __last, __comp,
                    reinterpret_cast<value_type*>(&__buff[0]),
                    __depth_limit);
}
}  // namespace __bitsetsort

// __branchlesscompimpl provides a branch-less comparator for pairs and tuples of primitive types.
// It provides 1.38x - 2x speed-up in pairs or tuples sorting.
template <bool __primitive>
struct __branchlesscompimpl {
  template <typename R>
  bool operator()(const R& lhs, const R& rhs) const {
    return lhs < rhs;
  }
};

template<>
struct __branchlesscompimpl<true> {
  template <typename R>
  bool operator()(const R& lhs, const R& rhs) const {
    return lhs < rhs;
  }
  template<typename T1, typename T2>
  bool operator()(const _VSTD::pair<T1, T2>& lhs, const _VSTD::pair<T1, T2>& rhs) const {
    const bool __c1 = lhs.first < rhs.first;
    const bool __c2 = rhs.first < lhs.first;
    const bool __c3 = lhs.second < rhs.second;
    return __c1 || (!__c2 && __c3);
  }
  template <typename T1, typename T2>
  bool operator()(const _VSTD::tuple<T1, T2>& lhs, const _VSTD::tuple<T1, T2>& rhs) const {
    const bool __c1 = _VSTD::get<0>(lhs) < _VSTD::get<0>(rhs);
    const bool __c2 = _VSTD::get<0>(rhs) < _VSTD::get<0>(lhs);
    const bool __c3 = _VSTD::get<1>(lhs) < _VSTD::get<1>(rhs);
    return __c1 || (!__c2 && __c3);
  }
  template <typename T1, typename T2, typename T3>
  bool operator()(const _VSTD::tuple<T1, T2, T3>& lhs, const _VSTD::tuple<T1, T2, T3>& rhs) const {
    const bool __c1 = _VSTD::get<0>(lhs) < _VSTD::get<0>(rhs);
    const bool __c2 = _VSTD::get<0>(rhs) < _VSTD::get<0>(lhs);
    const bool __c3 = _VSTD::get<1>(lhs) < _VSTD::get<1>(rhs);
    const bool __c4 = _VSTD::get<1>(rhs) < _VSTD::get<1>(lhs);
    const bool __c5 = _VSTD::get<2>(lhs) < _VSTD::get<2>(rhs);
    return __c1 || (!__c2 && (__c3 || (!__c4 && __c5)));
  }
};

template <typename _T>
struct __branchlesscomp {
  bool operator()(const _T& __x, const _T& __y) const {
    return __x < __y;
  }
};

template <typename T1, typename T2>
struct __branchlesscomp<_VSTD::pair<T1, T2>> : public __branchlesscompimpl<_VSTD::is_fundamental<T2>::value> {};

template <typename T1, typename T2>
struct __branchlesscomp<_VSTD::tuple<T1, T2>> : public __branchlesscompimpl<_VSTD::is_fundamental<T2>::value> {};

template <typename T1, typename T2, typename T3>
struct __branchlesscomp<_VSTD::tuple<T1, T2, T3>> : public __branchlesscompimpl<_VSTD::is_fundamental<T2>::value && _VSTD::is_fundamental<T3>::value> {};

template <class _RandomAccessIterator, class _Compare>
inline _LIBCPP_INLINE_VISIBILITY void bitsetsort(_RandomAccessIterator __first,
                                                 _RandomAccessIterator __last,
                                                 _Compare __comp) {
  /** This change is required for ClickHouse
    * /contrib/libcxx/include/algorithm:789:10: note: candidate function template not viable: 'this' argument has type
    * 'const std::__debug_less<DB::ColumnVector<unsigned short>::less>', but method is not marked const
    * bool operator()(const _Tp& __x,  const _Up& __y)
    */
  typedef typename _VSTD::__comp_ref_type<_Compare>::type _Comp_ref;
  __bitsetsort::__bitsetsort_internal<_Compare>(__first, __last,
                                                 __comp);
}

template <class _Tp, class _Compare>
inline _LIBCPP_INLINE_VISIBILITY void bitsetsort(_VSTD::__wrap_iter<_Tp*> __first,
                                                 _VSTD::__wrap_iter<_Tp*> __last,
                                                 _Compare __comp) {
  typedef typename _VSTD::add_lvalue_reference<_Compare>::type _Comp_ref;
  bitsetsort<_Tp*, _Comp_ref>(__first.base(), __last.base(), __comp);
}

template <class _RandomAccessIterator>
inline _LIBCPP_INLINE_VISIBILITY void bitsetsort(_RandomAccessIterator __first,
                                                 _RandomAccessIterator __last) {
  bitsetsort(
      __first, __last,
      __branchlesscomp<typename _VSTD::iterator_traits<_RandomAccessIterator>::value_type>());
}

template <class _Tp>
inline _LIBCPP_INLINE_VISIBILITY void bitsetsort(_VSTD::__wrap_iter<_Tp*> __first,
                                                 _VSTD::__wrap_iter<_Tp*> __last) {
  bitsetsort(__first.base(), __last.base());
}
}  // namespace stdext

#endif  // _LIBCPP___BITSETSORT
