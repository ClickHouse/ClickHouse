//////////////////////////////////////////////////////////////////////////////
//
// (C) Copyright Ion Gaztanaga 2015-2016.
// Distributed under the Boost Software License, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//
// See http://www.boost.org/libs/move for documentation.
//
//////////////////////////////////////////////////////////////////////////////

//! \file

#ifndef BOOST_MOVE_ALGO_BUFFERLESS_MERGE_SORT_HPP
#define BOOST_MOVE_ALGO_BUFFERLESS_MERGE_SORT_HPP

#ifndef BOOST_CONFIG_HPP
#  include <boost/config.hpp>
#endif
#
#if defined(BOOST_HAS_PRAGMA_ONCE)
#  pragma once
#endif

#include <boost/move/detail/config_begin.hpp>
#include <boost/move/detail/workaround.hpp>

#include <boost/move/utility_core.hpp>
#include <boost/move/adl_move_swap.hpp>

#include <boost/move/algo/move.hpp>
#include <boost/move/algo/detail/merge.hpp>

#include <boost/move/detail/iterator_traits.hpp>
#include <boost/move/algo/detail/insertion_sort.hpp>
#include <cassert>

namespace boost {
namespace movelib {
// @cond
namespace detail_bufferless_mergesort {

static const std::size_t UnbufferedMergeSortInsertionSortThreshold = 16;

//A in-placed version based on:
//Jyrki Katajainen, Tomi Pasanen, Jukka Teuhola.
//``Practical in-place mergesort''. Nordic Journal of Computing, 1996.

template<class RandIt, class Compare>
void bufferless_merge_sort(RandIt first, RandIt last, Compare comp);

template<class RandIt, class Compare>
void swap_sort(RandIt const first, RandIt const last, RandIt const buffer_first, RandIt const buffer_last, Compare comp, bool buffer_at_right)
{
   typedef typename iterator_traits<RandIt>::size_type size_type;
   if (size_type(last - first) > UnbufferedMergeSortInsertionSortThreshold) {
      RandIt m = first + (last - first) / 2;
      bufferless_merge_sort(first, m, comp);
      bufferless_merge_sort(m, last, comp);
      if(buffer_at_right){
         //Use antistable to minimize movements (if equal, move first half elements 
         //to maximize the chance last half elements are already in place.
         boost::movelib::swap_merge_right(first, m, last, buffer_last, boost::movelib::antistable<Compare>(comp));
      }
      else{
         boost::movelib::swap_merge_left(buffer_first, first, m, last, comp);
      }
   }
   else
      boost::movelib::insertion_sort_swap(first, last, buffer_first, comp);
}

template<class RandIt, class Compare>
void bufferless_merge_sort(RandIt const first, RandIt const last, Compare comp)
{
   typedef typename iterator_traits<RandIt>::size_type size_type;
   size_type len = size_type(last - first);
   if (len > size_type(UnbufferedMergeSortInsertionSortThreshold)) {
      len /= 2;
      RandIt h = last - len;  //ceil(half)
      RandIt f = h - len;     //ceil(first)
      swap_sort(f, h, h, last, comp, true);     //[h, last) contains sorted elements

      //Divide unsorted first half in two
      len = size_type(h - first);
      while (len > size_type(UnbufferedMergeSortInsertionSortThreshold)) {
         len /= 2;
         RandIt n = h;  //new end
         h = n - len;   //ceil(half')
         f = h - len;   //ceil(first')
         swap_sort(h, n, f, h, comp, false); // the first half of the previous working area [f, h)
                                             //contains sorted elements: working area in the middle [h, n)
         //Now merge small (left) sorted with big (right) sorted (buffer is between them)
         swap_merge_with_right_placed(f, h, h, n, last, comp); 
      }

      boost::movelib::insertion_sort(first, h, comp);
      boost::movelib::merge_bufferless(first, h, last, comp);
   }
   else{
      boost::movelib::insertion_sort(first, last, comp);
   }
}

}  //namespace detail_bufferless_mergesort {

// @endcond

//Unstable bufferless merge sort
template<class RandIt, class Compare>
void bufferless_merge_sort(RandIt first, RandIt last, Compare comp)
{
   detail_bufferless_mergesort::bufferless_merge_sort(first, last, comp);
}

}}   //namespace boost::movelib

#include <boost/move/detail/config_end.hpp>

#endif //#ifndef BOOST_MOVE_ALGO_BUFFERLESS_MERGE_SORT_HPP
