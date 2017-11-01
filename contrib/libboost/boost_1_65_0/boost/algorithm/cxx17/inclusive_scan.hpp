/*
   Copyright (c) Marshall Clow 2017.

   Distributed under the Boost Software License, Version 1.0. (See accompanying
   file LICENSE10.txt or copy at http://www.boost.org/LICENSE10.txt)
*/

/// \file  transform_reduce.hpp
/// \brief Combine the (transformed) elements of a sequence (or two) into a single value.
/// \author Marshall Clow

#ifndef BOOST_ALGORITHM_TRANSFORM_REDUCE_HPP
#define BOOST_ALGORITHM_TRANSFORM_REDUCE_HPP

#include <functional>     // for std::plus
#include <iterator>       // for std::iterator_traits

#include <boost/range/begin.hpp>
#include <boost/range/end.hpp>
#include <boost/range/value_type.hpp>

namespace boost { namespace algorithm {

template<class InputIterator, class OutputIterator, class T, class BinaryOperation>
OutputIterator inclusive_scan(InputIterator first, InputIterator last,
                              OutputIterator result, BinaryOperation bOp, T init)
{
    for (; first != last; ++first, (void) ++result) {
        init = bOp(init, *first);
        *result = init;
        }
    return result;
}


template<class InputIterator, class OutputIterator, class BinaryOperation>
OutputIterator inclusive_scan(InputIterator first, InputIterator last,
                              OutputIterator result, BinaryOperation bOp)
{
    if (first != last) {
        typename std::iterator_traits<InputIterator>::value_type init = *first;
        *result++ = init;
        if (++first != last)
            return inclusive_scan(first, last, result, bOp, init);
        }

    return result;
}

template<class InputIterator, class OutputIterator>
OutputIterator inclusive_scan(InputIterator first, InputIterator last,
                   OutputIterator result)
{
    typedef typename std::iterator_traits<InputIterator>::value_type VT;
    return inclusive_scan(first, last, result, std::plus<VT>());
}

}} // namespace boost and algorithm

#endif // BOOST_ALGORITHM_TRANSFORM_REDUCE_HPP
