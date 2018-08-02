#pragma once

#include <type_traits>
#include <utility>
#include <iterator>
#include <boost/iterator/counting_iterator.hpp>


/** Numeric range iterator, used to represent a half-closed interval [begin, end).
  * In conjunction with std::reverse_iterator allows for forward and backward iteration
  * over corresponding interval.
  */
namespace ext
{
    template <typename T>
    using range_iterator = boost::counting_iterator<T>;

    /** Range-based for loop adapter for (reverse_)range_iterator.
      * By and large should be in conjunction with ext::range and ext::reverse_range.
      */
    template <typename T>
    struct range_wrapper
    {
        using value_type = typename std::remove_reference<T>::type;
        using iterator = range_iterator<value_type>;

        value_type begin_;
        value_type end_;

        iterator begin() const { return iterator(begin_); }
        iterator end() const { return iterator(end_); }
    };

    /** Constructs range_wrapper for forward-iteration over [begin, end) in range-based for loop.
      *  Usage example:
      *      for (const auto i : ext::range(0, 4)) print(i);
      *  Output:
      *      0 1 2 3
      */
    template <typename T1, typename T2>
    inline range_wrapper<typename std::common_type<T1, T2>::type> range(T1 begin, T2 end)
    {
        using common_type = typename std::common_type<T1, T2>::type;
        return { static_cast<common_type>(begin), static_cast<common_type>(end) };
    }
}
