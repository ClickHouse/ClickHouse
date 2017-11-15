/*
Copyright 2014 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under the Boost Software License, Version 1.0.
(http://www.boost.org/LICENSE_1_0.txt)

*/
#ifndef BOOST_ALIGN_IS_ALIGNED_HPP
#define BOOST_ALIGN_IS_ALIGNED_HPP

#include <boost/align/detail/is_aligned.hpp>

namespace boost {
namespace alignment {

BOOST_CONSTEXPR inline bool
is_aligned(std::size_t value, std::size_t alignment) BOOST_NOEXCEPT
{
    return (value & (alignment - 1)) == 0;
}

} /* alignment */
} /* boost */

#endif
