/*
Copyright 2015 Glen Joseph Fernandes
(glenjofe@gmail.com)

Distributed under the Boost Software License, Version 1.0.
(http://www.boost.org/LICENSE_1_0.txt)
*/
#ifndef BOOST_ALIGN_ALIGN_UP_HPP
#define BOOST_ALIGN_ALIGN_UP_HPP

#include <boost/align/detail/align_up.hpp>

namespace boost {
namespace alignment {

BOOST_CONSTEXPR inline std::size_t
align_up(std::size_t value, std::size_t alignment) BOOST_NOEXCEPT
{
    return (value + alignment - 1) & ~(alignment - 1);
}

} /* alignment */
} /* boost */

#endif
