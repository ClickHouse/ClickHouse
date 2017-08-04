
//          Copyright Oliver Kowalke 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/context/stack_traits.hpp"

extern "C" {
#include <signal.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <unistd.h>
}

//#if _POSIX_C_SOURCE >= 200112L

#include <algorithm>
#include <cmath>

#include <boost/assert.hpp>
#include <boost/config.hpp>
#if defined(BOOST_NO_CXX11_HDR_MUTEX)
# include <boost/thread.hpp>
#else
# include <mutex>
#endif

#if !defined (SIGSTKSZ)
# define SIGSTKSZ (8 * 1024)
# define UDEF_SIGSTKSZ
#endif

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace {

void pagesize_( std::size_t * size) BOOST_NOEXCEPT_OR_NOTHROW {
    // conform to POSIX.1-2001
    * size = ::sysconf( _SC_PAGESIZE);
}

void stacksize_limit_( rlimit * limit) BOOST_NOEXCEPT_OR_NOTHROW {
    // conforming to POSIX.1-2001
    ::getrlimit( RLIMIT_STACK, limit);
}

std::size_t pagesize() BOOST_NOEXCEPT_OR_NOTHROW {
    static std::size_t size = 0;
#if defined(BOOST_NO_CXX11_HDR_MUTEX)
    static boost::once_flag flag = BOOST_ONCE_INIT;
    boost::call_once( flag, pagesize_, & size);
#else
    static std::once_flag flag;
    std::call_once( flag, pagesize_, & size);
#endif
    return size;
}

rlimit stacksize_limit() BOOST_NOEXCEPT_OR_NOTHROW {
    static rlimit limit;
#if defined(BOOST_NO_CXX11_HDR_MUTEX)
    static boost::once_flag flag = BOOST_ONCE_INIT;
    boost::call_once( flag, stacksize_limit_, & limit);
#else
    static std::once_flag flag;
    std::call_once( flag, stacksize_limit_, & limit);
#endif
    return limit;
}

}

namespace boost {
namespace context {

bool
stack_traits::is_unbounded() BOOST_NOEXCEPT_OR_NOTHROW {
    return RLIM_INFINITY == stacksize_limit().rlim_max;
}

std::size_t
stack_traits::page_size() BOOST_NOEXCEPT_OR_NOTHROW {
    return pagesize();
}

std::size_t
stack_traits::default_size() BOOST_NOEXCEPT_OR_NOTHROW {
    std::size_t size = 8 * minimum_size();
    if ( is_unbounded() ) {
        return size;
    }

    BOOST_ASSERT( maximum_size() >= minimum_size() );
    return maximum_size() == size
        ? size
        : (std::min)( size, maximum_size() );
}

std::size_t
stack_traits::minimum_size() BOOST_NOEXCEPT_OR_NOTHROW {
    return SIGSTKSZ;
}

std::size_t
stack_traits::maximum_size() BOOST_NOEXCEPT_OR_NOTHROW {
    BOOST_ASSERT( ! is_unbounded() );
    return static_cast< std::size_t >( stacksize_limit().rlim_max);
}

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#ifdef UDEF_SIGSTKSZ
# undef SIGSTKSZ
#endif
