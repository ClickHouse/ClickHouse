
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/context/execution_context.hpp"

#include <boost/config.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
# include BOOST_ABI_PREFIX
#endif

#if ! defined(BOOST_CONTEXT_NO_CXX11)
# if (defined(BOOST_EXECUTION_CONTEXT) && (BOOST_EXECUTION_CONTEXT == 1))
namespace boost {
namespace context {
namespace detail {

thread_local
activation_record::ptr_t
activation_record::current_rec;

// zero-initialization
thread_local static std::size_t counter;

// schwarz counter
activation_record_initializer::activation_record_initializer() noexcept {
    if ( 0 == counter++) {
        activation_record::current_rec.reset( new activation_record() );
    }
}

activation_record_initializer::~activation_record_initializer() {
    if ( 0 == --counter) {
        BOOST_ASSERT( activation_record::current_rec->is_main_context() );
        delete activation_record::current_rec.detach();
    }
}

}

execution_context
execution_context::current() noexcept {
    // initialized the first time control passes; per thread
    thread_local static detail::activation_record_initializer initializer;
    return execution_context();
}

}}
# endif
#endif

# ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
# endif
