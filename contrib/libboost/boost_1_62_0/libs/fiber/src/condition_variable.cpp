
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/condition_variable.hpp"

#include "boost/fiber/context.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {

void
condition_variable_any::notify_one() noexcept {
    // get one context' from wait-queue
    detail::spinlock_lock lk( wait_queue_splk_);
    if ( wait_queue_.empty() ) {
        return;
    }
    context * ctx = & wait_queue_.front();
    wait_queue_.pop_front();
    // notify context
    context::active()->set_ready( ctx);
}

void
condition_variable_any::notify_all() noexcept {
    // get all context' from wait-queue
    detail::spinlock_lock lk( wait_queue_splk_);
    // notify all context'
    while ( ! wait_queue_.empty() ) {
        context * ctx = & wait_queue_.front();
        wait_queue_.pop_front();
        context::active()->set_ready( ctx);
    }
}

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
