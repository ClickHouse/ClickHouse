
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/mutex.hpp"

#include <algorithm>
#include <functional>
#include <system_error>

#include "boost/fiber/exceptions.hpp"
#include "boost/fiber/scheduler.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {

void
mutex::lock() {
    context * ctx = context::active();
    // store this fiber in order to be notified later
    detail::spinlock_lock lk( wait_queue_splk_);
    if ( ctx == owner_) {
        throw lock_error(
                std::make_error_code( std::errc::resource_deadlock_would_occur),
                "boost fiber: a deadlock is detected");
    } else if ( nullptr == owner_) {
        owner_ = ctx;
        return;
    }
    BOOST_ASSERT( ! ctx->wait_is_linked() );
    ctx->wait_link( wait_queue_);
    // suspend this fiber
    ctx->suspend( lk);
    BOOST_ASSERT( ! ctx->wait_is_linked() );
}

bool
mutex::try_lock() {
    context * ctx = context::active();
    detail::spinlock_lock lk( wait_queue_splk_);
    if ( ctx == owner_) {
        throw lock_error(
                std::make_error_code( std::errc::resource_deadlock_would_occur),
                "boost fiber: a deadlock is detected");
    } else if ( nullptr == owner_) {
        owner_ = ctx;
    }
    lk.unlock();
    // let other fiber release the lock
    context::active()->yield();
    return ctx == owner_;
}

void
mutex::unlock() {
    context * ctx = context::active();
    detail::spinlock_lock lk( wait_queue_splk_);
    if ( ctx != owner_) {
        throw lock_error(
                std::make_error_code( std::errc::operation_not_permitted),
                "boost fiber: no  privilege to perform the operation");
    }
    if ( ! wait_queue_.empty() ) {
        context * ctx = & wait_queue_.front();
        wait_queue_.pop_front();
        owner_ = ctx;
        context::active()->set_ready( ctx);
    } else {
        owner_ = nullptr;
        return;
    }
}

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
