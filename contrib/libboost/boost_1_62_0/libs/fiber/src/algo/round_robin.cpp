
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/algo/round_robin.hpp"

#include <boost/assert.hpp>

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace algo {

void
round_robin::awakened( context * ctx) noexcept {
    BOOST_ASSERT( nullptr != ctx);

    BOOST_ASSERT( ! ctx->ready_is_linked() );
    ctx->ready_link( rqueue_);
}

context *
round_robin::pick_next() noexcept {
    context * victim{ nullptr };
    if ( ! rqueue_.empty() ) {
        victim = & rqueue_.front();
        rqueue_.pop_front();
        BOOST_ASSERT( nullptr != victim);
        BOOST_ASSERT( ! victim->ready_is_linked() );
    }
    return victim;
}

bool
round_robin::has_ready_fibers() const noexcept {
    return ! rqueue_.empty();
}

void
round_robin::suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept {
    if ( (std::chrono::steady_clock::time_point::max)() == time_point) {
        std::unique_lock< std::mutex > lk( mtx_);
        cnd_.wait( lk, [&](){ return flag_; });
        flag_ = false;
    } else {
        std::unique_lock< std::mutex > lk( mtx_);
        cnd_.wait_until( lk, time_point, [&](){ return flag_; });
        flag_ = false;
    }
}

void
round_robin::notify() noexcept {
    std::unique_lock< std::mutex > lk( mtx_);
    flag_ = true;
    lk.unlock();
    cnd_.notify_all();
}

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
