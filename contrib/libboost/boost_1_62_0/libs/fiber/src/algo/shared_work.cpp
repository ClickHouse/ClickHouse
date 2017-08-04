
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/algo/shared_work.hpp"

#include <boost/assert.hpp>

#include "boost/fiber/type.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace algo {

//[awakened_ws
void
shared_work::awakened( context * ctx) noexcept {
    if ( ctx->is_context( type::pinned_context) ) { /*<
            recognize when we're passed this thread's main fiber (or an
            implicit library helper fiber): never put those on the shared
            queue
        >*/
        lqueue_.push_back( * ctx);
    } else {
        ctx->detach();
        std::unique_lock< std::mutex > lk( rqueue_mtx_); /*<
                worker fiber, enqueue on shared queue
            >*/
        rqueue_.push_back( ctx);
    }
}
//]

//[pick_next_ws
context *
shared_work::pick_next() noexcept {
    context * ctx( nullptr);
    std::unique_lock< std::mutex > lk( rqueue_mtx_);
    if ( ! rqueue_.empty() ) { /*<
            pop an item from the ready queue
        >*/
        ctx = rqueue_.front();
        rqueue_.pop_front();
        lk.unlock();
        BOOST_ASSERT( nullptr != ctx);
        context::active()->attach( ctx); /*<
            attach context to current scheduler via the active fiber
            of this thread
        >*/
    } else {
        lk.unlock();
        if ( ! lqueue_.empty() ) { /*<
                nothing in the ready queue, return main or dispatcher fiber
            >*/
            ctx = & lqueue_.front();
            lqueue_.pop_front();
        }
    }
    return ctx;
}
//]

void
shared_work::suspend_until( std::chrono::steady_clock::time_point const& time_point) noexcept {
    if ( suspend_) {
        if ( (std::chrono::steady_clock::time_point::max)() == time_point) {
            std::unique_lock< std::mutex > lk( mtx_);
            cnd_.wait( lk, [this](){ return flag_; });
            flag_ = false;
        } else {
            std::unique_lock< std::mutex > lk( mtx_);
            cnd_.wait_until( lk, time_point, [this](){ return flag_; });
            flag_ = false;
        }
    }
}

void
shared_work::notify() noexcept {
    if ( suspend_) {
        std::unique_lock< std::mutex > lk( mtx_);
        flag_ = true;
        lk.unlock();
        cnd_.notify_all();
    }
}

shared_work::rqueue_t shared_work::rqueue_{};
std::mutex shared_work::rqueue_mtx_{};

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
