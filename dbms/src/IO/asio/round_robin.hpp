//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_FIBERS_ASIO_ROUND_ROBIN_H
#define BOOST_FIBERS_ASIO_ROUND_ROBIN_H

#include <chrono>
#include <cstddef>
#include <mutex>
#include <queue>

#include <boost/asio.hpp>
#include <boost/assert.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/config.hpp>

#include <boost/fiber/condition_variable.hpp>
#include <boost/fiber/context.hpp>
#include <boost/fiber/mutex.hpp>
#include <boost/fiber/operations.hpp>
#include <boost/fiber/scheduler.hpp>

#include "yield.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace asio {

class round_robin : public algo::algorithm {
private:
    typedef scheduler::ready_queue_t rqueue_t;

//[asio_rr_suspend_timer
    boost::asio::io_service                     &   io_svc_;
    boost::asio::steady_timer                       suspend_timer_;
//]
    rqueue_t                                        rqueue_{};

public:
//[asio_rr_service_top
    struct service : public boost::asio::io_service::service {
        static boost::asio::io_service::id                  id;

        std::unique_ptr< boost::asio::io_service::work >    work_;

        service( boost::asio::io_service & io_svc) :
            boost::asio::io_service::service( io_svc),
            work_{ new boost::asio::io_service::work( io_svc) } {
            io_svc.post([&io_svc](){
//]
//[asio_rr_service_lambda
                while ( ! io_svc.stopped() ) {
                    if ( boost::fibers::has_ready_fibers() ) {
                        // run all pending handlers in round_robin
                        while ( io_svc.poll() );
                        // run pending (ready) fibers
                        this_fiber::yield();
                    } else {
                        // run one handler inside io_service
                        // if no handler available, block this thread
                        if ( ! io_svc.run_one() ) {
                            break;
                        }
                    }
                }
//]
//[asio_rr_service_bottom
            });
        }

        virtual ~service() {}

        service( service const&) = delete;
        service & operator=( service const&) = delete;

        void shutdown_service() override final {
            work_.reset();
        }
    };
//]

//[asio_rr_ctor
    round_robin( boost::asio::io_service & io_svc) :
        io_svc_( io_svc),
        suspend_timer_( io_svc_) {
        // We use add_service() very deliberately. This will throw
        // service_already_exists if you pass the same io_service instance to
        // more than one round_robin instance.
        boost::asio::add_service( io_svc_, new service( io_svc_));
    }
//]

    void awakened( context * ctx) noexcept {
        BOOST_ASSERT( nullptr != ctx);
        ctx->ready_link( rqueue_); /*< fiber, enqueue on ready queue >*/
    }

    context * pick_next() noexcept {
        context * ctx( nullptr);
        if ( ! rqueue_.empty() ) { /*<
            pop an item from the ready queue
        >*/
            ctx = & rqueue_.front();
            rqueue_.pop_front();
            BOOST_ASSERT( nullptr != ctx);
            BOOST_ASSERT( context::active() != ctx);
        }
        return ctx;
    }

    bool has_ready_fibers() const noexcept {
        return ! rqueue_.empty();
    }

//[asio_rr_suspend_until
    void suspend_until( std::chrono::steady_clock::time_point const& abs_time) noexcept {
        // Set a timer so at least one handler will eventually fire, causing
        // run_one() to eventually return. Set a timer even if abs_time ==
        // time_point::max() so the timer can be canceled by our notify()
        // method -- which calls the handler.
        if ( suspend_timer_.expires_at() != abs_time) {
            // Each expires_at(time_point) call cancels any previous pending
            // call. We could inadvertently spin like this:
            // dispatcher calls suspend_until() with earliest wake time
            // suspend_until() sets suspend_timer_
            // lambda loop calls run_one()
            // some other asio handler runs before timer expires
            // run_one() returns to lambda loop
            // lambda loop yields to dispatcher
            // dispatcher finds no ready fibers
            // dispatcher calls suspend_until() with SAME wake time
            // suspend_until() sets suspend_timer_ to same time, canceling
            // previous async_wait()
            // lambda loop calls run_one()
            // asio calls suspend_timer_ handler with operation_aborted
            // run_one() returns to lambda loop... etc. etc.
            // So only actually set the timer when we're passed a DIFFERENT
            // abs_time value.
            suspend_timer_.expires_at( abs_time);
            // It really doesn't matter what the suspend_timer_ handler does,
            // or even whether it's called because the timer ran out or was
            // canceled. The whole point is to cause the run_one() call to
            // return. So just pass a no-op lambda with proper signature.
            suspend_timer_.async_wait([](boost::system::error_code const&){});
        }
    }
//]

//[asio_rr_notify
    void notify() noexcept {
        // Something has happened that should wake one or more fibers BEFORE
        // suspend_timer_ expires. Reset the timer to cause it to fire
        // immediately, causing the run_one() call to return. In theory we
        // could use cancel() because we don't care whether suspend_timer_'s
        // handler is called with operation_aborted or success. However --
        // cancel() doesn't change the expiration time, and we use
        // suspend_timer_'s expiration time to decide whether it's already
        // set. If suspend_until() set some specific wake time, then notify()
        // canceled it, then suspend_until() was called again with the same
        // wake time, it would match suspend_timer_'s expiration time and we'd
        // refrain from setting the timer. So instead of simply calling
        // cancel(), reset the timer, which cancels the pending sleep AND sets
        // a new expiration time. This will cause us to spin the loop twice --
        // once for the operation_aborted handler, once for timer expiration
        // -- but that shouldn't be a big problem.
        suspend_timer_.expires_at( std::chrono::steady_clock::now() );
    }
//]
};

boost::asio::io_service::id round_robin::service::id;

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_FIBERS_ASIO_ROUND_ROBIN_H
