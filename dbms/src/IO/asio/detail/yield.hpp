//          Copyright Oliver Kowalke, Nat Goodspeed 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#ifndef BOOST_FIBERS_ASIO_DETAIL_YIELD_HPP
#define BOOST_FIBERS_ASIO_DETAIL_YIELD_HPP

#include <boost/asio/async_result.hpp>
#include <boost/asio/detail/config.hpp>
#include <boost/asio/handler_type.hpp>
#include <boost/system/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <boost/throw_exception.hpp>
#include <boost/assert.hpp>

#include <boost/fiber/all.hpp>

#include <mutex>                    // std::unique_lock

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace asio {
namespace detail {

//[fibers_asio_yield_completion
// Bundle a completion bool flag with a spinlock to protect it.
struct yield_completion {
    typedef fibers::detail::spinlock    mutex_t;
    typedef std::unique_lock< mutex_t > lock_t;

    mutex_t mtx_{};
    bool    completed_{ false };

    void wait() {
        // yield_handler_base::operator()() will set completed_ true and
        // attempt to wake a suspended fiber. It would be Bad if that call
        // happened between our detecting (! completed_) and suspending.
        lock_t lk{ mtx_ };
        // If completed_ is already set, we're done here: don't suspend.
        if ( ! completed_) {
            // suspend(unique_lock<spinlock>) unlocks the lock in the act of
            // resuming another fiber
            fibers::context::active()->suspend( lk);
        }
    }
};
//]

//[fibers_asio_yield_handler_base
// This class encapsulates common elements between yield_handler<T> (capturing
// a value to return from asio async function) and yield_handler<void> (no
// such value). See yield_handler<T> and its <void> specialization below. Both
// yield_handler<T> and yield_handler<void> are passed by value through
// various layers of asio functions. In other words, they're potentially
// copied multiple times. So key data such as the yield_completion instance
// must be stored in our async_result<yield_handler<>> specialization, which
// should be instantiated only once.
class yield_handler_base {
public:
    yield_handler_base( yield_t const& y) :
        // capture the context* associated with the running fiber
        ctx_{ boost::fibers::context::active() },
        // capture the passed yield_t
        yt_( y ) {
    }

    // completion callback passing only (error_code)
    void operator()( boost::system::error_code const& ec) {
        BOOST_ASSERT_MSG( ycomp_,
                          "Must inject yield_completion* "
                          "before calling yield_handler_base::operator()()");
        BOOST_ASSERT_MSG( yt_.ec_,
                          "Must inject boost::system::error_code* "
                          "before calling yield_handler_base::operator()()");
        // If originating fiber is busy testing completed_ flag, wait until it
        // has observed (! completed_).
        yield_completion::lock_t lk{ ycomp_->mtx_ };
        // Notify a subsequent yield_completion::wait() call that it need not
        // suspend.
        ycomp_->completed_ = true;
        // set the error_code bound by yield_t
        * yt_.ec_ = ec;
        // If ctx_ is still active, e.g. because the async operation
        // immediately called its callback (this method!) before the asio
        // async function called async_result_base::get(), we must not set it
        // ready.
        if ( fibers::context::active() != ctx_ ) {
            // wake the fiber
            fibers::context::active()->set_ready( ctx_);
        }
    }

//private:
    boost::fibers::context      *   ctx_;
    yield_t                         yt_;
    // We depend on this pointer to yield_completion, which will be injected
    // by async_result.
    yield_completion            *   ycomp_{ nullptr };
};
//]

//[fibers_asio_yield_handler_T
// asio uses handler_type<completion token type, signature>::type to decide
// what to instantiate as the actual handler. Below, we specialize
// handler_type< yield_t, ... > to indicate yield_handler<>. So when you pass
// an instance of yield_t as an asio completion token, asio selects
// yield_handler<> as the actual handler class.
template< typename T >
class yield_handler: public yield_handler_base {
public:
    // asio passes the completion token to the handler constructor
    explicit yield_handler( yield_t const& y) :
        yield_handler_base{ y } {
    }

    // completion callback passing only value (T)
    void operator()( T t) {
        // just like callback passing success error_code
        (*this)( boost::system::error_code(), std::move(t) );
    }

    // completion callback passing (error_code, T)
    void operator()( boost::system::error_code const& ec, T t) {
        BOOST_ASSERT_MSG( value_,
                          "Must inject value ptr "
                          "before caling yield_handler<T>::operator()()");
        // move the value to async_result<> instance BEFORE waking up a
        // suspended fiber
        * value_ = std::move( t);
        // forward the call to base-class completion handler
        yield_handler_base::operator()( ec);
    }

//private:
    // pointer to destination for eventual value
    // this must be injected by async_result before operator()() is called
    T                           *   value_{ nullptr };
};
//]

//[fibers_asio_yield_handler_void
// yield_handler<void> is like yield_handler<T> without value_. In fact it's
// just like yield_handler_base.
template<>
class yield_handler< void >: public yield_handler_base {
public:
    explicit yield_handler( yield_t const& y) :
        yield_handler_base{ y } {
    }

    // nullary completion callback
    void operator()() {
        ( * this)( boost::system::error_code() );
    }

    // inherit operator()(error_code) overload from base class
    using yield_handler_base::operator();
};
//]

// Specialize asio_handler_invoke hook to ensure that any exceptions thrown
// from the handler are propagated back to the caller
template< typename Fn, typename T >
void asio_handler_invoke( Fn fn, yield_handler< T > * h) {
        fn();
}

//[fibers_asio_async_result_base
// Factor out commonality between async_result<yield_handler<T>> and
// async_result<yield_handler<void>>
class async_result_base {
public:
    explicit async_result_base( yield_handler_base & h) {
        // Inject ptr to our yield_completion instance into this
        // yield_handler<>.
        h.ycomp_ = & this->ycomp_;
        // if yield_t didn't bind an error_code, make yield_handler_base's
        // error_code* point to an error_code local to this object so
        // yield_handler_base::operator() can unconditionally store through
        // its error_code*
        if ( ! h.yt_.ec_) {
            h.yt_.ec_ = & ec_;
        }
    }
    
    void get() {
        // Unless yield_handler_base::operator() has already been called,
        // suspend the calling fiber until that call.
        ycomp_.wait();
        // The only way our own ec_ member could have a non-default value is
        // if our yield_handler did not have a bound error_code AND the
        // completion callback passed a non-default error_code.
        if ( ec_) {
            throw_exception( boost::system::system_error{ ec_ } );
        }
    }

private:
    // If yield_t does not bind an error_code instance, store into here.
    boost::system::error_code       ec_{};
    // async_result_base owns the yield_completion because, unlike
    // yield_handler<>, async_result<> is only instantiated once.
    yield_completion                ycomp_{};
};
//]

}}}}

namespace boost {
namespace asio {

//[fibers_asio_async_result_T
// asio constructs an async_result<> instance from the yield_handler specified
// by handler_type<>::type. A particular asio async method constructs the
// yield_handler, constructs this async_result specialization from it, then
// returns the result of calling its get() method.
template< typename T >
class async_result< boost::fibers::asio::detail::yield_handler< T > > :
    public boost::fibers::asio::detail::async_result_base {
public:
    // type returned by get()
    typedef T type;

    explicit async_result( boost::fibers::asio::detail::yield_handler< T > & h) :
        boost::fibers::asio::detail::async_result_base{ h } {
        // Inject ptr to our value_ member into yield_handler<>: result will
        // be stored here.
        h.value_ = & value_;
    }

    // asio async method returns result of calling get()
    type get() {
        boost::fibers::asio::detail::async_result_base::get();
        return std::move( value_);
    }

private:
    type                            value_{};
};
//]

//[fibers_asio_async_result_void
// Without the need to handle a passed value, our yield_handler<void>
// specialization is just like async_result_base.
template<>
class async_result< boost::fibers::asio::detail::yield_handler< void > > :
    public boost::fibers::asio::detail::async_result_base {
public:
    typedef void type;

    explicit async_result( boost::fibers::asio::detail::yield_handler< void > & h):
        boost::fibers::asio::detail::async_result_base{ h } {
    }
};
//]

// Handler type specialisation for fibers::asio::yield.
// When 'yield' is passed as a completion handler which accepts no parameters,
// use yield_handler<void>.
template< typename ReturnType >
struct handler_type< fibers::asio::yield_t, ReturnType() >
{ typedef fibers::asio::detail::yield_handler< void >    type; };

// Handler type specialisation for fibers::asio::yield.
// When 'yield' is passed as a completion handler which accepts a data
// parameter, use yield_handler<parameter type> to return that parameter to
// the caller.
template< typename ReturnType, typename Arg1 >
struct handler_type< fibers::asio::yield_t, ReturnType( Arg1) >
{ typedef fibers::asio::detail::yield_handler< Arg1 >    type; };

//[asio_handler_type
// Handler type specialisation for fibers::asio::yield.
// When 'yield' is passed as a completion handler which accepts only
// error_code, use yield_handler<void>. yield_handler will take care of the
// error_code one way or another.
template< typename ReturnType >
struct handler_type< fibers::asio::yield_t, ReturnType( boost::system::error_code) >
{ typedef fibers::asio::detail::yield_handler< void >    type; };
//]

// Handler type specialisation for fibers::asio::yield.
// When 'yield' is passed as a completion handler which accepts a data
// parameter and an error_code, use yield_handler<parameter type> to return
// just the parameter to the caller. yield_handler will take care of the
// error_code one way or another.
template< typename ReturnType, typename Arg2 >
struct handler_type< fibers::asio::yield_t, ReturnType( boost::system::error_code, Arg2) >
{ typedef fibers::asio::detail::yield_handler< Arg2 >    type; };

}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif

#endif // BOOST_FIBERS_ASIO_DETAIL_YIELD_HPP
