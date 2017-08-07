
//          Copyright Oliver Kowalke 2009.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/coroutine/detail/coroutine_context.hpp"

#include "boost/coroutine/detail/data.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

#if defined(_MSC_VER)
# pragma warning(push)
# pragma warning(disable:4355)
#endif

#if defined(BOOST_USE_SEGMENTED_STACKS)
extern "C" {

void __splitstack_getcontext( void * [BOOST_CONTEXT_SEGMENTS]);

void __splitstack_setcontext( void * [BOOST_CONTEXT_SEGMENTS]);

}
#endif

namespace boost {
namespace coroutines {
namespace detail {

coroutine_context::coroutine_context() :
    palloc_(),
    ctx_( 0)
{}

coroutine_context::coroutine_context( ctx_fn fn, preallocated const& palloc) :
    palloc_( palloc),
    ctx_( context::detail::make_fcontext( palloc_.sp, palloc_.size, fn) )
{}

coroutine_context::coroutine_context( coroutine_context const& other) :
    palloc_( other.palloc_),
    ctx_( other.ctx_)
{}

coroutine_context &
coroutine_context::operator=( coroutine_context const& other)
{
    if ( this == & other) return * this;

    palloc_ = other.palloc_;
    ctx_ = other.ctx_;

    return * this;
}

void *
coroutine_context::jump( coroutine_context & other, void * param)
{
#if defined(BOOST_USE_SEGMENTED_STACKS)
    __splitstack_getcontext( palloc_.sctx.segments_ctx);
    __splitstack_setcontext( other.palloc_.sctx.segments_ctx);
#endif
    data_t data = { this, param };
    context::detail::transfer_t t = context::detail::jump_fcontext( other.ctx_, & data);
    data_t * ret = static_cast< data_t * >( t.data);
    ret->from->ctx_ = t.fctx;
    return ret->data;
}

}}}

#if defined(_MSC_VER)
# pragma warning(pop)
#endif

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
