
//          Copyright Oliver Kowalke / Nat Goodspeed 2015.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/algo/algorithm.hpp"

#include "boost/fiber/context.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {
namespace algo {

//static
fiber_properties *
algorithm_with_properties_base::get_properties( context * ctx) noexcept {
    return ctx->get_properties();
}

//static
void
algorithm_with_properties_base::set_properties( context * ctx, fiber_properties * props) noexcept {
    ctx->set_properties( props);
}

}}}

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
