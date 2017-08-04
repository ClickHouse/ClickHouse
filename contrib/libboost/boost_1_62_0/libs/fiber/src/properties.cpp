//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include "boost/fiber/properties.hpp"

#include <boost/assert.hpp>

#include "boost/fiber/algo/algorithm.hpp"
#include "boost/fiber/scheduler.hpp"
#include "boost/fiber/context.hpp"

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_PREFIX
#endif

namespace boost {
namespace fibers {

void
fiber_properties::notify() noexcept {
    BOOST_ASSERT( nullptr != algo_);
    // Application code might change an important property for any fiber at
    // any time. The fiber in question might be ready, running or waiting.
    // Significantly, only a fiber which is ready but not actually running is
    // in the sched_algorithm's ready queue. Don't bother the sched_algorithm
    // with a change to a fiber it's not currently tracking: it will do the
    // right thing next time the fiber is passed to its awakened() method.
    if ( ctx_->ready_is_linked() ) {
        static_cast< algo::algorithm_with_properties_base * >( algo_)->
            property_change_( ctx_, this);
    }
}

}}                                  // boost::fibers

#ifdef BOOST_HAS_ABI_HEADERS
#  include BOOST_ABI_SUFFIX
#endif
