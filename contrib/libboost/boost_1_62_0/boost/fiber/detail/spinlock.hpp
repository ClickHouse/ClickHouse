
//          Copyright Oliver Kowalke 2013.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)
//
//  based on boost::interprocess::sync::interprocess_spin::mutex

#ifndef BOOST_FIBERS_SPINLOCK_H
#define BOOST_FIBERS_SPINLOCK_H

#include <mutex>

#include <boost/fiber/detail/config.hpp>

namespace boost {
namespace fibers {
namespace detail {

struct non_spinlock {
    constexpr non_spinlock() noexcept {}
    void lock() noexcept {}
    void unlock() noexcept {}
};

struct non_lock {
    constexpr non_lock( non_spinlock) noexcept {}
    void lock() noexcept {}
    void unlock() noexcept {}
};

#if ! defined(BOOST_FIBERS_NO_ATOMICS) 
typedef std::mutex      spinlock;
using spinlock_lock = std::unique_lock< spinlock >;
#else
typedef non_spinlock    spinlock;
using spinlock_lock = non_lock;
#endif

}}}

#endif // BOOST_FIBERS_SPINLOCK_H
