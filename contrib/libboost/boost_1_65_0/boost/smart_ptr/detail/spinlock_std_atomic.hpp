#ifndef BOOST_SMART_PTR_DETAIL_SPINLOCK_STD_ATOMIC_HPP_INCLUDED
#define BOOST_SMART_PTR_DETAIL_SPINLOCK_STD_ATOMIC_HPP_INCLUDED

// MS compatible compilers support #pragma once

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//
//  Copyright (c) 2014 Peter Dimov
//
//  Distributed under the Boost Software License, Version 1.0.
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/smart_ptr/detail/yield_k.hpp>
#include <atomic>

namespace boost
{

namespace detail
{

class spinlock
{
public:

    std::atomic_flag v_;

public:

    bool try_lock()
    {
        return !v_.test_and_set( std::memory_order_acquire );
    }

    void lock()
    {
        for( unsigned k = 0; !try_lock(); ++k )
        {
            boost::detail::yield( k );
        }
    }

    void unlock()
    {
        v_ .clear( std::memory_order_release );
    }

public:

    class scoped_lock
    {
    private:

        spinlock & sp_;

        scoped_lock( scoped_lock const & );
        scoped_lock & operator=( scoped_lock const & );

    public:

        explicit scoped_lock( spinlock & sp ): sp_( sp )
        {
            sp.lock();
        }

        ~scoped_lock()
        {
            sp_.unlock();
        }
    };
};

} // namespace detail
} // namespace boost

#define BOOST_DETAIL_SPINLOCK_INIT { ATOMIC_FLAG_INIT }

#endif // #ifndef BOOST_SMART_PTR_DETAIL_SPINLOCK_STD_ATOMIC_HPP_INCLUDED
