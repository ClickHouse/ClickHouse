#ifndef BOOST_SMART_PTR_DETAIL_SP_COUNTED_BASE_CLANG_HPP_INCLUDED
#define BOOST_SMART_PTR_DETAIL_SP_COUNTED_BASE_CLANG_HPP_INCLUDED

// MS compatible compilers support #pragma once

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//  detail/sp_counted_base_clang.hpp - __c11 clang intrinsics
//
//  Copyright (c) 2007, 2013, 2015 Peter Dimov
//
//  Distributed under the Boost Software License, Version 1.0.
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt

#include <boost/detail/sp_typeinfo.hpp>
#include <boost/cstdint.hpp>

namespace boost
{

namespace detail
{

typedef _Atomic( boost::int_least32_t ) atomic_int_least32_t;

inline void atomic_increment( atomic_int_least32_t * pw )
{
    __c11_atomic_fetch_add( pw, 1, __ATOMIC_RELAXED );
}

inline boost::int_least32_t atomic_decrement( atomic_int_least32_t * pw )
{
    return __c11_atomic_fetch_sub( pw, 1, __ATOMIC_ACQ_REL );
}

inline boost::int_least32_t atomic_conditional_increment( atomic_int_least32_t * pw )
{
    // long r = *pw;
    // if( r != 0 ) ++*pw;
    // return r;

    boost::int_least32_t r = __c11_atomic_load( pw, __ATOMIC_RELAXED );

    for( ;; )
    {
        if( r == 0 )
        {
            return r;
        }

        if( __c11_atomic_compare_exchange_weak( pw, &r, r + 1, __ATOMIC_RELAXED, __ATOMIC_RELAXED ) )
        {
            return r;
        }
    }    
}

#if defined(__clang__)
# pragma clang diagnostic push
# pragma clang diagnostic ignored "-Wweak-vtables"
#endif

class sp_counted_base
{
private:

    sp_counted_base( sp_counted_base const & );
    sp_counted_base & operator= ( sp_counted_base const & );

    atomic_int_least32_t use_count_;	// #shared
    atomic_int_least32_t weak_count_;	// #weak + (#shared != 0)

public:

    sp_counted_base()
    {
        __c11_atomic_init( &use_count_, 1 );
        __c11_atomic_init( &weak_count_, 1 );
    }

    virtual ~sp_counted_base() // nothrow
    {
    }

    // dispose() is called when use_count_ drops to zero, to release
    // the resources managed by *this.

    virtual void dispose() = 0; // nothrow

    // destroy() is called when weak_count_ drops to zero.

    virtual void destroy() // nothrow
    {
        delete this;
    }

    virtual void * get_deleter( sp_typeinfo const & ti ) = 0;
    virtual void * get_local_deleter( sp_typeinfo const & ti ) = 0;
    virtual void * get_untyped_deleter() = 0;

    void add_ref_copy()
    {
        atomic_increment( &use_count_ );
    }

    bool add_ref_lock() // true on success
    {
        return atomic_conditional_increment( &use_count_ ) != 0;
    }

    void release() // nothrow
    {
        if( atomic_decrement( &use_count_ ) == 1 )
        {
            dispose();
            weak_release();
        }
    }

    void weak_add_ref() // nothrow
    {
        atomic_increment( &weak_count_ );
    }

    void weak_release() // nothrow
    {
        if( atomic_decrement( &weak_count_ ) == 1 )
        {
            destroy();
        }
    }

    long use_count() const // nothrow
    {
        return __c11_atomic_load( const_cast< atomic_int_least32_t* >( &use_count_ ), __ATOMIC_ACQUIRE );
    }
};

#if defined(__clang__)
# pragma clang diagnostic pop
#endif

} // namespace detail

} // namespace boost

#endif  // #ifndef BOOST_SMART_PTR_DETAIL_SP_COUNTED_BASE_CLANG_HPP_INCLUDED
