#ifndef BOOST_SMART_PTR_DETAIL_SP_HAS_SYNC_HPP_INCLUDED
#define BOOST_SMART_PTR_DETAIL_SP_HAS_SYNC_HPP_INCLUDED

// MS compatible compilers support #pragma once

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//
//  boost/smart_ptr/detail/sp_has_sync.hpp
//
//  Copyright (c) 2008, 2009 Peter Dimov
//
//  Distributed under the Boost Software License, Version 1.0.
//  See accompanying file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
//  Defines the BOOST_SP_HAS_SYNC macro if the __sync_* intrinsics
//  are available.
//

#ifndef BOOST_SP_NO_SYNC

#if !defined( __c2__ ) && defined( __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4 )

# define BOOST_SP_HAS_SYNC

#elif defined( __IBMCPP__ ) && ( __IBMCPP__ >= 1210 ) && !defined( __COMPILER_VER__ )

# define BOOST_SP_HAS_SYNC

#elif !defined( __c2__ ) && defined( __GNUC__ ) && ( __GNUC__ * 100 + __GNUC_MINOR__ >= 401 )

#define BOOST_SP_HAS_SYNC

#if defined( __arm__ )  || defined( __armel__ )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined( __hppa ) || defined( __hppa__ )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined( __m68k__ )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined( __sh__ )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined( __sparc__ )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined( __INTEL_COMPILER ) && !defined( __ia64__ ) && ( __INTEL_COMPILER < 1110 )
#undef BOOST_SP_HAS_SYNC
#endif

#if defined(__PATHSCALE__) && ((__PATHCC__ == 4) && (__PATHCC_MINOR__ < 9)) 
#undef BOOST_SP_HAS_SYNC
#endif

#endif

#endif // #ifndef BOOST_SP_NO_SYNC

#endif // #ifndef BOOST_SMART_PTR_DETAIL_SP_HAS_SYNC_HPP_INCLUDED
