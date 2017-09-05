//  debugapi.hpp  --------------------------------------------------------------
//
//  Copyright 2017 Vinnie Falco
//
//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt


#ifndef BOOST_DETAIL_WINAPI_DEBUGAPI_HPP
#define BOOST_DETAIL_WINAPI_DEBUGAPI_HPP

#include <boost/detail/winapi/basic_types.hpp>
#include <boost/detail/winapi/config.hpp>

#ifdef BOOST_HAS_PRAGMA_ONCE
#pragma once
#endif

#if !defined( BOOST_USE_WINDOWS_H )
extern "C" {

#if (BOOST_USE_WINAPI_VERSION >= BOOST_WINAPI_VERSION_NT4)

BOOST_SYMBOL_IMPORT
boost::detail::winapi::BOOL_
WINAPI
IsDebuggerPresent(
    BOOST_DETAIL_WINAPI_VOID
    );

#endif

BOOST_SYMBOL_IMPORT
boost::detail::winapi::VOID_
WINAPI
OutputDebugStringA(
    boost::detail::winapi::LPCSTR_
    );

BOOST_SYMBOL_IMPORT
boost::detail::winapi::VOID_
WINAPI
OutputDebugStringW(
    boost::detail::winapi::LPCWSTR_
    );

}
#endif // extern "C"

namespace boost {
namespace detail {
namespace winapi {

#if (BOOST_USE_WINAPI_VERSION >= BOOST_WINAPI_VERSION_NT4)
using ::IsDebuggerPresent;
#endif

using ::OutputDebugStringA;
using ::OutputDebugStringW;

inline
void
output_debug_string(char const* s)
{
    ::OutputDebugStringA(s);
}

inline
void
output_debug_string(wchar_t const* s)
{
    ::OutputDebugStringW(s);
}

}
}
}

#endif // BOOST_DETAIL_WINAPI_DEBUGAPI_HPP
