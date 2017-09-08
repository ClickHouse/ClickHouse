//  basic_types.hpp  --------------------------------------------------------------//

//  Copyright 2010 Vicente J. Botet Escriba
//  Copyright 2015 Andrey Semashev

//  Distributed under the Boost Software License, Version 1.0.
//  See http://www.boost.org/LICENSE_1_0.txt

#ifndef BOOST_DETAIL_WINAPI_BASIC_TYPES_HPP
#define BOOST_DETAIL_WINAPI_BASIC_TYPES_HPP

#include <cstdarg>
#include <boost/cstdint.hpp>
#include <boost/detail/winapi/config.hpp>

#ifdef BOOST_HAS_PRAGMA_ONCE
#pragma once
#endif

#if defined( BOOST_USE_WINDOWS_H )
# include <windows.h>
#elif defined( WIN32 ) || defined( _WIN32 ) || defined( __WIN32__ ) ||  defined(__CYGWIN__)
# include <winerror.h>
# ifdef UNDER_CE
#  ifndef WINAPI
#   ifndef _WIN32_WCE_EMULATION
#    define WINAPI  __cdecl     // Note this doesn't match the desktop definition
#   else
#    define WINAPI  __stdcall
#   endif
#  endif
// Windows CE defines a few functions as inline functions in kfuncs.h
typedef int BOOL;
typedef unsigned long DWORD;
typedef void* HANDLE;
#  include <kfuncs.h>
# else
#  ifndef WINAPI
#   define WINAPI  __stdcall
#  endif
# endif
# ifndef NTAPI
#  define NTAPI __stdcall
# endif
#else
# error "Win32 functions not available"
#endif

#ifndef NO_STRICT
#ifndef STRICT
#define STRICT 1
#endif
#endif

#if defined(STRICT)
#define BOOST_DETAIL_WINAPI_DECLARE_HANDLE(x) struct x##__; typedef struct x##__ *x
#else
#define BOOST_DETAIL_WINAPI_DECLARE_HANDLE(x) typedef void* x
#endif

#if !defined( BOOST_USE_WINDOWS_H )
extern "C" {
union _LARGE_INTEGER;
struct _SECURITY_ATTRIBUTES;
BOOST_DETAIL_WINAPI_DECLARE_HANDLE(HINSTANCE);
typedef HINSTANCE HMODULE;
}
#endif

#if defined(__GNUC__)
#define BOOST_DETAIL_WINAPI_MAY_ALIAS __attribute__ ((__may_alias__))
#else
#define BOOST_DETAIL_WINAPI_MAY_ALIAS
#endif

// MinGW64 gcc 4.8.2 fails to compile function declarations with boost::detail::winapi::VOID_ arguments even though
// the typedef expands to void. In Windows SDK, VOID is a macro which unfolds to void. We use our own macro in such cases.
#define BOOST_DETAIL_WINAPI_VOID void

namespace boost {
namespace detail {
namespace winapi {
#if defined( BOOST_USE_WINDOWS_H )

typedef ::BOOL BOOL_;
typedef ::PBOOL PBOOL_;
typedef ::LPBOOL LPBOOL_;
typedef ::BOOLEAN BOOLEAN_;
typedef ::PBOOLEAN PBOOLEAN_;
typedef ::BYTE BYTE_;
typedef ::PBYTE PBYTE_;
typedef ::LPBYTE LPBYTE_;
typedef ::WORD WORD_;
typedef ::PWORD PWORD_;
typedef ::LPWORD LPWORD_;
typedef ::DWORD DWORD_;
typedef ::PDWORD PDWORD_;
typedef ::LPDWORD LPDWORD_;
typedef ::HANDLE HANDLE_;
typedef ::PHANDLE PHANDLE_;
typedef ::SHORT SHORT_;
typedef ::PSHORT PSHORT_;
typedef ::USHORT USHORT_;
typedef ::PUSHORT PUSHORT_;
typedef ::INT INT_;
typedef ::PINT PINT_;
typedef ::LPINT LPINT_;
typedef ::UINT UINT_;
typedef ::PUINT PUINT_;
typedef ::LONG LONG_;
typedef ::PLONG PLONG_;
typedef ::LPLONG LPLONG_;
typedef ::ULONG ULONG_;
typedef ::PULONG PULONG_;
typedef ::LONGLONG LONGLONG_;
typedef ::ULONGLONG ULONGLONG_;
typedef ::INT_PTR INT_PTR_;
typedef ::UINT_PTR UINT_PTR_;
typedef ::LONG_PTR LONG_PTR_;
typedef ::ULONG_PTR ULONG_PTR_;
typedef ::DWORD_PTR DWORD_PTR_;
typedef ::PDWORD_PTR PDWORD_PTR_;
typedef ::SIZE_T SIZE_T_;
typedef ::PSIZE_T PSIZE_T_;
typedef ::SSIZE_T SSIZE_T_;
typedef ::PSSIZE_T PSSIZE_T_;
typedef VOID VOID_; // VOID is a macro
typedef ::PVOID PVOID_;
typedef ::LPVOID LPVOID_;
typedef ::LPCVOID LPCVOID_;
typedef ::CHAR CHAR_;
typedef ::LPSTR LPSTR_;
typedef ::LPCSTR LPCSTR_;
typedef ::WCHAR WCHAR_;
typedef ::LPWSTR LPWSTR_;
typedef ::LPCWSTR LPCWSTR_;

#else // defined( BOOST_USE_WINDOWS_H )

typedef int BOOL_;
typedef BOOL_* PBOOL_;
typedef BOOL_* LPBOOL_;
typedef unsigned char BYTE_;
typedef BYTE_* PBYTE_;
typedef BYTE_* LPBYTE_;
typedef BYTE_ BOOLEAN_;
typedef BOOLEAN_* PBOOLEAN_;
typedef unsigned short WORD_;
typedef WORD_* PWORD_;
typedef WORD_* LPWORD_;
typedef unsigned long DWORD_;
typedef DWORD_* PDWORD_;
typedef DWORD_* LPDWORD_;
typedef void* HANDLE_;
typedef void** PHANDLE_;

typedef short SHORT_;
typedef SHORT_* PSHORT_;
typedef unsigned short USHORT_;
typedef USHORT_* PUSHORT_;
typedef int INT_;
typedef INT_* PINT_;
typedef INT_* LPINT_;
typedef unsigned int UINT_;
typedef UINT_* PUINT_;
typedef long LONG_;
typedef LONG_* PLONG_;
typedef LONG_* LPLONG_;
typedef unsigned long ULONG_;
typedef ULONG_* PULONG_;

typedef boost::int64_t LONGLONG_;
typedef boost::uint64_t ULONGLONG_;

# ifdef _WIN64
#  if defined(__CYGWIN__)
typedef long INT_PTR_;
typedef unsigned long UINT_PTR_;
typedef long LONG_PTR_;
typedef unsigned long ULONG_PTR_;
#  else
typedef __int64 INT_PTR_;
typedef unsigned __int64 UINT_PTR_;
typedef __int64 LONG_PTR_;
typedef unsigned __int64 ULONG_PTR_;
#  endif
# else
typedef int INT_PTR_;
typedef unsigned int UINT_PTR_;
typedef long LONG_PTR_;
typedef unsigned long ULONG_PTR_;
# endif

typedef ULONG_PTR_ DWORD_PTR_, *PDWORD_PTR_;
typedef ULONG_PTR_ SIZE_T_, *PSIZE_T_;
typedef LONG_PTR_ SSIZE_T_, *PSSIZE_T_;

typedef void VOID_;
typedef void *PVOID_;
typedef void *LPVOID_;
typedef const void *LPCVOID_;

typedef char CHAR_;
typedef CHAR_ *LPSTR_;
typedef const CHAR_ *LPCSTR_;

typedef wchar_t WCHAR_;
typedef WCHAR_ *LPWSTR_;
typedef const WCHAR_ *LPCWSTR_;

#endif // defined( BOOST_USE_WINDOWS_H )

typedef ::HMODULE HMODULE_;

typedef union BOOST_DETAIL_WINAPI_MAY_ALIAS _LARGE_INTEGER {
    struct {
        DWORD_ LowPart;
        LONG_ HighPart;
    } u;
    LONGLONG_ QuadPart;
} LARGE_INTEGER_, *PLARGE_INTEGER_;

typedef struct BOOST_DETAIL_WINAPI_MAY_ALIAS _SECURITY_ATTRIBUTES {
    DWORD_  nLength;
    LPVOID_ lpSecurityDescriptor;
    BOOL_   bInheritHandle;
} SECURITY_ATTRIBUTES_, *PSECURITY_ATTRIBUTES_, *LPSECURITY_ATTRIBUTES_;

}
}
}

#endif // BOOST_DETAIL_WINAPI_BASIC_TYPES_HPP
