#ifndef BOOST_SMART_PTR_DETAIL_LWM_WIN32_CS_HPP_INCLUDED
#define BOOST_SMART_PTR_DETAIL_LWM_WIN32_CS_HPP_INCLUDED

// MS compatible compilers support #pragma once

#if defined(_MSC_VER) && (_MSC_VER >= 1020)
# pragma once
#endif

//
//  boost/detail/lwm_win32_cs.hpp
//
//  Copyright (c) 2002, 2003 Peter Dimov
//  Copyright (c) Microsoft Corporation 2014
//
// Distributed under the Boost Software License, Version 1.0. (See
// accompanying file LICENSE_1_0.txt or copy at
// http://www.boost.org/LICENSE_1_0.txt)
//

#include <boost/predef.h>

#ifdef BOOST_USE_WINDOWS_H

#include <windows.h>

#else

struct _RTL_CRITICAL_SECTION;

#endif

namespace boost
{

namespace detail
{

#ifndef BOOST_USE_WINDOWS_H

struct critical_section
{
    struct critical_section_debug * DebugInfo;
    long LockCount;
    long RecursionCount;
    void * OwningThread;
    void * LockSemaphore;
#if defined(_WIN64)
    unsigned __int64 SpinCount;
#else
    unsigned long SpinCount;
#endif
};

#if BOOST_PLAT_WINDOWS_RUNTIME
extern "C" __declspec(dllimport) void __stdcall InitializeCriticalSectionEx(::_RTL_CRITICAL_SECTION *, unsigned long, unsigned long);
#else
extern "C" __declspec(dllimport) void __stdcall InitializeCriticalSection(::_RTL_CRITICAL_SECTION *);
#endif
extern "C" __declspec(dllimport) void __stdcall EnterCriticalSection(::_RTL_CRITICAL_SECTION *);
extern "C" __declspec(dllimport) void __stdcall LeaveCriticalSection(::_RTL_CRITICAL_SECTION *);
extern "C" __declspec(dllimport) void __stdcall DeleteCriticalSection(::_RTL_CRITICAL_SECTION *);

#else

typedef ::CRITICAL_SECTION critical_section;

#endif // #ifndef BOOST_USE_WINDOWS_H

class lightweight_mutex
{
private:

    critical_section cs_;

    lightweight_mutex(lightweight_mutex const &);
    lightweight_mutex & operator=(lightweight_mutex const &);

public:

    lightweight_mutex()
    {
#if BOOST_PLAT_WINDOWS_RUNTIME
        boost::detail::InitializeCriticalSectionEx(reinterpret_cast< ::_RTL_CRITICAL_SECTION* >(&cs_), 4000, 0);
#else
        boost::detail::InitializeCriticalSection(reinterpret_cast< ::_RTL_CRITICAL_SECTION* >(&cs_));
#endif
    }

    ~lightweight_mutex()
    {
        boost::detail::DeleteCriticalSection(reinterpret_cast< ::_RTL_CRITICAL_SECTION* >(&cs_));
    }

    class scoped_lock;
    friend class scoped_lock;

    class scoped_lock
    {
    private:

        lightweight_mutex & m_;

        scoped_lock(scoped_lock const &);
        scoped_lock & operator=(scoped_lock const &);

    public:

        explicit scoped_lock(lightweight_mutex & m): m_(m)
        {
            boost::detail::EnterCriticalSection(reinterpret_cast< ::_RTL_CRITICAL_SECTION* >(&m_.cs_));
        }

        ~scoped_lock()
        {
            boost::detail::LeaveCriticalSection(reinterpret_cast< ::_RTL_CRITICAL_SECTION* >(&m_.cs_));
        }
    };
};

} // namespace detail

} // namespace boost

#endif // #ifndef BOOST_SMART_PTR_DETAIL_LWM_WIN32_CS_HPP_INCLUDED
