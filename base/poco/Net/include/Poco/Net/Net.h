//
// Net.h
//
// Library: Net
// Package: NetCore
// Module:  Net
//
// Basic definitions for the Poco Net library.
// This file must be the first file included by every other Net
// header file.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_Net_INCLUDED
#define Net_Net_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Net_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Net_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(Net_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define Net_API __attribute__((visibility("default")))
#    else
#        define Net_API
#    endif
#endif


//
// Automatically link Net library.
//


// Default to enabled IPv6 support if not explicitly disabled
#if !defined(POCO_NET_NO_IPv6) && !defined(POCO_HAVE_IPv6)
#    define POCO_HAVE_IPv6
#elif defined(POCO_NET_NO_IPv6) && defined(POCO_HAVE_IPv6)
#    undef POCO_HAVE_IPv6
#endif // POCO_NET_NO_IPv6, POCO_HAVE_IPv6


namespace Poco
{
namespace Net
{


    void Net_API initializeNetwork();
    /// Initialize the network subsystem.
    /// (Windows only, no-op elsewhere)


    void Net_API uninitializeNetwork();
    /// Uninitialize the network subsystem.
    /// (Windows only, no-op elsewhere)


}
} // namespace Poco::Net


//
// Automate network initialization (only relevant on Windows).
//



//
// Define POCO_NET_HAS_INTERFACE for platforms that have network interface detection implemented.
//
#if defined(POCO_OS_FAMILY_WINDOWS) || (POCO_OS == POCO_OS_LINUX) || (POCO_OS == POCO_OS_ANDROID) || defined(POCO_OS_FAMILY_BSD) \
    || (POCO_OS == POCO_OS_SOLARIS) || (POCO_OS == POCO_OS_QNX)
#    define POCO_NET_HAS_INTERFACE
#endif


#endif // Net_Net_INCLUDED
