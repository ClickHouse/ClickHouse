//
// NetSSL.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  SSLCore
//
// Basic definitions for the Poco NetSSL library.
// This file must be the first file included by every other NetSSL
// header file.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_NetSSL_INCLUDED
#define NetSSL_NetSSL_INCLUDED


#include "Poco/Net/Net.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the NetSSL_Win_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// NetSSL_Win_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(POCO_DLL)
	#if defined(NetSSL_Win_EXPORTS)
		#define NetSSL_Win_API __declspec(dllexport)
	#else
		#define NetSSL_Win_API __declspec(dllimport)
	#endif
#endif


#if !defined(NetSSL_Win_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define NetSSL_Win_API __attribute__ ((visibility ("default")))
	#else
		#define NetSSL_Win_API
	#endif
#endif


//
// Allow detection of NetSSL_Win at compile time
//
#define POCO_NETSSL_WIN 1


//
// Automatically link NetSSL library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(NetSSL_Win_EXPORTS)
		#pragma comment(lib, "PocoNetSSLWin" POCO_LIB_SUFFIX)
	#endif
#endif


namespace Poco {
namespace Net {


void NetSSL_Win_API initializeSSL();
	/// Initialize the NetSSL library, as well as the underlying Windows
	/// libraries.
	///
	/// Can be called multiple times; however, for every call to
	/// initializeSSL(), a matching call to uninitializeSSL()
	/// must be performed.
	

void NetSSL_Win_API uninitializeSSL();
	/// Uninitializes the NetSSL library and
	/// shutting down the SSLManager.


} } // namespace Poco::Net


#endif // NetSSL_NetSSL_INCLUDED
