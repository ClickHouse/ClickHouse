//
// NetSSL.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  OpenSSL
//
// Basic definitions for the Poco OpenSSL library.
// This file must be the first file included by every other OpenSSL
// header file.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_NetSSL_INCLUDED
#define NetSSL_NetSSL_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Crypto/Crypto.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the NetSSL_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// NetSSL_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(POCO_DLL)
	#if defined(NetSSL_EXPORTS)
		#define NetSSL_API __declspec(dllexport)
	#else
		#define NetSSL_API __declspec(dllimport)
	#endif
#endif


#if !defined(NetSSL_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define NetSSL_API __attribute__ ((visibility ("default")))
	#else
		#define NetSSL_API
	#endif
#endif


//
// Automatically link NetSSL and OpenSSL libraries.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS)
		#if !defined(NetSSL_EXPORTS)
			#pragma comment(lib, "PocoNetSSL" POCO_LIB_SUFFIX)
		#endif
	#endif // POCO_NO_AUTOMATIC_LIBS
#endif


namespace Poco {
namespace Net {


void NetSSL_API initializeSSL();
	/// Initialize the NetSSL library, as well as the underlying OpenSSL
	/// libraries, by calling Poco::Crypto::OpenSSLInitializer::initialize().
	///
	/// Should be called before using any class from the NetSSL library.
	/// The NetSSL will be initialized automatically, through 
	/// Poco::Crypto::OpenSSLInitializer instances or similar mechanisms
	/// when creating Context or SSLManager instances.
	/// However, it is recommended to call initializeSSL()
	/// in any case at application startup.
	///
	/// Can be called multiple times; however, for every call to
	/// initializeSSL(), a matching call to uninitializeSSL()
	/// must be performed.
	

void NetSSL_API uninitializeSSL();
	/// Uninitializes the NetSSL library by calling 
	/// Poco::Crypto::OpenSSLInitializer::uninitialize() and
	/// shutting down the SSLManager.


} } // namespace Poco::Net


#endif // NetSSL_NetSSL_INCLUDED
