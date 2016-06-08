//
// Foundation.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Foundation.h#2 $
//
// Library: Foundation
// Package: Core
// Module:  Foundation
//
// Basic definitions for the POCO Foundation library.
// This file must be the first file included by every other Foundation
// header file.
//
// Copyright (c) 2004-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Foundation_INCLUDED
#define Foundation_Foundation_INCLUDED


//
// Include library configuration
//
#include "Poco/Config.h"


//
// Ensure that POCO_DLL is default unless POCO_STATIC is defined
//
#if defined(_WIN32) && defined(_DLL)
	#if !defined(POCO_DLL) && !defined(POCO_STATIC)
		#define POCO_DLL
	#endif
#endif


//
// The following block is the standard way of creating macros which make exporting 
// from a DLL simpler. All files within this DLL are compiled with the Foundation_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see 
// Foundation_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if (defined(_WIN32) || defined(_WIN32_WCE)) && defined(POCO_DLL)
	#if defined(Foundation_EXPORTS)
		#define Foundation_API __declspec(dllexport)
	#else
		#define Foundation_API __declspec(dllimport)	
	#endif
#endif


#if !defined(Foundation_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Foundation_API __attribute__ ((visibility ("default")))
	#else
		#define Foundation_API
	#endif
#endif


//
// Automatically link Foundation library.
//
#if defined(_MSC_VER)
	#if defined(POCO_DLL)
		#if defined(_DEBUG)
			#define POCO_LIB_SUFFIX "d.lib"
		#else
			#define POCO_LIB_SUFFIX ".lib"
		#endif
	#elif defined(_DLL)
		#if defined(_DEBUG)
			#define POCO_LIB_SUFFIX "mdd.lib"
		#else
			#define POCO_LIB_SUFFIX "md.lib"
		#endif
	#else
		#if defined(_DEBUG)
			#define POCO_LIB_SUFFIX "mtd.lib"
		#else
			#define POCO_LIB_SUFFIX "mt.lib"
		#endif
	#endif

	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Foundation_EXPORTS)
		#pragma comment(lib, "PocoFoundation" POCO_LIB_SUFFIX)
	#endif
#endif


//
// Include platform-specific definitions
//
#include "Poco/Platform.h"
#if defined(_WIN32)
	#include "Poco/Platform_WIN32.h"
#elif defined(__VMS)
	#include "Poco/Platform_VMS.h"
#elif defined(POCO_VXWORKS)
	#include "Poco/Platform_VX.h"
#elif defined(POCO_OS_FAMILY_UNIX)
	#include "Poco/Platform_POSIX.h"
#endif


//
// Include alignment settings early
//
#include "Poco/Alignment.h"

//
// Cleanup inconsistencies
//
#ifdef POCO_OS_FAMILY_WINDOWS
	#if defined(POCO_WIN32_UTF8) && defined(POCO_NO_WSTRING)
		#error POCO_WIN32_UTF8 and POCO_NO_WSTRING are mutually exclusive.
	#endif
#else
	#ifdef POCO_WIN32_UTF8
		#undef POCO_WIN32_UTF8
	#endif
#endif


//
// POCO_JOIN
//
// The following piece of macro magic joins the two
// arguments together, even when one of the arguments is
// itself a macro (see 16.3.1 in C++ standard).  The key
// is that macro expansion of macro arguments does not
// occur in POCO_DO_JOIN2 but does in POCO_DO_JOIN.
//
#define POCO_JOIN(X, Y) POCO_DO_JOIN(X, Y)
#define POCO_DO_JOIN(X, Y) POCO_DO_JOIN2(X, Y)
#define POCO_DO_JOIN2(X, Y) X##Y


//
// Pull in basic definitions
//
#include "Poco/Bugcheck.h"
#include "Poco/Types.h"
#include <string>


#endif // Foundation_Foundation_INCLUDED
