//
// Util.h
//
// Library: Util
// Package: Util
// Module:  Util
//
// Basic definitions for the Poco Util library.
// This file must be the first file included by every other Util
// header file.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_Util_INCLUDED
#define Util_Util_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Util_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Util_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(Util_EXPORTS)
		#define Util_API __declspec(dllexport)
	#else
		#define Util_API __declspec(dllimport)
	#endif
#endif


#if !defined(Util_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Util_API __attribute__ ((visibility ("default")))
	#else
		#define Util_API
	#endif
#endif


//
// Automatically link Util library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Util_EXPORTS)
		#pragma comment(lib, "PocoUtil" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // Util_Util_INCLUDED
