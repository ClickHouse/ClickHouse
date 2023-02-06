//
// CppParser.h
//
// Library: CppParser
// Package: CppParser
// Module:  CppParser
//
// Basic definitions for the Poco CppParser library.
// This file must be the first file included by every other CppParser
// header file.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CppParser_CppParser_INCLUDED
#define CppParser_CppParser_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the CppParser_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// CppParser_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(POCO_DLL)
	#if defined(CppParser_EXPORTS)
		#define CppParser_API __declspec(dllexport)
	#else
		#define CppParser_API __declspec(dllimport)
	#endif
#endif


#if !defined(CppParser_API)
	#define CppParser_API
#endif


//
// Automatically link CppParser library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(CppParser_EXPORTS)
		#pragma comment(lib, "PocoCppParser" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // CppParser_CppParser_INCLUDED
