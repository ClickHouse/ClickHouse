//
// Encodings.h
//
// Library: Encodings
// Package: Encodings
// Module:  Encodings
//
// Basic definitions for the Poco Encodings library.
// This file must be the first file included by every other Encodings
// header file.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Encodings_Encodings_INCLUDED
#define Encodings_Encodings_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Encodings_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Encodings_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(Encodings_EXPORTS)
		#define Encodings_API __declspec(dllexport)
	#else
		#define Encodings_API __declspec(dllimport)
	#endif
#endif


#if !defined(Encodings_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Encodings_API __attribute__ ((visibility ("default")))
	#else
		#define Encodings_API
	#endif
#endif


//
// Automatically link Encodings library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Encodings_EXPORTS)
		#pragma comment(lib, "PocoEncodings" POCO_LIB_SUFFIX)
	#endif
#endif


namespace Poco {


void Encodings_API registerExtraEncodings();
	/// Registers the character encodings from the Encodings library
	/// with the TextEncoding class.


} // namespace Poco


#endif // Encodings_Encodings_INCLUDED
