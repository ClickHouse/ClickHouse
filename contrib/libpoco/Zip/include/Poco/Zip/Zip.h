//
// Zip.h
//
// $Id: //poco/1.4/Zip/include/Poco/Zip/Zip.h#1 $
//
// Library: Zip
// Package: Zip
// Module:  Zip
//
// Basic definitions for the Poco Zip library.
// This file must be the first file included by every other Zip
// header file.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Zip_Zip_INCLUDED
#define Zip_Zip_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Zip_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Zip_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(Zip_EXPORTS)
		#define Zip_API __declspec(dllexport)
	#else
		#define Zip_API __declspec(dllimport)
	#endif
#endif


#if !defined(Zip_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Zip_API __attribute__ ((visibility ("default")))
	#else
		#define Zip_API
	#endif
#endif


//
// Automatically link Zip library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Zip_EXPORTS)
		#pragma comment(lib, "PocoZip" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // Zip_Zip_INCLUDED
