//
// JSON.h
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  JSON
//
// Basic definitions for the Poco JSON library.
// This file must be the first file included by every other JSON
// header file.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSON_INCLUDED
#define JSON_JSON_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the JSON_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// JSON_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(JSON_EXPORTS)
		#define JSON_API __declspec(dllexport)
	#else
		#define JSON_API __declspec(dllimport)
	#endif
#endif


#if !defined(JSON_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define JSON_API __attribute__ ((visibility ("default")))
	#else
		#define JSON_API
	#endif
#endif


//
// Automatically link JSON library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(JSON_EXPORTS)
		#pragma comment(lib, "PocoJSON" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // JSON_JSON_INCLUDED
