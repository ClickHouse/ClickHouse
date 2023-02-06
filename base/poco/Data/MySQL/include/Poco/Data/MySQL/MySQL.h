//
// MySQL.h
//
// Library: Data/MySQL
// Package: MySQL
// Module:  MySQL
//
// Basic definitions for the MySQL library.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MySQL_MySQL_INCLUDED
#define MySQL_MySQL_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the ODBC_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// ODBC_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(MySQL_EXPORTS)
		#define MySQL_API __declspec(dllexport)
	#else
		#define MySQL_API __declspec(dllimport)
	#endif
#endif


#if !defined(MySQL_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define MySQL_API __attribute__ ((visibility ("default")))
	#else
		#define MySQL_API
	#endif
#endif


//
// Automatically link Data library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(MySQL_EXPORTS)
		#pragma comment(lib, "PocoDataMySQL" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // MySQL_MySQL_INCLUDED
