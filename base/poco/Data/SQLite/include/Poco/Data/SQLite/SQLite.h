//
// SQLite.h
//
// Library: Data/SQLite
// Package: SQLite
// Module:  SQLite
//
// Basic definitions for the Poco SQLite library.
// This file must be the first file included by every other SQLite
// header file.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLite_SQLite_INCLUDED
#define SQLite_SQLite_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the SQLite_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// SQLite_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(SQLite_EXPORTS)
		#define SQLite_API __declspec(dllexport)
	#else
		#define SQLite_API __declspec(dllimport)
	#endif
#endif


#if !defined(SQLite_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define SQLite_API __attribute__ ((visibility ("default")))
	#else
		#define SQLite_API
	#endif
#endif


//
// Automatically link SQLite library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(SQLite_EXPORTS)
		#pragma comment(lib, "PocoDataSQLite" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // SQLite_SQLite_INCLUDED
