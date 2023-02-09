//
// Redis.h
//
// Library: Redis
// Package: Redis
// Module:  Redis
//
// Basic definitions for the Poco Redis library.
// This file must be the first file included by every other Redis
// header file.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef RedisRedis_INCLUDED
#define RedisRedis_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Redis_EXPORTS
// symbol defined on the command line. This symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Redis_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(Redis_EXPORTS)
		#define Redis_API __declspec(dllexport)
	#else
		#define Redis_API __declspec(dllimport)
	#endif
#endif


#if !defined(Redis_API)
	#if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined (__GNUC__) && (__GNUC__ >= 4)
		#define Redis_API __attribute__ ((visibility ("default")))
	#else
		#define Redis_API
	#endif
#endif


//
// Automatically link Redis library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(Redis_EXPORTS)
		#pragma comment(lib, "PocoRedis" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // RedisRedis_INCLUDED
