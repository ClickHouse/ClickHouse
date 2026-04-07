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


#if !defined(Util_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define Util_API __attribute__((visibility("default")))
#    else
#        define Util_API
#    endif
#endif


//
// Automatically link Util library.
//


#endif // Util_Util_INCLUDED
