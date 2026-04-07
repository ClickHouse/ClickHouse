//
// Foundation.h
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


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the Foundation_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// Foundation_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(Foundation_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define Foundation_API __attribute__((visibility("default")))
#    else
#        define Foundation_API
#    endif
#endif


//
// Automatically link Foundation library.
//


//
// Include platform-specific definitions
//
#include "Poco/Platform.h"
#if   defined(POCO_OS_FAMILY_UNIX)
#    include "Poco/Platform_POSIX.h"
#endif


//
// Include alignment settings early
//
#include "Poco/Alignment.h"

//
// Cleanup inconsistencies
//


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
// POCO_DEPRECATED
//
// A macro expanding to a compiler-specific clause to
// mark a class or function as deprecated.
//
#if defined(POCO_NO_DEPRECATED)
#    define POCO_DEPRECATED
#elif defined(_GNUC_)
#    define POCO_DEPRECATED __attribute__((deprecated))
#else
#    define POCO_DEPRECATED __attribute__((deprecated))
#endif


//
// Pull in basic definitions
//
#include <string>
#include "Poco/Bugcheck.h"
#include "Poco/Types.h"


#endif // Foundation_Foundation_INCLUDED
