//
// Types.h
//
// Library: Foundation
// Package: Core
// Module:  Types
//
// Definitions of fixed-size integer types for various platforms
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Types_INCLUDED
#define Foundation_Types_INCLUDED


#include "Poco/Foundation.h"


namespace Poco
{


//
// Unix/GCC/Clang
//
typedef signed char Int8;
typedef unsigned char UInt8;
typedef signed short Int16;
typedef unsigned short UInt16;
typedef signed int Int32;
typedef unsigned int UInt32;
typedef signed long IntPtr;
typedef unsigned long UIntPtr;
#        if defined(__LP64__)
#            define POCO_PTR_IS_64_BIT 1
#            define POCO_LONG_IS_64_BIT 1
typedef signed long Int64;
typedef unsigned long UInt64;
#        else
typedef signed long long Int64;
typedef unsigned long long UInt64;
#        endif


} // namespace Poco


#endif // Foundation_Types_INCLUDED
