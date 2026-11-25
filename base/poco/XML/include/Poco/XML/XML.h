//
// XML.h
//
// Library: XML
// Package: XML
// Module:  XML
//
// Basic definitions for the Poco XML library.
// This file must be the first file included by every other XML
// header file.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef XML_XML_INCLUDED
#define XML_XML_INCLUDED


#include "Poco/Foundation.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the XML_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// XML_API functions as being imported from a DLL, whereas this DLL sees symbols
// defined with this macro as being exported.
//


#if !defined(XML_API)
#    if !defined(POCO_NO_GCC_API_ATTRIBUTE) && defined(__GNUC__) && (__GNUC__ >= 4)
#        define XML_API __attribute__((visibility("default")))
#    else
#        define XML_API
#    endif
#endif


//
// Automatically link XML library.
//


#endif // XML_XML_INCLUDED
