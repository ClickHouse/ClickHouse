//
// PDF.h
//
// Library: PDF
// Package: PDFCore
// Module:  PDF
//
// Basic definitions for the Poco PDF library.
// This file must be the first file included by every other PDF
// header file.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef PDF_PDF_INCLUDED
#define PDF_PDF_INCLUDED

#if defined(_MSC_VER) && !defined(POCO_MSVC_SECURE_WARNINGS) && (!defined(_CRT_SECURE_NO_WARNINGS) || !defined(_CRT_SECURE_NO_DEPRECATE))
	#ifndef _CRT_SECURE_NO_WARNINGS
		#define _CRT_SECURE_NO_WARNINGS
	#endif
	#ifndef _CRT_SECURE_NO_DEPRECATE
		#define _CRT_SECURE_NO_DEPRECATE
	#endif
#endif

#include "Poco/Foundation.h"
#include "hpdf.h"


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the PDF_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// PDF_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(PDF_EXPORTS)
		#define PDF_API __declspec(dllexport)
	#else
		#define PDF_API __declspec(dllimport)
	#endif
#endif


#if !defined(PDF_API)
	#if defined (__GNUC__) && (__GNUC__ >= 4)
		#define PDF_API __attribute__ ((visibility ("default")))
	#else
		#define PDF_API
	#endif
#endif


//
// Automatically link PDF library.
//
#if defined(_MSC_VER)
	#if !defined(POCO_NO_AUTOMATIC_LIBS) && !defined(PDF_EXPORTS)
		#pragma comment(lib, "PocoPDF" POCO_LIB_SUFFIX)
	#endif
#endif


#endif // PDF_PDF_INCLUDED
