//
// CppUnit.h
//
// $Id: //poco/1.4/CppUnit/include/CppUnit/CppUnit.h#1 $
//


#ifndef CppUnit_CppUnit_INCLUDED
#define CppUnit_CppUnit_INCLUDED


//
// Ensure that POCO_DLL is default unless POCO_STATIC is defined
//
#if defined(_WIN32) && defined(_DLL)
	#if !defined(POCO_DLL) && !defined(POCO_STATIC)
		#define POCO_DLL
	#endif
#endif


//
// The following block is the standard way of creating macros which make exporting
// from a DLL simpler. All files within this DLL are compiled with the CppUnit_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see
// CppUnit_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
//
#if defined(_WIN32) && defined(POCO_DLL)
	#if defined(CppUnit_EXPORTS)
		#define CppUnit_API __declspec(dllexport)
	#else
		#define CppUnit_API __declspec(dllimport)
	#endif
#endif


#if !defined(CppUnit_API)
	#if defined (__GNUC__) && (__GNUC__ >= 4)
		#define CppUnit_API __attribute__ ((visibility ("default")))
	#else
		#define CppUnit_API
	#endif
#endif


// Turn off some annoying warnings
#ifdef _MSC_VER
	#pragma warning(disable:4786)  // identifier truncation warning
	#pragma warning(disable:4503)  // decorated name length exceeded - mainly a problem with STLPort
	#pragma warning(disable:4018)  // signed/unsigned comparison
	#pragma warning(disable:4284)  // return type for operator -> is not UDT
	#pragma warning(disable:4251)  // ... needs to have dll-interface warning 
	#pragma warning(disable:4273) 
	#pragma warning(disable:4275)  // ... non dll-interface class used as base for dll-interface class
#endif


#endif // CppUnit_CppUnit_INCLUDED
