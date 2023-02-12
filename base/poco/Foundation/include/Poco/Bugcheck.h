//
// Bugcheck.h
//
// Library: Foundation
// Package: Core
// Module:  Bugcheck
//
// Definition of the Bugcheck class and the self-testing macros.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Bugcheck_INCLUDED
#define Foundation_Bugcheck_INCLUDED


#include "Poco/Foundation.h"
#include <string>
#include <cstdlib>
#if defined(_DEBUG)
#	include <iostream>
#endif


namespace Poco {


class Foundation_API Bugcheck
	/// This class provides some static methods that are
	/// used by the
	/// poco_assert_dbg(), poco_assert(), poco_check_ptr(), 
	/// poco_bugcheck() and poco_unexpected() macros. 
	/// You should not invoke these methods
	/// directly. Use the macros instead, as they
	/// automatically provide useful context information.
{
public:
	static void assertion(const char* cond, const char* file, int line, const char* text = 0);
		/// An assertion failed. Break into the debugger, if
		/// possible, then throw an AssertionViolationException.

	static void nullPointer(const char* ptr, const char* file, int line);
		/// An null pointer was encountered. Break into the debugger, if
		/// possible, then throw an NullPointerException.

	static void bugcheck(const char* file, int line);
		/// An internal error was encountered. Break into the debugger, if
		/// possible, then throw an BugcheckException.

	static void bugcheck(const char* msg, const char* file, int line);
		/// An internal error was encountered. Break into the debugger, if
		/// possible, then throw an BugcheckException.

	static void unexpected(const char* file, int line);
		/// An exception was caught in a destructor. Break into debugger,
		/// if possible and report exception. Must only be called from
		/// within a catch () block as it rethrows the exception to
		/// determine its class.

	static void debugger(const char* file, int line);
		/// An internal error was encountered. Break into the debugger, if
		/// possible.

	static void debugger(const char* msg, const char* file, int line);
		/// An internal error was encountered. Break into the debugger, if
		/// possible.

protected:
	static std::string what(const char* msg, const char* file, int line, const char* text = 0);
};


} // namespace Poco


//
// useful macros (these automatically supply line number and file name)
//
#if defined(__KLOCWORK__) || defined(__clang_analyzer__)


// Short-circuit these macros when under static analysis.
// Ideally, static analysis tools should understand and reason correctly about
// noreturn methods such as Bugcheck::bugcheck(). In practice, they don't.
// Help them by turning these macros into std::abort() as described here:
// https://developer.klocwork.com/documentation/en/insight/10-1/tuning-cc-analysis#Usingthe__KLOCWORK__macro

#include <cstdlib> // for abort
#define poco_assert_dbg(cond)           do { if (!(cond)) std::abort(); } while (0)
#define poco_assert_msg_dbg(cond, text) do { if (!(cond)) std::abort(); } while (0)
#define poco_assert(cond)               do { if (!(cond)) std::abort(); } while (0)
#define poco_assert_msg(cond, text)     do { if (!(cond)) std::abort(); } while (0)
#define poco_check_ptr(ptr)             do { if (!(ptr)) std::abort(); } while (0)
#define poco_bugcheck()                 do { std::abort(); } while (0)
#define poco_bugcheck_msg(msg)          do { std::abort(); } while (0)


#else // defined(__KLOCWORK__) || defined(__clang_analyzer__)


#if defined(_DEBUG)
	#define poco_assert_dbg(cond) \
		if (!(cond)) Poco::Bugcheck::assertion(#cond, __FILE__, __LINE__); else (void) 0

	#define poco_assert_msg_dbg(cond, text) \
		if (!(cond)) Poco::Bugcheck::assertion(#cond, __FILE__, __LINE__, text); else (void) 0
#else
	#define poco_assert_msg_dbg(cond, text)
	#define poco_assert_dbg(cond)
#endif


#define poco_assert(cond) \
	if (!(cond)) Poco::Bugcheck::assertion(#cond, __FILE__, __LINE__); else (void) 0


#define poco_assert_msg(cond, text) \
	if (!(cond)) Poco::Bugcheck::assertion(#cond, __FILE__, __LINE__, text); else (void) 0


#define poco_check_ptr(ptr) \
	if (!(ptr)) Poco::Bugcheck::nullPointer(#ptr, __FILE__, __LINE__); else (void) 0


#define poco_bugcheck() \
	Poco::Bugcheck::bugcheck(__FILE__, __LINE__)


#define poco_bugcheck_msg(msg) \
	Poco::Bugcheck::bugcheck(msg, __FILE__, __LINE__)


#endif // defined(__KLOCWORK__) || defined(__clang_analyzer__)


#define poco_unexpected() \
	Poco::Bugcheck::unexpected(__FILE__, __LINE__);


#define poco_debugger() \
	Poco::Bugcheck::debugger(__FILE__, __LINE__)


#define poco_debugger_msg(msg) \
	Poco::Bugcheck::debugger(msg, __FILE__, __LINE__)


#if defined(_DEBUG)
#	define poco_stdout_dbg(outstr) \
	std::cout << __FILE__ << '(' << std::dec << __LINE__ << "):" << outstr << std::endl;
#else
#	define poco_stdout_dbg(outstr)
#endif


#if defined(_DEBUG)
#	define poco_stderr_dbg(outstr) \
		std::cerr << __FILE__ << '(' << std::dec << __LINE__ << "):" << outstr << std::endl;
#else
#	define poco_stderr_dbg(outstr)
#endif


//
// poco_static_assert
//
// The following was ported from <boost/static_assert.hpp>
//


template <bool x>
struct POCO_STATIC_ASSERTION_FAILURE;


template <>
struct POCO_STATIC_ASSERTION_FAILURE<true>
{
	enum
	{
		value = 1
	};
};


template <int x> 
struct poco_static_assert_test
{
};


#if defined(__GNUC__) && (__GNUC__ == 3) && ((__GNUC_MINOR__ == 3) || (__GNUC_MINOR__ == 4))
#define poco_static_assert(B) \
	typedef char POCO_JOIN(poco_static_assert_typedef_, __LINE__) \
        [POCO_STATIC_ASSERTION_FAILURE<(bool) (B)>::value]
#else
#define poco_static_assert(B) \
	typedef poco_static_assert_test<sizeof(POCO_STATIC_ASSERTION_FAILURE<(bool) (B)>)> \
		POCO_JOIN(poco_static_assert_typedef_, __LINE__) POCO_UNUSED
#endif


#endif // Foundation_Bugcheck_INCLUDED
