//
// Error.h
//
// Library: Foundation
// Package: Core
// Module:  Error
//
// Definition of the Error class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Error_INCLUDED
#define Foundation_Error_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Error
	/// The Error class provides utility functions
	/// for error reporting.
{
public:

#ifdef POCO_OS_FAMILY_WINDOWS
	static DWORD last();
		/// Utility function returning the last error.

	static std::string getMessage(DWORD errorCode);
		/// Utility function translating numeric error code to string.
#else
	static int last();
		/// Utility function returning the last error.

	static std::string getMessage(int errorCode);
		/// Utility function translating numeric error code to string.
#endif
};


} // namespace Poco


#endif // Foundation_Error_INCLUDED
