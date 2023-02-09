//
// Utility.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  Utility
//
// Definition of the Utility class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_Utility_INCLUDED
#define NetSSL_Utility_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/Context.h"


namespace Poco {
namespace Net {


class NetSSL_API Utility
	/// This class provides various helper functions for working
	/// with the OpenSSL library.
{
public:
	static Context::VerificationMode convertVerificationMode(const std::string& verMode);
		/// Non-case sensitive conversion of a string to a VerificationMode enum.
		/// If verMode is illegal an InvalidArgumentException is thrown.

	static std::string convertCertificateError(long errCode);
		/// Converts an SSL certificate handling error code into an error message.

	static std::string getLastError();
		/// Returns the last error from the error stack

	static void clearErrorStack();
		/// Clears the error stack
};


} } // namespace Poco::Net


#endif // NetSSL_Utility_INCLUDED
