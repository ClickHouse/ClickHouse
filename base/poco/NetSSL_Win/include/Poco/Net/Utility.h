//
// Utility.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  Utility
//
// Definition of the Utility class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_Utility_INCLUDED
#define NetSSL_Utility_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/Context.h"
#include <map>


namespace Poco {
namespace Net {


class NetSSL_Win_API Utility
	/// Various helper functions.
{
public:
	static Context::VerificationMode convertVerificationMode(const std::string& verMode);
		/// Non-case sensitive conversion of a string to a VerificationMode enum.
		/// If verMode is illegal an OptionException is thrown.

	static const std::string& formatError(long errCode);
		/// Converts an winerror.h code into human readable form.

private:
	static std::map<long, const std::string> initSSPIErr();
	static Poco::FastMutex _mutex;
};


} } // namespace Poco::Net


#endif // NetSSL_Utility_INCLUDED
