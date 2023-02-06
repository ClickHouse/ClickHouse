//
// VerificationErrorArgs.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  VerificationErrorArgs
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/VerificationErrorArgs.h"


namespace Poco {
namespace Net {


VerificationErrorArgs::VerificationErrorArgs(const X509Certificate& cert, int errDepth, int errNum, const std::string& errMsg):
	_cert(cert),
	_errorDepth(errDepth),
	_errorNumber(errNum),
	_errorMessage(errMsg),
	_ignoreError(false)
{
}


VerificationErrorArgs::~VerificationErrorArgs()
{
}


} } // namespace Poco::Net
