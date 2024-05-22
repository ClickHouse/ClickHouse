//
// Utility.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  Utility
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Utility.h"
#include "Poco/String.h"
#include "Poco/Util/OptionException.h"
#include <openssl/err.h>


namespace Poco {
namespace Net {


Context::VerificationMode Utility::convertVerificationMode(const std::string& vMode)
{
	std::string mode = Poco::toLower(vMode);
	Context::VerificationMode verMode = Context::VERIFY_STRICT;

	if (mode == "none")
		verMode = Context::VERIFY_NONE;
	else if (mode == "relaxed")
		verMode = Context::VERIFY_RELAXED;
	else if (mode == "strict")
		verMode = Context::VERIFY_STRICT;
	else if (mode == "once")
		verMode = Context::VERIFY_ONCE;
	else
		throw Poco::InvalidArgumentException("Invalid verification mode. Should be relaxed, strict or once but got", vMode);

	return verMode;
}


std::string Utility::convertCertificateError(long errCode)
{
	std::string errMsg(X509_verify_cert_error_string(errCode));
	return errMsg;
}


std::string Utility::getLastError()
{
	unsigned long errCode = ERR_get_error();
	if (errCode != 0)
	{
		char buffer[256];
		ERR_error_string_n(errCode, buffer, sizeof(buffer));
		return std::string(buffer);
	}
	else return "No error";
}


void Utility::clearErrorStack()
{
	ERR_clear_error();
}


} } // namespace Poco::Net
