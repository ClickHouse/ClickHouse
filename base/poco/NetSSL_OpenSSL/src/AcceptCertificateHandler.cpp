//
// AcceptCertificateHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  AcceptCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/AcceptCertificateHandler.h"


namespace Poco {
namespace Net {


AcceptCertificateHandler::AcceptCertificateHandler(bool server): InvalidCertificateHandler(server)
{
}


AcceptCertificateHandler::~AcceptCertificateHandler()
{
}


void AcceptCertificateHandler::onInvalidCertificate(const void*, VerificationErrorArgs& errorCert)
{
	errorCert.setIgnoreError(true);
}


} } // namespace Poco::Net
