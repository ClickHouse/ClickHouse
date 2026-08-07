//
// RejectCertificateHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  RejectCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/RejectCertificateHandler.h"


namespace Poco {
namespace Net {


RejectCertificateHandler::RejectCertificateHandler(bool server): InvalidCertificateHandler(server)
{
}


RejectCertificateHandler::~RejectCertificateHandler()
{
}


void RejectCertificateHandler::onInvalidCertificate(const void*, VerificationErrorArgs& errorCert)
{
	errorCert.setIgnoreError(false);
}


} } // namespace Poco::Net
