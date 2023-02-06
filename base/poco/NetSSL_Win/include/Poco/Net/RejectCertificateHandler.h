//
// RejectCertificateHandler.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  RejectCertificateHandler
//
// Definition of the RejectCertificateHandler class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_RejectCertificateHandler_INCLUDED
#define NetSSL_RejectCertificateHandler_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/InvalidCertificateHandler.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API RejectCertificateHandler: public InvalidCertificateHandler
	/// A RejectCertificateHandler is invoked whenever an error 
	/// occurs verifying the certificate. It always rejects
	/// the certificate.
{
public:
	RejectCertificateHandler(bool handleErrorsOnServerSide);
		/// Creates the RejectCertificateHandler

	virtual ~RejectCertificateHandler();
		/// Destroys the RejectCertificateHandler.

	void onInvalidCertificate(const void* pSender, VerificationErrorArgs& errorCert);
};


} } // namespace Poco::Net


#endif // NetSSL_RejectCertificateHandler_INCLUDED
