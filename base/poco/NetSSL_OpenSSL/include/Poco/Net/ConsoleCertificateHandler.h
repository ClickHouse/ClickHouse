//
// ConsoleCertificateHandler.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  ConsoleCertificateHandler
//
// Definition of the ConsoleCertificateHandler class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_ConsoleCertificateHandler_INCLUDED
#define NetSSL_ConsoleCertificateHandler_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/InvalidCertificateHandler.h"


namespace Poco {
namespace Net {


class NetSSL_API ConsoleCertificateHandler: public InvalidCertificateHandler
	/// A ConsoleCertificateHandler is invoked whenever an error occurs verifying the certificate.
	/// 
	/// The certificate is printed to stdout and the user is asked via console if he wants to accept it.
{
public:
	ConsoleCertificateHandler(bool handleErrorsOnServerSide);
		/// Creates the ConsoleCertificateHandler.

	virtual ~ConsoleCertificateHandler();
		/// Destroys the ConsoleCertificateHandler.

	void onInvalidCertificate(const void* pSender, VerificationErrorArgs& errorCert);
		/// Prints the certificate to stdout and waits for user input on the console
		/// to decide if a certificate should be accepted/rejected.
};


} } // namespace Poco::Net


#endif // NetSSL_ConsoleCertificateHandler_INCLUDED
