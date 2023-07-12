//
// ConsoleCertificateHandler.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  ConsoleCertificateHandler
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ConsoleCertificateHandler.h"
#include <iostream>


namespace Poco {
namespace Net {


ConsoleCertificateHandler::ConsoleCertificateHandler(bool server): InvalidCertificateHandler(server)
{
}


ConsoleCertificateHandler::~ConsoleCertificateHandler()
{
}


void ConsoleCertificateHandler::onInvalidCertificate(const void*, VerificationErrorArgs& errorCert)
{
	const X509Certificate& aCert = errorCert.certificate();
	std::cout << "\n";
	std::cout << "WARNING: Certificate verification failed\n";
	std::cout << "----------------------------------------\n";
	std::cout << "Issuer Name:  " << aCert.issuerName() << "\n";
	std::cout << "Subject Name: " << aCert.subjectName() << "\n\n";
	std::cout << "The certificate yielded the error: " << errorCert.errorMessage() << "\n\n";
	std::cout << "The error occurred in the certificate chain at position " << errorCert.errorDepth() << "\n";
	std::cout << "Accept the certificate (y,n)? ";
	char c = 0;
	std::cin >> c;
	if (c == 'y' || c == 'Y')
		errorCert.setIgnoreError(true);
	else
		errorCert.setIgnoreError(false);
}


} } // namespace Poco::Net
