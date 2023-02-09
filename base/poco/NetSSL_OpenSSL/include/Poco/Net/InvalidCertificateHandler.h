//
// InvalidCertificateHandler.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  InvalidCertificateHandler
//
// Definition of the InvalidCertificateHandler class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_InvalidCertificateHandler_INCLUDED
#define NetSSL_InvalidCertificateHandler_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/VerificationErrorArgs.h"


namespace Poco {
namespace Net {


class NetSSL_API InvalidCertificateHandler
	/// A InvalidCertificateHandler is invoked whenever an error occurs verifying the certificate. It allows the user
	/// to inspect and accept/reject the certificate.
	/// One can install one's own InvalidCertificateHandler by implementing this interface. Note that
	/// in the implementation file of the subclass the following code must be present (assuming you use the namespace My_API 
	/// and the name of your handler class is MyGuiHandler):
	///    
	///    #include "Poco/Net/CertificateHandlerFactory.h"
	///    ...
	///    POCO_REGISTER_CHFACTORY(My_API, MyGuiHandler)
	///
	/// One can either set the handler directly in the startup code of the main method of ones application by calling
	///
	///    SSLManager::instance().initialize(mypassphraseHandler, myguiHandler, mySSLContext)
	///
	/// or in case one uses Poco::Util::Application one can rely on an XML configuration and put the following entry
	/// under the path openSSL.invalidCertificateHandler:
	///    
	///    <invalidCertificateHandler>
	///        <name>MyGuiHandler<name>
	///        <options>
	///            [...] // Put optional config params for the handler here
	///        </options>
	///    </invalidCertificateHandler>
	///
	/// Note that the name of the InvalidCertificateHandler must be same as the one provided to the POCO_REGISTER_CHFACTORY macro.
{
public:
	InvalidCertificateHandler(bool handleErrorsOnServerSide);
		/// Creates the InvalidCertificateHandler.
		/// 
		/// Set handleErrorsOnServerSide to true if the certificate handler is used on the server side.
		/// Automatically registers at one of the SSLManager::VerificationError events.

	virtual ~InvalidCertificateHandler();
		/// Destroys the InvalidCertificateHandler.

	virtual void onInvalidCertificate(const void* pSender, VerificationErrorArgs& errorCert) = 0;
		/// Receives the questionable certificate in parameter errorCert. If one wants to accept the
		/// certificate, call errorCert.setIgnoreError(true).

protected:
	bool _handleErrorsOnServerSide;
		/// Stores if the certificate handler gets invoked by the server (i.e. a client certificate is wrong)
		/// or the client (a server certificate is wrong)
};


} } // namespace Poco::Net


#endif // NetSSL_InvalidCertificateHandler_INCLUDED
