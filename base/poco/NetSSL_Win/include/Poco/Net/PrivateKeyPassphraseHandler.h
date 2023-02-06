//
// PrivateKeyPassphraseHandler.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  PrivateKeyPassphraseHandler
//
// Definition of the PrivateKeyPassphraseHandler class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_PrivateKeyPassphraseHandler_INCLUDED
#define NetSSL_PrivateKeyPassphraseHandler_INCLUDED


#include "Poco/Net/NetSSL.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API PrivateKeyPassphraseHandler
	/// A passphrase handler is needed whenever the private key of a certificate is loaded and the certificate is protected
	/// by a passphrase. The PrivateKeyPassphraseHandler's task is to provide that passphrase.
	/// One can install one's own PrivateKeyPassphraseHandler by implementing this interface. Note that
	/// in the implementation file of the subclass the following code must be present (assuming you use the namespace My_API 
	/// and the name of your handler class is MyGuiHandler):
	///    
	///    #include "Poco/Net/PrivateKeyFactory.h"
	///    ...
	///    POCO_REGISTER_KEYFACTORY(My_API, MyGuiHandler)
	///
	/// One can either set the handler directly in the startup code of the main method of ones application by calling
	///
	///    SSLManager::instance().initialize(myguiHandler, myInvalidCertificateHandler, mySSLContext)
	///
	/// or in case one's application extends Poco::Util::Application one can use an XML configuration and put the following entry
	/// under the path openSSL.privateKeyPassphraseHandler:
	///    
	///    <privateKeyPassphraseHandler>
	///        <name>MyGuiHandler</name>
	///        <options>
	///            [...] // Put optional config params for the handler here
	///        </options>
	///    </privateKeyPassphraseHandler>
	///
	/// Note that the name of the passphrase handler must be same as the one provided to the POCO_REGISTER_KEYFACTORY macro.
{
public:
	PrivateKeyPassphraseHandler(bool onServerSide);
		/// Creates the PrivateKeyPassphraseHandler. Automatically registers at the SSLManager::PrivateKeyPassword event.

	virtual ~PrivateKeyPassphraseHandler();
		/// Destroys the PrivateKeyPassphraseHandler.

	virtual void onPrivateKeyRequested(const void* pSender, std::string& privateKey) = 0;
		/// Returns the requested private key in the parameter privateKey.

	bool serverSide() const;

private:
	bool _serverSide;
};


//
// inlines
//
inline bool PrivateKeyPassphraseHandler::serverSide() const
{
	return _serverSide;
}


} } // namespace Poco::Net


#endif // NetSSL_PrivateKeyPassphraseHandler_INCLUDED
