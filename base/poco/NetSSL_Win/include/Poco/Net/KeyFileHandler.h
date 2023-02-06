//
// KeyFileHandler.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  KeyFileHandler
//
// Definition of the KeyFileHandler class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_KeyFileHandler_INCLUDED
#define NetSSL_KeyFileHandler_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/PrivateKeyPassphraseHandler.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API KeyFileHandler: public PrivateKeyPassphraseHandler
	/// An implementation of PrivateKeyPassphraseHandler that 
	/// reads the key for a certificate from a configuration file
	/// under the path "openSSL.privateKeyPassphraseHandler.options.password".
{
public:
	KeyFileHandler(bool server);
		/// Creates the KeyFileHandler.

	virtual ~KeyFileHandler();
		/// Destroys the KeyFileHandler.

	void onPrivateKeyRequested(const void* pSender, std::string& privateKey);

private:
	static const std::string CFG_PRIV_KEY_FILE;
};


} } // namespace Poco::Net


#endif // NetSSL_KeyFileHandler_INCLUDED
