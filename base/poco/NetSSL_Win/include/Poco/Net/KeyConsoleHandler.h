//
// KeyConsoleHandler.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  KeyConsoleHandler
//
// Definition of the KeyConsoleHandler class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_KeyConsoleHandler_INCLUDED
#define NetSSL_KeyConsoleHandler_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/PrivateKeyPassphraseHandler.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API KeyConsoleHandler: public PrivateKeyPassphraseHandler
	/// An implementation of PrivateKeyPassphraseHandler that
	/// reads the key for a certificate from the console.
{
public:
	KeyConsoleHandler(bool server);
		/// Creates the KeyConsoleHandler.

	~KeyConsoleHandler();
		/// Destroys the KeyConsoleHandler.

	void onPrivateKeyRequested(const void* pSender, std::string& privateKey);
};


} } // namespace Poco::Net


#endif // NetSSL_KeyConsoleHandler_INCLUDED
