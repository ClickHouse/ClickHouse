//
// HTTPSTestServer.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/testsuite/src/HTTPSTestServer.h#1 $
//
// Definition of the HTTPSTestServer class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPSTestServer_INCLUDED
#define HTTPSTestServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SecureServerSocket.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"


class HTTPSTestServer: public Poco::Runnable
	/// A simple sequential echo server.
{
public:
	HTTPSTestServer();
		/// Creates the HTTPSTestServer.

	explicit HTTPSTestServer(Poco::Net::Context::Ptr pContext);
		/// Creates the HTTPSTestServer using the given Context.

	~HTTPSTestServer();
		/// Destroys the HTTPSTestServer.

	Poco::UInt16 port() const;
		/// Returns the port the echo server is
		/// listening on.
		
	void run();
		/// Does the work.
		
	const std::string& lastRequest() const;
		/// Returns the last request.

	static const std::string SMALL_BODY;
	static const std::string LARGE_BODY;	

protected:
	bool requestComplete() const;
	std::string handleRequest() const;

private:
	Poco::Net::SecureServerSocket _socket;
	Poco::Thread _thread;
	Poco::Event  _ready;
	bool         _stop;
	std::string  _lastRequest;
};


#endif // HTTPSTestServer_INCLUDED
