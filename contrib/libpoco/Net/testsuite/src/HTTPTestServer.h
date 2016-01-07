//
// HTTPTestServer.h
//
// $Id: //poco/1.4/Net/testsuite/src/HTTPTestServer.h#1 $
//
// Definition of the HTTPTestServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef HTTPTestServer_INCLUDED
#define HTTPTestServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"


class HTTPTestServer: public Poco::Runnable
	/// A simple sequential echo server.
{
public:
	HTTPTestServer();
		/// Creates the HTTPTestServer.

	~HTTPTestServer();
		/// Destroys the HTTPTestServer.

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
	Poco::Net::ServerSocket  _socket;
	Poco::Thread _thread;
	Poco::Event  _ready;
	bool         _stop;
	std::string  _lastRequest;
};


#endif // HTTPTestServer_INCLUDED
