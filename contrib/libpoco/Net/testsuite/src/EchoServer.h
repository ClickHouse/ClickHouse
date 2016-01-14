//
// EchoServer.h
//
// $Id: //poco/1.4/Net/testsuite/src/EchoServer.h#1 $
//
// Definition of the EchoServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef EchoServer_INCLUDED
#define EchoServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"


class EchoServer: public Poco::Runnable
	/// A simple sequential echo server.
{
public:
	EchoServer();
		/// Creates the EchoServer.

	~EchoServer();
		/// Destroys the EchoServer.

	Poco::UInt16 port() const;
		/// Returns the port the echo server is
		/// listening on.
		
	void run();
		/// Does the work.
		
private:
	Poco::Net::ServerSocket _socket;
	Poco::Thread _thread;
	Poco::Event  _ready;
	bool         _stop;
};


#endif // EchoServer_INCLUDED
