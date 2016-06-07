//
// UDPEchoServer.h
//
// $Id: //poco/1.4/Net/testsuite/src/UDPEchoServer.h#1 $
//
// Definition of the UDPEchoServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef UDPEchoServer_INCLUDED
#define UDPEchoServer_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"


class UDPEchoServer: public Poco::Runnable
	/// A simple sequential UDP echo server.
{
public:
	UDPEchoServer();
		/// Creates the UDPEchoServer.

	UDPEchoServer(const Poco::Net::SocketAddress& sa);
		/// Creates the UDPEchoServer and binds it to
		/// the given address.

	~UDPEchoServer();
		/// Destroys the UDPEchoServer.

	Poco::UInt16 port() const;
		/// Returns the port the echo server is
		/// listening on.
		
	Poco::Net::SocketAddress address() const;
		/// Returns the address of the server.	
		
	void run();
		/// Does the work.

private:
	Poco::Net::DatagramSocket _socket;
	Poco::Thread _thread;
	Poco::Event  _ready;
	bool         _stop;
};


#endif // UDPEchoServer_INCLUDED
