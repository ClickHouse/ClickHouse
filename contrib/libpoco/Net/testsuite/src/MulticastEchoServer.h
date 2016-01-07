//
// MulticastEchoServer.h
//
// $Id: //poco/1.4/Net/testsuite/src/MulticastEchoServer.h#1 $
//
// Definition of the MulticastEchoServer class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MulticastEchoServer_INCLUDED
#define MulticastEchoServer_INCLUDED


#include "Poco/Net/Net.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Net/MulticastSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/NetworkInterface.h"
#include "Poco/Thread.h"
#include "Poco/Event.h"


class MulticastEchoServer: public Poco::Runnable
	/// A simple sequential Multicast echo server.
{
public:
	MulticastEchoServer();
		/// Creates the MulticastEchoServer.

	~MulticastEchoServer();
		/// Destroys the MulticastEchoServer.

	Poco::UInt16 port() const;
		/// Returns the port the echo server is
		/// listening on.
		
	void run();
		/// Does the work.
		
	const Poco::Net::SocketAddress& group() const;
		/// Returns the group address where the server listens.
	
	const Poco::Net::NetworkInterface& interfc() const;
		/// Returns the network interface for multicasting.
	
protected:	
	static Poco::Net::NetworkInterface findInterface();
		/// Finds an appropriate network interface for
		/// multicasting.
	
private:
	Poco::Net::MulticastSocket  _socket;
	Poco::Net::SocketAddress    _group;
	Poco::Net::NetworkInterface _if;
	Poco::Thread _thread;
	Poco::Event  _ready;
	bool         _stop;
};


#endif // POCO_NET_HAS_INTERFACE


#endif // MulticastEchoServer_INCLUDED
