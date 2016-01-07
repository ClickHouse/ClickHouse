//
// MulticastEchoServer.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/MulticastEchoServer.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "MulticastEchoServer.h"


#ifdef POCO_NET_HAS_INTERFACE


#include "Poco/Timespan.h"
#include <iostream>


using Poco::Net::Socket;
using Poco::Net::DatagramSocket;
using Poco::Net::SocketAddress;
using Poco::Net::IPAddress;
using Poco::Net::NetworkInterface;


MulticastEchoServer::MulticastEchoServer():
	_group("239.255.1.2", 12345),
	_if(findInterface()),
	_thread("MulticastEchoServer"),
	_stop(false)
{
	_socket.bind(SocketAddress(IPAddress(), _group.port()), true);
	_socket.joinGroup(_group.host(), _if);
	_thread.start(*this);
	_ready.wait();
}


MulticastEchoServer::~MulticastEchoServer()
{
	_stop = true;
	_thread.join();
	_socket.leaveGroup(_group.host(), _if);
}


Poco::UInt16 MulticastEchoServer::port() const
{
	return _socket.address().port();
}


void MulticastEchoServer::run()
{
	_ready.set();
	Poco::Timespan span(250000);
	while (!_stop)
	{
		if (_socket.poll(span, Socket::SELECT_READ))
		{
			try
			{
				char buffer[256];
				SocketAddress sender;
				int n = _socket.receiveFrom(buffer, sizeof(buffer), sender);
				_socket.sendTo(buffer, n, sender);
			}
			catch (Poco::Exception& exc)
			{
				std::cerr << "MulticastEchoServer: " << exc.displayText() << std::endl;
			}
		}
	}
}


const SocketAddress& MulticastEchoServer::group() const
{
	return _group;
}


const NetworkInterface& MulticastEchoServer::interfc() const
{
	return _if;
}


Poco::Net::NetworkInterface MulticastEchoServer::findInterface()
{
	NetworkInterface::Map m = NetworkInterface::map();
	for (NetworkInterface::Map::const_iterator it = m.begin(); it != m.end(); ++it)
	{
		if (it->second.supportsIPv4() && 
			it->second.firstAddress(IPAddress::IPv4).isUnicast() && 
			!it->second.isLoopback() && 
			!it->second.isPointToPoint())
		{
			return it->second;
		}
	}
	return NetworkInterface();
}

#endif // POCO_NET_HAS_INTERFACE
