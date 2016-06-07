//
// EchoServer.cpp
//
// $Id: //poco/1.4/Net/testsuite/src/EchoServer.cpp#1 $
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "EchoServer.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Timespan.h"
#include <iostream>


using Poco::Net::Socket;
using Poco::Net::StreamSocket;
using Poco::Net::SocketAddress;


EchoServer::EchoServer():
	_socket(SocketAddress()),
	_thread("EchoServer"),
	_stop(false)
{
	_thread.start(*this);
	_ready.wait();
}


EchoServer::~EchoServer()
{
	_stop = true;
	_thread.join();
}


Poco::UInt16 EchoServer::port() const
{
	return _socket.address().port();
}


void EchoServer::run()
{
	_ready.set();
	Poco::Timespan span(250000);
	while (!_stop)
	{
		if (_socket.poll(span, Socket::SELECT_READ))
		{
			StreamSocket ss = _socket.acceptConnection();
			try
			{
				char buffer[256];
				int n = ss.receiveBytes(buffer, sizeof(buffer));
				while (n > 0 && !_stop)
				{
					ss.sendBytes(buffer, n);
					n = ss.receiveBytes(buffer, sizeof(buffer));
				}
			}
			catch (Poco::Exception& exc)
			{
				std::cerr << "EchoServer: " << exc.displayText() << std::endl;
			}
		}
	}
}

