//
// Connection.cpp
//
// $Id$
//
// Library: MongoDB
// Package: MongoDB
// Module:  Connection
//
// Implementation of the Connection class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SocketStream.h"
#include "Poco/MongoDB/Connection.h"
#include <iostream>


namespace Poco {
namespace MongoDB {


Connection::Connection() : _address(), _socket()
{
}


Connection::Connection(const std::string& hostAndPort) : _address(hostAndPort), _socket()
{
	connect();
}


Connection::Connection(const std::string& host, int port) : _address(host, port), _socket()
{
	connect();
}


Connection::Connection(const Net::SocketAddress& addrs) : _address(addrs), _socket()
{
	connect();
}


Connection::~Connection()
{
	try
	{
		_socket.close();
	}
	catch (...)
	{
	}
}


void Connection::connect()
{
	_socket.connect(_address);
}


void Connection::connect(const std::string& hostAndPort)
{
	_address = Net::SocketAddress(hostAndPort);
	connect();
}


void Connection::connect(const std::string& host, int port)
{
	_address = Net::SocketAddress(host, port);
	connect();
}


void Connection::connect(const Net::SocketAddress& addrs)
{
	_address = addrs;
	connect();
}


void Connection::disconnect()
{
	_socket.close();
}


void Connection::sendRequest(RequestMessage& request)
{
	Net::SocketOutputStream sos(_socket);
	request.send(sos);
}


void Connection::sendRequest(RequestMessage& request, ResponseMessage& response)
{
	sendRequest(request);

	Net::SocketInputStream sis(_socket);
	response.read(sis);
}

} } // Poco::MongoDB
