//
// DatagramSocket.cpp
//
// $Id: //poco/1.4/Net/src/DatagramSocket.cpp#1 $
//
// Library: Net
// Package: Sockets
// Module:  DatagramSocket
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/DatagramSocketImpl.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


DatagramSocket::DatagramSocket(): Socket(new DatagramSocketImpl)
{
}


DatagramSocket::DatagramSocket(IPAddress::Family family): Socket(new DatagramSocketImpl(family))
{
}


DatagramSocket::DatagramSocket(const SocketAddress& address, bool reuseAddress): Socket(new DatagramSocketImpl(address.family()))
{
	bind(address, reuseAddress);
}


DatagramSocket::DatagramSocket(const Socket& socket): Socket(socket)
{
	if (!dynamic_cast<DatagramSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


DatagramSocket::DatagramSocket(SocketImpl* pImpl): Socket(pImpl)
{
	if (!dynamic_cast<DatagramSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


DatagramSocket::~DatagramSocket()
{
}


DatagramSocket& DatagramSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<DatagramSocketImpl*>(socket.impl()))
		Socket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


void DatagramSocket::connect(const SocketAddress& address)
{
	impl()->connect(address);
}


void DatagramSocket::bind(const SocketAddress& address, bool reuseAddress)
{
	impl()->bind(address, reuseAddress);
}


int DatagramSocket::sendBytes(const void* buffer, int length, int flags)
{
	return impl()->sendBytes(buffer, length, flags);
}


int DatagramSocket::receiveBytes(void* buffer, int length, int flags)
{
	return impl()->receiveBytes(buffer, length, flags);
}


int DatagramSocket::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	return impl()->sendTo(buffer, length, address, flags);
}


int DatagramSocket::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	return impl()->receiveFrom(buffer, length, address, flags);
}


} } // namespace Poco::Net
