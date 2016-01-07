//
// RawSocket.cpp
//
// $Id: //poco/1.4/Net/src/RawSocket.cpp#1 $
//
// Library: Net
// Package: Sockets
// Module:  RawSocket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/RawSocket.h"
#include "Poco/Net/RawSocketImpl.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


RawSocket::RawSocket(): 
	Socket(new RawSocketImpl)
{
}


RawSocket::RawSocket(IPAddress::Family family, int proto): 
	Socket(new RawSocketImpl(family, proto))
{
}


RawSocket::RawSocket(const SocketAddress& address, bool reuseAddress): 
	Socket(new RawSocketImpl(address.family()))
{
	bind(address, reuseAddress);
}


RawSocket::RawSocket(const Socket& socket): Socket(socket)
{
	if (!dynamic_cast<RawSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


RawSocket::RawSocket(SocketImpl* pImpl): Socket(pImpl)
{
	if (!dynamic_cast<RawSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


RawSocket::~RawSocket()
{
}


RawSocket& RawSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<RawSocketImpl*>(socket.impl()))
		Socket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


void RawSocket::connect(const SocketAddress& address)
{
	impl()->connect(address);
}


void RawSocket::bind(const SocketAddress& address, bool reuseAddress)
{
	impl()->bind(address, reuseAddress);
}


int RawSocket::sendBytes(const void* buffer, int length, int flags)
{
	return impl()->sendBytes(buffer, length, flags);
}


int RawSocket::receiveBytes(void* buffer, int length, int flags)
{
	return impl()->receiveBytes(buffer, length, flags);
}


int RawSocket::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	return impl()->sendTo(buffer, length, address, flags);
}


int RawSocket::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	return impl()->receiveFrom(buffer, length, address, flags);
}


} } // namespace Poco::Net
