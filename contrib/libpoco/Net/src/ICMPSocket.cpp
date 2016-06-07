//
// ICMPSocket.cpp
//
// $Id: //poco/1.4/Net/src/ICMPSocket.cpp#1 $
//
// Library: Net
// Package: ICMP
// Module:  ICMPSocket
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/ICMPSocket.h"
#include "Poco/Net/ICMPSocketImpl.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


ICMPSocket::ICMPSocket(IPAddress::Family family, int dataSize, int ttl, int timeout): 
	Socket(new ICMPSocketImpl(family, dataSize, ttl, timeout)),
	_dataSize(dataSize), 
	_ttl(ttl),
	_timeout(timeout)
{
}


ICMPSocket::ICMPSocket(const Socket& socket): 
	Socket(socket)
{
	if (!dynamic_cast<ICMPSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


ICMPSocket::ICMPSocket(SocketImpl* pImpl): 
	Socket(pImpl)
{
	if (!dynamic_cast<ICMPSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


ICMPSocket::~ICMPSocket()
{
}


ICMPSocket& ICMPSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<ICMPSocketImpl*>(socket.impl()))
		Socket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


int ICMPSocket::sendTo(const SocketAddress& address, int flags)
{
	return impl()->sendTo(0, 0, address, flags);
}


int ICMPSocket::receiveFrom(SocketAddress& address, int flags)
{
	return impl()->receiveFrom(0, 0, address, flags);
}


} } // namespace Poco::Net
