//
// SecureServerSocket.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/SecureServerSocket.cpp#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureServerSocket
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureServerSocket.h"
#include "Poco/Net/SecureServerSocketImpl.h"
#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


SecureServerSocket::SecureServerSocket(): 
	ServerSocket(new SecureServerSocketImpl(SSLManager::instance().defaultServerContext()), true)
{
}


SecureServerSocket::SecureServerSocket(Context::Ptr pContext):
	ServerSocket(new SecureServerSocketImpl(pContext), true)
{
}


SecureServerSocket::SecureServerSocket(const Socket& socket): 
	ServerSocket(socket)
{
	if (!dynamic_cast<SecureServerSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


SecureServerSocket::SecureServerSocket(const SocketAddress& address, int backlog): 
	ServerSocket(new SecureServerSocketImpl(SSLManager::instance().defaultServerContext()), true)
{
	impl()->bind(address, true);
	impl()->listen(backlog);
}


SecureServerSocket::SecureServerSocket(const SocketAddress& address, int backlog, Context::Ptr pContext): 
	ServerSocket(new SecureServerSocketImpl(pContext), true)
{
	impl()->bind(address, true);
	impl()->listen(backlog);
}


SecureServerSocket::SecureServerSocket(Poco::UInt16 port, int backlog): 
	ServerSocket(new SecureServerSocketImpl(SSLManager::instance().defaultServerContext()), true)
{
	IPAddress wildcardAddr;
	SocketAddress address(wildcardAddr, port);
	impl()->bind(address, true);
	impl()->listen(backlog);
}

SecureServerSocket::SecureServerSocket(Poco::UInt16 port, int backlog, Context::Ptr pContext): 
	ServerSocket(new SecureServerSocketImpl(pContext), true)
{
	IPAddress wildcardAddr;
	SocketAddress address(wildcardAddr, port);
	impl()->bind(address, true);
	impl()->listen(backlog);
}


SecureServerSocket::~SecureServerSocket()
{
}


SecureServerSocket& SecureServerSocket::operator = (const Socket& socket)
{
	if (&socket != this)
	{
		if (dynamic_cast<SecureServerSocketImpl*>(socket.impl()))
			ServerSocket::operator = (socket);
		else
			throw InvalidArgumentException("Cannot assign incompatible socket");
	}
	return *this;
}


StreamSocket SecureServerSocket::acceptConnection(SocketAddress& clientAddr)
{
	return SecureStreamSocket(impl()->acceptConnection(clientAddr));
}


StreamSocket SecureServerSocket::acceptConnection()
{
	SocketAddress clientAddr;
	return SecureStreamSocket(impl()->acceptConnection(clientAddr));
}


Context::Ptr SecureServerSocket::context() const
{
	return static_cast<SecureServerSocketImpl*>(impl())->context();
}


} } // namespace Poco::Net
