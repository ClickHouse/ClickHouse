//
// SecureStreamSocket.cpp
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureStreamSocket
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureStreamSocket.h"
#include "Poco/Net/SecureStreamSocketImpl.h"
#include "Poco/Net/SocketImpl.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/Exception.h"


using Poco::InvalidArgumentException;


namespace Poco {
namespace Net {


SecureStreamSocket::SecureStreamSocket(): 
	StreamSocket(new SecureStreamSocketImpl(SSLManager::instance().defaultClientContext()))
{
}


SecureStreamSocket::SecureStreamSocket(Context::Ptr pContext): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
}


SecureStreamSocket::SecureStreamSocket(Context::Ptr pContext, Session::Ptr pSession): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
	useSession(pSession);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address): 
	StreamSocket(new SecureStreamSocketImpl(SSLManager::instance().defaultClientContext()))
{
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address, const std::string& hostName): 
	StreamSocket(new SecureStreamSocketImpl(SSLManager::instance().defaultClientContext()))
{
	static_cast<SecureStreamSocketImpl*>(impl())->setPeerHostName(hostName);
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address, Context::Ptr pContext): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address, Context::Ptr pContext, Session::Ptr pSession): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
	useSession(pSession);
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address, const std::string& hostName, Context::Ptr pContext): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
	static_cast<SecureStreamSocketImpl*>(impl())->setPeerHostName(hostName);
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const SocketAddress& address, const std::string& hostName, Context::Ptr pContext, Session::Ptr pSession): 
	StreamSocket(new SecureStreamSocketImpl(pContext))
{
	static_cast<SecureStreamSocketImpl*>(impl())->setPeerHostName(hostName);
	useSession(pSession);
	connect(address);
}


SecureStreamSocket::SecureStreamSocket(const Socket& socket): 
	StreamSocket(socket)
{
	if (!dynamic_cast<SecureStreamSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


SecureStreamSocket::SecureStreamSocket(SocketImpl* pImpl): 
	StreamSocket(pImpl)
{
	if (!dynamic_cast<SecureStreamSocketImpl*>(impl()))
		throw InvalidArgumentException("Cannot assign incompatible socket");
}


SecureStreamSocket::~SecureStreamSocket()
{
}


SecureStreamSocket& SecureStreamSocket::operator = (const Socket& socket)
{
	if (dynamic_cast<SecureStreamSocketImpl*>(socket.impl()))
		StreamSocket::operator = (socket);
	else
		throw InvalidArgumentException("Cannot assign incompatible socket");
	return *this;
}


bool SecureStreamSocket::havePeerCertificate() const
{
	return static_cast<SecureStreamSocketImpl*>(impl())->havePeerCertificate();
}


X509Certificate SecureStreamSocket::peerCertificate() const
{
	return static_cast<SecureStreamSocketImpl*>(impl())->peerCertificate();
}


void SecureStreamSocket::setPeerHostName(const std::string& hostName)
{
	static_cast<SecureStreamSocketImpl*>(impl())->setPeerHostName(hostName);
}

	
const std::string& SecureStreamSocket::getPeerHostName() const
{
	return static_cast<SecureStreamSocketImpl*>(impl())->getPeerHostName();
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), SSLManager::instance().defaultClientContext());
	SecureStreamSocket result(pImpl);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket, Context::Ptr pContext)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), pContext);
	SecureStreamSocket result(pImpl);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket, Context::Ptr pContext, Session::Ptr pSession)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), pContext);
	SecureStreamSocket result(pImpl);
	result.useSession(pSession);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket, const std::string& peerHostName)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), SSLManager::instance().defaultClientContext());
	SecureStreamSocket result(pImpl);
	result.setPeerHostName(peerHostName);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket, const std::string& peerHostName, Context::Ptr pContext)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), pContext);
	SecureStreamSocket result(pImpl);
	result.setPeerHostName(peerHostName);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


SecureStreamSocket SecureStreamSocket::attach(const StreamSocket& streamSocket, const std::string& peerHostName, Context::Ptr pContext, Session::Ptr pSession)
{
	SecureStreamSocketImpl* pImpl = new SecureStreamSocketImpl(static_cast<StreamSocketImpl*>(streamSocket.impl()), pContext);
	SecureStreamSocket result(pImpl);
	result.setPeerHostName(peerHostName);
	result.useSession(pSession);
	if (pImpl->context()->isForServerUse())
		pImpl->acceptSSL();
	else
		pImpl->connectSSL();
	return result;
}


Context::Ptr SecureStreamSocket::context() const
{
	return static_cast<SecureStreamSocketImpl*>(impl())->context();
}


void SecureStreamSocket::setLazyHandshake(bool flag)
{
	static_cast<SecureStreamSocketImpl*>(impl())->setLazyHandshake(flag);
}

	
bool SecureStreamSocket::getLazyHandshake() const
{
	return static_cast<SecureStreamSocketImpl*>(impl())->getLazyHandshake();
}


void SecureStreamSocket::verifyPeerCertificate()
{
	static_cast<SecureStreamSocketImpl*>(impl())->verifyPeerCertificate();
}


void SecureStreamSocket::verifyPeerCertificate(const std::string& hostName)
{
	static_cast<SecureStreamSocketImpl*>(impl())->verifyPeerCertificate(hostName);
}


int SecureStreamSocket::completeHandshake()
{
	return static_cast<SecureStreamSocketImpl*>(impl())->completeHandshake();
}


Session::Ptr SecureStreamSocket::currentSession()
{
	return static_cast<SecureStreamSocketImpl*>(impl())->currentSession();
}

	
void SecureStreamSocket::useSession(Session::Ptr pSession)
{
	static_cast<SecureStreamSocketImpl*>(impl())->useSession(pSession);
}

	
bool SecureStreamSocket::sessionWasReused()
{
	return static_cast<SecureStreamSocketImpl*>(impl())->sessionWasReused();
}


void SecureStreamSocket::abort()
{
	static_cast<SecureStreamSocketImpl*>(impl())->abort();
}


} } // namespace Poco::Net
