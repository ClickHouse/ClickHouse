//
// SecureStreamSocketImpl.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/SecureStreamSocketImpl.cpp#6 $
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureStreamSocketImpl
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureStreamSocketImpl.h"
#include "Poco/Net/SSLException.h"
#include "Poco/Thread.h"


namespace Poco {
namespace Net {


SecureStreamSocketImpl::SecureStreamSocketImpl(Context::Ptr pContext):
	_impl(new StreamSocketImpl, pContext),
	_lazyHandshake(false)
{
}


SecureStreamSocketImpl::SecureStreamSocketImpl(StreamSocketImpl* pStreamSocket, Context::Ptr pContext):
	_impl(pStreamSocket, pContext),
	_lazyHandshake(false)
{
	pStreamSocket->duplicate();
	reset(_impl.sockfd());
}


SecureStreamSocketImpl::~SecureStreamSocketImpl()
{
	try
	{
		reset();
	}
	catch (...)
	{
		poco_unexpected();
	}
}


SocketImpl* SecureStreamSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	throw Poco::InvalidAccessException("Cannot acceptConnection() on a SecureStreamSocketImpl");
}


void SecureStreamSocketImpl::acceptSSL()
{
	_impl.acceptSSL();
}


void SecureStreamSocketImpl::connect(const SocketAddress& address)
{
	_impl.connect(address, !_lazyHandshake);
	reset(_impl.sockfd());
}


void SecureStreamSocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout)
{
	_impl.connect(address, timeout, !_lazyHandshake);
	reset(_impl.sockfd());
}
	

void SecureStreamSocketImpl::connectNB(const SocketAddress& address)
{
	_impl.connectNB(address);
	reset(_impl.sockfd());
}


void SecureStreamSocketImpl::connectSSL()
{
	_impl.connectSSL(!_lazyHandshake);
}
	

void SecureStreamSocketImpl::bind(const SocketAddress& address, bool reuseAddress)
{
	throw Poco::InvalidAccessException("Cannot bind() a SecureStreamSocketImpl");
}

	
void SecureStreamSocketImpl::listen(int backlog)
{
	throw Poco::InvalidAccessException("Cannot listen() on a SecureStreamSocketImpl");
}
	

void SecureStreamSocketImpl::close()
{
	reset();
	_impl.close();
}


void SecureStreamSocketImpl::abort()
{
	reset();
	_impl.abort();
}


int SecureStreamSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	return _impl.sendBytes(buffer, length, flags);
}


int SecureStreamSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	return _impl.receiveBytes(buffer, length, flags);
}


int SecureStreamSocketImpl::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	throw Poco::InvalidAccessException("Cannot sendTo() on a SecureStreamSocketImpl");
}


int SecureStreamSocketImpl::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	throw Poco::InvalidAccessException("Cannot receiveFrom() on a SecureStreamSocketImpl");
}


void SecureStreamSocketImpl::sendUrgent(unsigned char data)
{
	throw Poco::InvalidAccessException("Cannot sendUrgent() on a SecureStreamSocketImpl");
}


int SecureStreamSocketImpl::available()
{
	return _impl.available();
}


void SecureStreamSocketImpl::shutdownReceive()
{
}

	
void SecureStreamSocketImpl::shutdownSend()
{
}

	
void SecureStreamSocketImpl::shutdown()
{
	_impl.shutdown();
}


bool SecureStreamSocketImpl::secure() const
{
	return true;
}


bool SecureStreamSocketImpl::havePeerCertificate() const
{
	X509* pCert = _impl.peerCertificate();
	if (pCert)
	{
		X509_free(pCert);
		return true;
	}
	else return false;
}


X509Certificate SecureStreamSocketImpl::peerCertificate() const
{
	X509* pCert = _impl.peerCertificate();
	if (pCert)
		return X509Certificate(pCert);
	else
		throw SSLException("No certificate available");
}


void SecureStreamSocketImpl::setLazyHandshake(bool flag)
{
	_lazyHandshake = flag;
}

	
bool SecureStreamSocketImpl::getLazyHandshake() const
{
	return _lazyHandshake;
}


void SecureStreamSocketImpl::verifyPeerCertificate()
{
	_impl.verifyPeerCertificate();
}


void SecureStreamSocketImpl::verifyPeerCertificate(const std::string& hostName)
{
	_impl.verifyPeerCertificate(hostName);
}


int SecureStreamSocketImpl::completeHandshake()
{
	return _impl.completeHandshake();
}


} } // namespace Poco::Net
