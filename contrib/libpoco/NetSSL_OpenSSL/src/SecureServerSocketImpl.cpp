//
// SecureServerSocketImpl.cpp
//
// $Id: //poco/1.4/NetSSL_OpenSSL/src/SecureServerSocketImpl.cpp#1 $
//
// Library: NetSSL_OpenSSL
// Package: SSLSockets
// Module:  SecureServerSocketImpl
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SecureServerSocketImpl.h"


namespace Poco {
namespace Net {


SecureServerSocketImpl::SecureServerSocketImpl(Context::Ptr pContext):
	_impl(new ServerSocketImpl, pContext)
{
}


SecureServerSocketImpl::~SecureServerSocketImpl()
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


SocketImpl* SecureServerSocketImpl::acceptConnection(SocketAddress& clientAddr)
{
	return _impl.acceptConnection(clientAddr);
}


void SecureServerSocketImpl::connect(const SocketAddress& address)
{
	throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}


void SecureServerSocketImpl::connect(const SocketAddress& address, const Poco::Timespan& timeout)
{
	throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}
	

void SecureServerSocketImpl::connectNB(const SocketAddress& address)
{
	throw Poco::InvalidAccessException("Cannot connect() a SecureServerSocket");
}
	

void SecureServerSocketImpl::bind(const SocketAddress& address, bool reuseAddress)
{
	_impl.bind(address, reuseAddress);
	reset(_impl.sockfd());
}

	
void SecureServerSocketImpl::listen(int backlog)
{
	_impl.listen(backlog);
	reset(_impl.sockfd());
}
	

void SecureServerSocketImpl::close()
{
	reset();
	_impl.close();
}
	

int SecureServerSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	throw Poco::InvalidAccessException("Cannot sendBytes() on a SecureServerSocket");
}


int SecureServerSocketImpl::receiveBytes(void* buffer, int length, int flags)
{
	throw Poco::InvalidAccessException("Cannot receiveBytes() on a SecureServerSocket");
}


int SecureServerSocketImpl::sendTo(const void* buffer, int length, const SocketAddress& address, int flags)
{
	throw Poco::InvalidAccessException("Cannot sendTo() on a SecureServerSocket");
}


int SecureServerSocketImpl::receiveFrom(void* buffer, int length, SocketAddress& address, int flags)
{
	throw Poco::InvalidAccessException("Cannot receiveFrom() on a SecureServerSocket");
}


void SecureServerSocketImpl::sendUrgent(unsigned char data)
{
	throw Poco::InvalidAccessException("Cannot sendUrgent() on a SecureServerSocket");
}


bool SecureServerSocketImpl::secure() const
{
	return true;
}


} } // namespace Poco::Net
