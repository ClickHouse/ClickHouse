//
// StreamSocketImpl.cpp
//
// Library: Net
// Package: Sockets
// Module:  StreamSocketImpl
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/Exception.h"
#include "Poco/Thread.h"


namespace Poco {
namespace Net {


StreamSocketImpl::StreamSocketImpl()
{
}


StreamSocketImpl::StreamSocketImpl(SocketAddress::Family family)
{
	if (family == SocketAddress::IPv4)
		init(AF_INET);
#if defined(POCO_HAVE_IPv6)
	else if (family == SocketAddress::IPv6)
		init(AF_INET6);
#endif
#if defined(POCO_OS_FAMILY_UNIX)
	else if (family == SocketAddress::UNIX_LOCAL)
		init(AF_UNIX);
#endif
	else throw Poco::InvalidArgumentException("Invalid or unsupported address family passed to StreamSocketImpl");
}


StreamSocketImpl::StreamSocketImpl(poco_socket_t sockfd): SocketImpl(sockfd)
{
}


StreamSocketImpl::~StreamSocketImpl()
{
}


int StreamSocketImpl::sendBytes(const void* buffer, int length, int flags)
{
	const char* p = reinterpret_cast<const char*>(buffer);
	int remaining = length;
	int sent = 0;
	bool blocking = getBlocking();
	while (remaining > 0)
	{
		int n = SocketImpl::sendBytes(p, remaining, flags);
		poco_assert_dbg (n >= 0);
		p += n; 
		sent += n;
		remaining -= n;
		if (blocking && remaining > 0)
			Poco::Thread::yield();
		else
			break;
	}
	return sent;
}


} } // namespace Poco::Net
